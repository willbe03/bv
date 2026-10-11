package dev.aaa1115910.bv.player.parallel

import io.github.oshai.kotlinlogging.KotlinLogging
import okhttp3.Call
import okhttp3.OkHttpClient
import okhttp3.Request
import java.io.Closeable
import java.io.EOFException
import java.io.IOException
import java.io.InterruptedIOException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.math.min

/**
 * Reads one media file as a single stream while downloading the bytes ahead of the reader
 * in pieces, several at a time, each from whichever CDN node is expected to deliver it
 * soonest. A piece that fails or stalls resumes from where it stopped on another node.
 *
 * At most [windowBytes] are held ahead of the reader; pieces get smaller as [threads] grows
 * so that every thread has one.
 */
class ParallelRangeLoader(
    private val client: OkHttpClient,
    private val urls: List<String>,
    private val headers: Map<String, String>,
    private val scoreboard: CdnScoreboard,
    threads: Int,
    windowBytes: Long = DEFAULT_WINDOW_BYTES,
    private val clock: () -> Long = System::currentTimeMillis
) : Closeable {
    private class Piece(val index: Int, val start: Long, var end: Long) {
        val data = ByteArray((end - start).toInt())
        var filled = 0
        var running: Attempt? = null
        val failedHosts = mutableSetOf<String>()
        val refusedHosts = mutableSetOf<String>()
        var failures = 0
        var evictions = 0
        var firstFailureAt = 0L
        var retryAt = 0L
        val size get() = (end - start).toInt()
        val done get() = filled >= size
    }

    private class Attempt(val url: String, val startedAt: Long, val startFilled: Int) {
        @Volatile
        var call: Call? = null

        @Volatile
        var abandoned = false
    }

    /** A node answered with an error status; when every node refuses, the address is bad. */
    class HttpStatusException(val code: Int) : IOException("HTTP $code")

    private val lock = ReentrantLock()
    private val dataReady = lock.newCondition()
    private val workReady = lock.newCondition()
    private val threads = threads.coerceIn(1, 64)
    private val pieceBytes = (windowBytes / (this.threads + 4)).coerceIn(MIN_PIECE_BYTES, MAX_PIECE_BYTES)
    private val windowPieces = (windowBytes / pieceBytes).toInt().coerceAtLeast(this.threads + 1)
    private val preferredHost = urls.firstOrNull()?.let(BiliCdn::hostOf)
    private val hostCount = urls.map(BiliCdn::hostOf).distinct().size
    private val pieces = HashMap<Int, Piece>()
    private var position = 0L
    private var end = -1L
    private var readPosition = 0L
    private var error: IOException? = null
    private var closed = false

    /** Starts downloading at [position] and returns how many bytes follow it. */
    fun open(position: Long, length: Long): Long {
        require(urls.isNotEmpty()) { "no address to download from" }
        logger.debug { "open at $position, length $length, ${urls.size} addresses, $threads threads, ${pieceBytes / 1024} KiB pieces" }
        lock.withLock {
            this.position = position
            readPosition = position
            if (length >= 0) end = position + length
        }
        repeat(threads) { pool.execute { work(rescuer = false) } }
        // These only take the pieces at the read position, so a piece moved off a slow node
        // restarts at once even while every other thread is busy.
        repeat(RESCUE_THREADS) { pool.execute { work(rescuer = true) } }
        lock.withLock {
            while (end < 0 && error == null && !closed) awaitInterruptibly { dataReady.await() }
            error?.let { throw it }
            if (closed) throw IOException("closed")
            return end - position
        }
    }

    /** Copies the next bytes into [buffer]; -1 at the end. Blocks until some are there. */
    fun read(buffer: ByteArray, offset: Int, length: Int): Int {
        lock.withLock {
            while (true) {
                if (closed) throw IOException("closed")
                if (end in 0..readPosition) return -1
                val index = pieceIndexOf(readPosition)
                val piece = pieces[index]
                if (piece != null) {
                    val offsetInPiece = (readPosition - piece.start).toInt()
                    val available = piece.filled - offsetInPiece
                    if (available > 0) {
                        val count = min(available, length)
                        System.arraycopy(piece.data, offsetInPiece, buffer, offset, count)
                        readPosition += count
                        if (readPosition >= piece.end) {
                            pieces.remove(index)
                            workReady.signalAll()
                        }
                        return count
                    }
                }
                error?.let { throw it }
                evictStragglers()
                awaitInterruptibly { dataReady.await(100, TimeUnit.MILLISECONDS) }
            }
        }
    }

    private inline fun awaitInterruptibly(wait: () -> Unit) {
        try {
            wait()
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            throw InterruptedIOException()
        }
    }

    override fun close() {
        lock.withLock {
            closed = true
            for (piece in pieces.values) piece.running?.let(::abandon)
            pieces.clear()
            workReady.signalAll()
            dataReady.signalAll()
        }
    }

    private fun pieceStart(index: Int): Long {
        val small = min(SMALL_PIECE_BYTES, pieceBytes)
        return position + if (index < SMALL_PIECES) index * small
        else SMALL_PIECES * small + (index - SMALL_PIECES) * pieceBytes
    }

    private fun pieceIndexOf(offset: Long): Int {
        val small = min(SMALL_PIECE_BYTES, pieceBytes)
        val relative = offset - position
        return if (relative < SMALL_PIECES * small) (relative / small).toInt()
        else SMALL_PIECES + ((relative - SMALL_PIECES * small) / pieceBytes).toInt()
    }

    private fun work(rescuer: Boolean) {
        while (true) {
            val (piece, attempt) = lock.withLock { nextJob(rescuer) } ?: return
            download(piece, attempt)
        }
    }

    /** The lowest piece in the window that nobody is downloading; waits while there is none. */
    private fun nextJob(rescuer: Boolean): Pair<Piece, Attempt>? {
        while (!closed && error == null) {
            val now = clock()
            val first = pieceIndexOf(readPosition)
            // Until the first answer gives the file size, only the first piece is asked for.
            val last = when {
                end < 0 -> first
                rescuer -> min(first + RESCUE_SPAN - 1, pieceIndexOf(end - 1))
                else -> min(first + windowPieces - 1, pieceIndexOf(end - 1))
            }
            var nextRetryAt = Long.MAX_VALUE
            for (index in first..last) {
                val start = pieceStart(index)
                if (end in 0..start) break
                val piece = pieces.getOrPut(index) {
                    val next = pieceStart(index + 1)
                    Piece(index, start, if (end >= 0) min(next, end) else next)
                }
                if (piece.done || piece.running != null) continue
                if (piece.retryAt > now) {
                    nextRetryAt = min(nextRetryAt, piece.retryAt)
                    continue
                }
                if (piece.firstFailureAt > 0 && now - piece.firstFailureAt > RETRY_WINDOW_MS) {
                    fail(IOException("no CDN node delivered bytes ${piece.start}-${piece.end - 1}"))
                    return null
                }
                if (piece.failedHosts.size >= hostCount) piece.failedHosts.clear()
                val urgent = index - first < RESCUE_SPAN || piece.evictions > 0
                val url = scoreboard.acquire(urls, piece.failedHosts, urgent)
                val attempt = Attempt(url, now, piece.filled)
                piece.running = attempt
                return piece to attempt
            }
            if (nextRetryAt == Long.MAX_VALUE) workReady.await()
            else workReady.await(nextRetryAt - now, TimeUnit.MILLISECONDS)
        }
        return null
    }

    private fun download(piece: Piece, attempt: Attempt) {
        var received = 0L
        var failure: IOException? = null
        try {
            val from = piece.start + attempt.startFilled
            val to = lock.withLock { piece.end } - 1
            val request = Request.Builder().url(attempt.url).apply {
                headers.forEach { (name, value) -> header(name, value) }
                header("Range", "bytes=$from-$to")
            }.build()
            val call = client.newCall(request)
            attempt.call = call
            if (attempt.abandoned) return
            call.execute().use { response ->
                val range = ContentRange.parse(response.header("Content-Range"))
                if (response.code == 416 && range?.total != null && from >= range.total) {
                    lock.withLock { learnTotal(range.total, piece) }
                    return
                }
                if (response.code != 206) throw HttpStatusException(response.code)
                if (range == null || range.start != from) throw IOException("unexpected Content-Range ${response.header("Content-Range")}")
                range.total?.let { total -> lock.withLock { learnTotal(total, piece) } }
                val body = response.body?.byteStream() ?: throw IOException("empty response")
                val buffer = ByteArray(READ_BUFFER_BYTES)
                // A node may answer with less than was asked for; the rest is asked for again.
                val sent = range.end - range.start + 1
                while (!attempt.abandoned && received < sent) {
                    val wanted = min(lock.withLock { piece.size - piece.filled }.toLong(), sent - received).toInt()
                    if (wanted <= 0) break
                    val count = body.read(buffer, 0, min(buffer.size, wanted))
                    if (count < 0) throw EOFException("the node closed the connection early")
                    lock.withLock {
                        if (attempt.abandoned || closed) return
                        System.arraycopy(buffer, 0, piece.data, piece.filled, count)
                        piece.filled += count
                        dataReady.signal()
                    }
                    received += count
                }
            }
        } catch (e: IOException) {
            failure = e
        } finally {
            val elapsed = clock() - attempt.startedAt
            logger.debug {
                "piece ${piece.index} from ${BiliCdn.hostOf(attempt.url)}: $received bytes in $elapsed ms" +
                    (failure?.let { ", failed: ${it.message}" } ?: if (attempt.abandoned) ", moved" else "")
            }
            scoreboard.release(attempt.url)
            lock.withLock {
                val finishedHere = failure == null && !attempt.abandoned && piece.done
                scoreboard.record(attempt.url, received, elapsed, finishedHere)
                if (piece.running === attempt) piece.running = null
                if (received > 0 || piece.done) piece.firstFailureAt = 0
                if (failure != null && !attempt.abandoned && !closed) recordFailure(piece, attempt, failure, received)
                workReady.signalAll()
                dataReady.signalAll()
            }
        }
    }

    /** Called with the lock held. */
    private fun recordFailure(piece: Piece, attempt: Attempt, failure: IOException, received: Long) {
        val host = BiliCdn.hostOf(attempt.url)
        val status = (failure as? HttpStatusException)?.code ?: 0
        val transient = status == 408 || status == 429
        scoreboard.failure(attempt.url, received, transient)
        piece.failedHosts += host
        piece.failures += 1
        if (piece.firstFailureAt == 0L) piece.firstFailureAt = clock()
        if (status in 400..499 && !transient) {
            piece.refusedHosts += host
            // Bilibili's own node and one more refusing, or all of them, means the address
            // itself is bad, as an expired signature makes it, so the error goes to the player
            // at once instead of after the retry window.
            val refusedByOwnNode = preferredHost in piece.refusedHosts && piece.refusedHosts.size >= 2
            if (refusedByOwnNode || piece.refusedHosts.size >= hostCount) fail(HttpStatusException(status))
        } else {
            piece.retryAt = clock() + min(2_000L, 250L * piece.failures)
        }
    }

    /** Called with the lock held. */
    private fun fail(e: IOException) {
        if (error == null) error = e
        for (each in pieces.values) each.running?.let(::abandon)
        workReady.signalAll()
        dataReady.signalAll()
    }

    /** Called with the lock held. */
    private fun learnTotal(total: Long, piece: Piece) {
        end = if (end < 0) total else min(end, total)
        if (end < position) end = position
        for (each in pieces.values) if (each.end > end) each.end = maxOf(each.start, end)
        if (piece.end > end) piece.end = maxOf(piece.start, end)
        workReady.signalAll()
        dataReady.signalAll()
    }

    /**
     * Called with the lock held while the reader waits. A piece near the read position whose
     * node would take far longer to finish it than the fastest other node moves there, and
     * the node sits out for a while. A node that has sent nothing yet is left to the read
     * timeout, except on the piece the reader is waiting for.
     */
    private fun evictStragglers() {
        val now = clock()
        val first = pieceIndexOf(readPosition)
        for (index in first until first + STRAGGLER_SPAN) {
            val piece = pieces[index] ?: continue
            val attempt = piece.running ?: continue
            if (piece.evictions >= MAX_EVICTIONS) continue
            val elapsed = now - attempt.startedAt
            val got = piece.filled - attempt.startFilled
            val silent = got <= 0 && index == first && elapsed >= SILENT_HEAD_MS
            if (!silent) {
                if (elapsed < STRAGGLER_AFTER_MS || got <= 0) continue
                val best = scoreboard.bestSpeed(urls, piece.failedHosts + BiliCdn.hostOf(attempt.url)) ?: continue
                val remaining = piece.size - piece.filled
                val here = remaining / (got * 1000.0 / elapsed)
                val there = remaining / best + MOVE_OVERHEAD_SECONDS
                if (here <= there * 3) continue
            }
            abandon(attempt)
            piece.running = null
            piece.evictions += 1
            piece.failedHosts += BiliCdn.hostOf(attempt.url)
            scoreboard.bench(attempt.url)
            workReady.signalAll()
        }
    }

    /** Closing a socket can block on a stalled connection, so it never happens under the lock. */
    private fun abandon(attempt: Attempt) {
        attempt.abandoned = true
        val call = attempt.call ?: return
        pool.execute { call.cancel() }
    }

    internal data class ContentRange(val start: Long, val end: Long, val total: Long?) {
        companion object {
            private val satisfied = Regex("""^bytes\s+(\d+)-(\d+)/(\d+|\*)$""", RegexOption.IGNORE_CASE)
            private val unsatisfied = Regex("""^bytes\s+\*/(\d+)$""", RegexOption.IGNORE_CASE)

            fun parse(value: String?): ContentRange? {
                val text = value?.trim() ?: return null
                satisfied.find(text)?.let {
                    val (start, end, total) = it.destructured
                    return ContentRange(start.toLong(), end.toLong(), total.toLongOrNull())
                }
                unsatisfied.find(text)?.let {
                    return ContentRange(-1, -1, it.groupValues[1].toLong())
                }
                return null
            }
        }
    }

    companion object {
        private val logger = KotlinLogging.logger("ParallelRangeLoader")
        private val pool: ExecutorService = Executors.newCachedThreadPool { runnable ->
            Thread(runnable, "parallel-range").apply { isDaemon = true }
        }
        const val DEFAULT_WINDOW_BYTES = 16L * 1024 * 1024
        private const val SMALL_PIECES = 2
        private const val SMALL_PIECE_BYTES = 512L * 1024
        private const val MIN_PIECE_BYTES = 512L * 1024
        private const val MAX_PIECE_BYTES = 2048L * 1024
        private const val READ_BUFFER_BYTES = 64 * 1024
        private const val RETRY_WINDOW_MS = 25_000L
        private const val RESCUE_THREADS = 2
        private const val RESCUE_SPAN = 3
        private const val STRAGGLER_SPAN = 6
        private const val STRAGGLER_AFTER_MS = 2_000L
        private const val MOVE_OVERHEAD_SECONDS = 1.5
        private const val MAX_EVICTIONS = 2
        private const val SILENT_HEAD_MS = 3_500L
    }
}
