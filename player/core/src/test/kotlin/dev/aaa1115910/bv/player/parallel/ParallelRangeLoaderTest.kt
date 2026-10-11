package dev.aaa1115910.bv.player.parallel

import okhttp3.OkHttpClient
import okhttp3.Protocol
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import kotlin.random.Random
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class ParallelRangeLoaderTest {
    private val file = Random(42).nextBytes(5 * 1024 * 1024 + 12345)
    private val servers = mutableListOf<ServerSocket>()
    private val client = OkHttpClient.Builder()
        .protocols(listOf(Protocol.HTTP_1_1))
        .readTimeout(1, TimeUnit.SECONDS)
        .build()

    private enum class Behavior { Serve, Refuse, Stall, CutShort, Slow, Capped, TooMany }
    private val inFlight = AtomicInteger()
    private val maxInFlight = AtomicInteger()

    /** A node on [host] (127.0.0.1 or localhost, so they count as two nodes). */
    private fun node(host: String, behavior: (Int) -> Behavior = { Behavior.Serve }): Pair<String, AtomicInteger> {
        val requests = AtomicInteger()
        val server = ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"))
        servers += server
        thread(isDaemon = true) {
            while (!server.isClosed) {
                val socket = runCatching { server.accept() }.getOrNull() ?: break
                thread(isDaemon = true) { socket.use { answer(it, behavior(requests.incrementAndGet())) } }
            }
        }
        return "http://$host:${server.localPort}/video.m4s" to requests
    }

    private fun answer(socket: Socket, behavior: Behavior) {
        val input = socket.getInputStream().bufferedReader(Charsets.ISO_8859_1)
        val lines = generateSequence { input.readLine() }.takeWhile { it.isNotEmpty() }.toList()
        val range = lines.first { it.startsWith("Range:", ignoreCase = true) }
            .substringAfter("bytes=").split("-").map { it.trim().toLong() }
        val start = range[0]
        val end = minOf(range[1], file.size - 1L)
        val out = socket.getOutputStream()
        maxInFlight.accumulateAndGet(inFlight.incrementAndGet(), ::maxOf)
        try {
            respond(out, behavior, start, end)
        } finally {
            inFlight.decrementAndGet()
        }
    }

    private fun respond(out: java.io.OutputStream, behavior: Behavior, start: Long, end: Long) {
        fun head(status: String, extra: String = "") =
            out.write("HTTP/1.1 $status\r\nConnection: close\r\n$extra\r\n".toByteArray())
        when (behavior) {
            Behavior.Refuse -> head("403 Forbidden", "Content-Length: 0\r\n")
            Behavior.TooMany -> head("429 Too Many Requests", "Content-Length: 0\r\n")
            Behavior.Capped -> {
                val cappedEnd = minOf(end, start + 200_000 - 1)
                val length = cappedEnd - start + 1
                head("206 Partial Content", "Content-Range: bytes $start-$cappedEnd/${file.size}\r\nContent-Length: $length\r\n")
                out.write(file, start.toInt(), length.toInt())
            }
            Behavior.Stall -> Thread.sleep(3000)
            Behavior.Serve, Behavior.CutShort, Behavior.Slow -> {
                if (start >= file.size) {
                    head("416 Range Not Satisfiable", "Content-Range: bytes */${file.size}\r\nContent-Length: 0\r\n")
                    return
                }
                val length = end - start + 1
                head("206 Partial Content", "Content-Range: bytes $start-$end/${file.size}\r\nContent-Length: $length\r\n")
                val sent = if (behavior == Behavior.CutShort) length / 2 else length
                if (behavior == Behavior.Slow) Thread.sleep(150)
                out.write(file, start.toInt(), sent.toInt())
            }
        }
        out.flush()
    }

    @AfterTest
    fun stopServers() = servers.forEach { it.close() }

    private fun readAll(loader: ParallelRangeLoader): ByteArray {
        val out = ByteArrayOutputStream()
        val buffer = ByteArray(37_000)
        while (true) {
            val count = loader.read(buffer, 0, buffer.size)
            if (count < 0) break
            out.write(buffer, 0, count)
        }
        return out.toByteArray()
    }

    private fun loader(urls: List<String>, threads: Int = 6) =
        ParallelRangeLoader(client, urls, emptyMap(), CdnScoreboard(), threads, 8L * 1024 * 1024)

    @Test
    fun `reads the whole file from two nodes`() {
        val (a, aRequests) = node("127.0.0.1")
        val (b, bRequests) = node("localhost")
        loader(listOf(a, b)).use { loader ->
            assertEquals(file.size.toLong(), loader.open(0, -1))
            assertContentEquals(file, readAll(loader))
        }
        assertTrue(aRequests.get() > 0 && bRequests.get() > 0, "both nodes carried pieces")
    }

    @Test
    fun `starts in the middle and honours a bounded length`() {
        val (a, _) = node("127.0.0.1")
        loader(listOf(a)).use { loader ->
            assertEquals(3_000_000L, loader.open(1_000_000, 3_000_000))
            assertContentEquals(file.copyOfRange(1_000_000, 4_000_000), readAll(loader))
        }
    }

    @Test
    fun `opening at the end returns no bytes`() {
        val (a, _) = node("127.0.0.1")
        loader(listOf(a)).use { loader ->
            loader.open(file.size.toLong(), -1)
            assertEquals(-1, loader.read(ByteArray(10), 0, 10))
        }
    }

    @Test
    fun `a refusing node is left out and the data stays intact`() {
        val (good, _) = node("127.0.0.1")
        val (bad, badRequests) = node("localhost") { Behavior.Refuse }
        loader(listOf(bad, good)).use { loader ->
            loader.open(0, -1)
            assertContentEquals(file, readAll(loader))
        }
        assertTrue(badRequests.get() <= 3, "a banned node gets no more pieces, got ${badRequests.get()}")
    }

    @Test
    fun `pieces cut short and stalled resume on another node`() {
        val (flaky, _) = node("127.0.0.1") { if (it % 3 == 0) Behavior.CutShort else if (it % 7 == 0) Behavior.Stall else Behavior.Serve }
        val (good, _) = node("localhost")
        loader(listOf(flaky, good)).use { loader ->
            loader.open(0, -1)
            assertContentEquals(file, readAll(loader))
        }
    }

    @Test
    fun `an address every node refuses fails at once with its status`() {
        val (a, _) = node("127.0.0.1") { Behavior.Refuse }
        val (b, _) = node("localhost") { Behavior.Refuse }
        val started = System.currentTimeMillis()
        loader(listOf(a, b)).use { loader ->
            val error = assertFailsWith<ParallelRangeLoader.HttpStatusException> { loader.open(0, -1) }
            assertEquals(403, error.code)
        }
        assertTrue(System.currentTimeMillis() - started < 5_000, "failed within seconds")
    }

    @Test
    fun `a node that answers with shorter ranges is not treated as failing`() {
        val (capped, cappedRequests) = node("127.0.0.1") { Behavior.Capped }
        val scoreboard = CdnScoreboard()
        ParallelRangeLoader(client, listOf(capped), emptyMap(), scoreboard, 4, 8L * 1024 * 1024).use { loader ->
            loader.open(0, -1)
            assertContentEquals(file, readAll(loader))
        }
        assertEquals("healthy", scoreboard.status().single().state)
        assertTrue(cappedRequests.get() > file.size / 200_000)
    }

    @Test
    fun `rate limiting is retried rather than failing playback`() {
        val (limited, _) = node("127.0.0.1") { if (it <= 2) Behavior.TooMany else Behavior.Serve }
        val (other, _) = node("localhost") { if (it <= 2) Behavior.TooMany else Behavior.Serve }
        loader(listOf(limited, other)).use { loader ->
            loader.open(0, -1)
            assertContentEquals(file, readAll(loader))
        }
    }

    @Test
    fun `an interrupted reader gets an InterruptedIOException`() {
        val (slow, _) = node("127.0.0.1") { Behavior.Stall }
        val loader = loader(listOf(slow))
        var thrown: Throwable? = null
        val opener = Thread { thrown = runCatching { loader.open(0, -1) }.exceptionOrNull() }
        opener.start()
        Thread.sleep(300)
        opener.interrupt()
        opener.join(2000)
        loader.close()
        assertTrue(thrown is java.io.InterruptedIOException, "got $thrown")
    }

    @Test
    fun `sixteen threads keep more than ten requests in flight`() {
        val (a, _) = node("127.0.0.1") { Behavior.Slow }
        val (b, _) = node("localhost") { Behavior.Slow }
        loader(listOf(a, b), threads = 16).use { loader ->
            loader.open(0, -1)
            assertContentEquals(file, readAll(loader))
        }
        assertTrue(maxInFlight.get() > 10, "at most ${maxInFlight.get()} requests ran at once")
    }

    @Test
    fun `close while the reader waits does not hang`() {
        val (slow, _) = node("127.0.0.1") { Behavior.Stall }
        val loader = loader(listOf(slow))
        val opener = Thread { runCatching { loader.open(0, -1) } }
        opener.start()
        Thread.sleep(300)
        loader.close()
        opener.join(2000)
        assertTrue(!opener.isAlive)
    }

    @Test
    fun `candidate urls move one ordinary address to every node of the mode`() {
        val base = "https://upos-sz-mirrorcosov.bilivideo.com/upgcxcode/1/2/3/3-1-30120.m4s?e=x&deadline=1"
        val akamai = "https://upos-hz-mirrorakam.akamaized.net/upgcxcode/1/2/3/3-1-30120.m4s?e=y"
        val overseas = BiliCdn.candidateUrls(listOf(base, akamai), CdnMode.Overseas)
        assertEquals(listOf(base, akamai) + BiliCdn.overseasHosts.drop(1).map { BiliCdn.withHost(base, it) }, overseas)
        val mainland = BiliCdn.candidateUrls(listOf(base, akamai), CdnMode.Mainland)
        assertEquals(listOf(base, akamai) + BiliCdn.mainlandHosts.map { BiliCdn.withHost(base, it) }, mainland)
        assertEquals(
            listOf(akamai) + BiliCdn.mainlandHosts.map { BiliCdn.withHost(akamai, it) },
            BiliCdn.candidateUrls(listOf(akamai), CdnMode.Mainland)
        )
        val pcdn = "https://xy1x2x3x4xy.mcdn.bilivideo.cn:4483/upgcxcode/1/2/3/3-1-30120.m4s?e=z"
        assertEquals(
            listOf(pcdn, base) + BiliCdn.mainlandHosts.map { BiliCdn.withHost(base, it) },
            BiliCdn.candidateUrls(listOf(pcdn, base), CdnMode.Mainland),
            "a PCDN address is kept but never moved"
        )
        assertEquals(listOf(pcdn), BiliCdn.candidateUrls(listOf(pcdn), CdnMode.Mainland))
    }
}
