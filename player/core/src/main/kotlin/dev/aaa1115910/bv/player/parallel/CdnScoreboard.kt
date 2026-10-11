package dev.aaa1115910.bv.player.parallel

import kotlin.math.min

/**
 * What one playback has learned about each CDN node: how fast it delivers, how often it has
 * failed lately, and whether it is banned. The video and audio streams of the same video share
 * one, and a new video starts with a new one.
 */
class CdnScoreboard(private val clock: () -> Long = System::currentTimeMillis) {
    private class Node {
        var bps = 0.0
        var measuredAt = 0L
        var delivered = false
        var failures = 0
        var blockedUntil = 0L
        var benchedUntil = 0L
        var emptyFailures = 0
        var active = 0
    }

    data class NodeStatus(val host: String, val state: String, val bps: Long)

    private val nodes = LinkedHashMap<String, Node>()

    private fun node(url: String) = nodes.getOrPut(BiliCdn.hostOf(url)) { Node() }

    /** A node that twice failed without sending a byte, and never delivered anything. */
    private fun banned(node: Node) = node.emptyFailures >= BAN_AFTER_EMPTY_FAILURES && !node.delivered

    private fun available(node: Node, now: Long) = node.blockedUntil <= now && node.benchedUntil <= now

    private fun speedOf(node: Node, now: Long): Double? =
        node.bps.takeIf { it > 0 && now - node.measuredAt < MEASUREMENT_TTL_MS }

    /**
     * The address whose node is expected to finish a piece soonest. The first candidate is
     * Bilibili's own pick; an [urgent] piece goes only to it or to nodes that have delivered.
     */
    @Synchronized
    fun acquire(candidates: List<String>, exclude: Set<String> = emptySet(), urgent: Boolean = false): String {
        val now = clock()
        val notExcluded = candidates.filter { BiliCdn.hostOf(it) !in exclude }.ifEmpty { candidates }
        val allowed = notExcluded.filter { !banned(node(it)) }.ifEmpty { notExcluded }
        val preferred = candidates.firstOrNull()
        val usable = allowed.filter { available(node(it), now) }.ifEmpty { allowed }
        val proven = usable.filter { it == preferred || node(it).delivered }
        val ready = if (urgent && proven.isNotEmpty()) proven else usable
        val fastest = ready.mapNotNull { speedOf(node(it), now) }.maxOrNull()
        val assumed = fastest?.div(2) ?: DEFAULT_BPS
        val chosen = ready.minBy { url ->
            val node = node(url)
            val unmeasured = if (url == preferred) assumed * PREFERRED_BOOST else assumed
            (node.active + 1) / (speedOf(node, now) ?: unmeasured)
        }
        node(chosen).active += 1
        return chosen
    }

    @Synchronized
    fun release(url: String) {
        val node = node(url)
        node.active = (node.active - 1).coerceAtLeast(0)
    }

    /** The best measured speed among the [candidates] that could take a piece now. */
    @Synchronized
    fun bestSpeed(candidates: List<String>, exclude: Set<String>): Double? {
        val now = clock()
        return candidates.filter { BiliCdn.hostOf(it) !in exclude }
            .map(::node)
            .filter { !banned(it) && available(it, now) }
            .mapNotNull { speedOf(it, now) }
            .maxOrNull()
    }

    /** A finished or cut-off transfer. Only one long enough says anything about speed. */
    @Synchronized
    fun record(url: String, bytes: Long, elapsedMs: Long, completed: Boolean) {
        val node = node(url)
        if (bytes > 0) {
            node.delivered = true
            node.emptyFailures = 0
        }
        if (completed) {
            node.failures = 0
            node.blockedUntil = 0
        }
        if (bytes >= SPEED_SAMPLE_MIN_BYTES && elapsedMs > 0) {
            val bps = bytes * 1000.0 / elapsedMs
            node.bps = if (node.bps > 0) node.bps * 0.65 + bps * 0.35 else bps
            node.measuredAt = clock()
        }
    }

    /** A node that was too slow for a piece: left out for a while, without counting a failure. */
    @Synchronized
    fun bench(url: String) {
        val node = node(url)
        node.benchedUntil = clock() + BENCH_MS
    }

    /** A [transient] failure, such as a rate limit, backs the node off without counting toward a ban. */
    @Synchronized
    fun failure(url: String, receivedBytes: Long, transient: Boolean = false) {
        val node = node(url)
        node.failures += 1
        node.blockedUntil = clock() + min(60_000L, 3_000L shl min(node.failures, 4))
        if (receivedBytes == 0L && !transient) node.emptyFailures += 1
    }

    @Synchronized
    fun status(): List<NodeStatus> {
        val now = clock()
        return nodes.map { (host, node) ->
            val state = when {
                banned(node) -> "banned"
                !available(node, now) -> "blocked"
                node.delivered -> "healthy"
                else -> "untested"
            }
            NodeStatus(host, state, node.bps.toLong())
        }
    }

    companion object {
        private const val BAN_AFTER_EMPTY_FAILURES = 2
        private const val MEASUREMENT_TTL_MS = 90_000L
        private const val SPEED_SAMPLE_MIN_BYTES = 48L * 1024
        private const val DEFAULT_BPS = 512.0 * 1024
        private const val BENCH_MS = 15_000L
        private const val PREFERRED_BOOST = 4
    }
}
