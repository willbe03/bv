package dev.aaa1115910.bv.player.parallel

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class CdnScoreboardTest {
    private var now = 1_000_000L
    private val scoreboard = CdnScoreboard { now }
    private val own = "https://upos-sz-mirrorcosov.bilivideo.com/v.m4s"
    private val fast = "https://upos-sz-mirrorali.bilivideo.com/v.m4s"
    private val cold = "https://upos-sz-mirrorhw.bilivideo.com/v.m4s"
    private val all = listOf(own, fast, cold)

    private fun deliver(url: String, bytesPerSecond: Long) {
        scoreboard.acquire(listOf(url))
        scoreboard.release(url)
        scoreboard.record(url, bytesPerSecond, 1000, completed = true)
    }

    @Test
    fun `a bench outlasts the node finishing another piece`() {
        deliver(own, 1_000_000)
        deliver(fast, 4_000_000)
        scoreboard.bench(fast)
        scoreboard.record(fast, 2_000_000, 500, completed = true)
        assertEquals(own, scoreboard.acquire(listOf(fast, own)))
        now += 16_000
        assertEquals(fast, scoreboard.acquire(listOf(fast, own)))
    }

    @Test
    fun `best speed leaves out benched, blocked and excluded nodes`() {
        deliver(own, 1_000_000)
        deliver(fast, 4_000_000)
        assertEquals(4_000_000.0, scoreboard.bestSpeed(all, emptySet()))
        scoreboard.bench(fast)
        assertEquals(1_000_000.0, scoreboard.bestSpeed(all, emptySet()))
        assertNull(scoreboard.bestSpeed(all, setOf(BiliCdn.hostOf(own))))
        now += 16_000
        scoreboard.failure(fast, 0)
        assertEquals(1_000_000.0, scoreboard.bestSpeed(all, emptySet()))
    }

    @Test
    fun `an urgent piece goes only to Bilibili's own node or nodes that have delivered`() {
        repeat(6) { assertEquals(own, scoreboard.acquire(all, urgent = true)) }
        deliver(fast, 4_000_000)
        val picks = List(6) { scoreboard.acquire(all, urgent = true) }
        assertEquals(setOf(own, fast), picks.toSet())
    }

    @Test
    fun `rate limiting backs a node off without banning it`() {
        repeat(3) {
            scoreboard.failure(own, 0, transient = true)
            now += 61_000
        }
        assertEquals("untested", scoreboard.status().single { it.host == BiliCdn.hostOf(own) }.state)
        scoreboard.failure(cold, 0)
        scoreboard.failure(cold, 0)
        assertEquals("banned", scoreboard.status().single { it.host == BiliCdn.hostOf(cold) }.state)
    }
}
