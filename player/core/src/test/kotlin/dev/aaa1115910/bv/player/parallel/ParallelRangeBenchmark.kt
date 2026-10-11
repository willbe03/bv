package dev.aaa1115910.bv.player.parallel

import okhttp3.ConnectionPool
import okhttp3.OkHttpClient
import okhttp3.Protocol
import java.io.File
import java.util.concurrent.TimeUnit
import kotlin.test.Test

/** Downloads a real signed address for a while; only runs when BV_BENCH_URL_FILE is set. */
class ParallelRangeBenchmark {
    @Test
    fun benchmark() {
        val url = System.getenv("BV_BENCH_URL_FILE")?.let { File(it).readText().trim() } ?: return
        val threads = System.getenv("BV_BENCH_THREADS")?.toInt() ?: 8
        val mode = if (System.getenv("BV_BENCH_MODE") == "overseas") CdnMode.Overseas else CdnMode.Mainland
        val seconds = System.getenv("BV_BENCH_SECONDS")?.toInt() ?: 20
        val client = OkHttpClient.Builder()
            .protocols(listOf(Protocol.HTTP_1_1))
            .connectionPool(ConnectionPool(64, 5, TimeUnit.MINUTES))
            .connectTimeout(5, TimeUnit.SECONDS)
            .readTimeout(5, TimeUnit.SECONDS)
            .build()
        val urls = BiliCdn.candidateUrls(listOf(url), mode).ifEmpty { listOf(url) }
        val scoreboard = CdnScoreboard()
        val headers = mapOf("User-Agent" to "Mozilla/5.0", "Referer" to "https://www.bilibili.com")
        ParallelRangeLoader(client, urls, headers, scoreboard, threads).use { loader ->
            val started = System.currentTimeMillis()
            kotlin.concurrent.thread(isDaemon = true) {
                Thread.sleep(seconds * 1000L + 15_000)
                println("watchdog: no progress, closing")
                loader.close()
            }
            val length = loader.open(0, -1)
            println("open took ${System.currentTimeMillis() - started} ms, total $length")
            val buffer = ByteArray(64 * 1024)
            var total = 0L
            var second = 0L
            var secondStart = System.currentTimeMillis()
            val line = StringBuilder()
            while (System.currentTimeMillis() - started < seconds * 1000L) {
                val count = runCatching { loader.read(buffer, 0, buffer.size) }.getOrElse { -1 }
                if (count < 0) break
                total += count
                second += count
                val now = System.currentTimeMillis()
                if (now - secondStart >= 1000) {
                    line.append(" ").append(second * 8 / (now - secondStart) / 1000)
                    second = 0
                    secondStart = now
                }
            }
            val elapsed = System.currentTimeMillis() - started
            println("mode=$mode threads=$threads: ${total / 1048576} MiB in ${elapsed / 1000.0}s = ${total * 8 / elapsed / 1000} Mbit/s")
            println("Mbit/s per second:$line")
            scoreboard.status().forEach { println("  ${it.host}: ${it.state} ${it.bps * 8 / 1000} kbit/s") }
        }
    }
}
