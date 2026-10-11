package dev.aaa1115910.bv.player

import dev.aaa1115910.bv.player.parallel.CdnMode

data class VideoPlayerOptions(
    val userAgent: String? = null,
    val referer: String? = null,
    val enableFfmpegAudioRenderer: Boolean = false,
    val parallelDownload: ParallelDownloadOptions? = null
)

/** Download each stream from several CDN nodes at once, [threads] requests at a time. */
data class ParallelDownloadOptions(
    val threads: Int = 8,
    val cdnMode: CdnMode = CdnMode.Mainland
)
