package dev.aaa1115910.bv.player.impl.exo

import android.content.Context
import androidx.annotation.OptIn
import androidx.media3.common.C
import androidx.media3.common.MediaItem
import androidx.media3.common.PlaybackException
import androidx.media3.common.Player
import androidx.media3.common.util.UnstableApi
import androidx.media3.datasource.DataSource
import androidx.media3.datasource.okhttp.OkHttpDataSource
import androidx.media3.exoplayer.DefaultRenderersFactory
import androidx.media3.exoplayer.ExoPlayer
import androidx.media3.exoplayer.Renderer
import androidx.media3.exoplayer.source.MediaSource
import androidx.media3.exoplayer.source.MergingMediaSource
import androidx.media3.exoplayer.source.ProgressiveMediaSource
import dev.aaa1115910.bv.player.AbstractVideoPlayer
import dev.aaa1115910.bv.player.OkHttpUtil
import dev.aaa1115910.bv.player.VideoPlayerOptions
import dev.aaa1115910.bv.player.parallel.BiliCdn
import dev.aaa1115910.bv.player.parallel.CdnScoreboard
import dev.aaa1115910.bv.player.parallel.ParallelRangeDataSource
import dev.aaa1115910.bv.player.parallel.ParallelRangeLoader
import dev.aaa1115910.bv.util.formatHourMinSec
import io.github.oshai.kotlinlogging.KotlinLogging
import okhttp3.ConnectionPool
import okhttp3.OkHttpClient
import okhttp3.Protocol
import java.util.concurrent.TimeUnit

@OptIn(UnstableApi::class)
class ExoMediaPlayer(
    private val context: Context,
    private val options: VideoPlayerOptions
) : AbstractVideoPlayer(), Player.Listener {
    var mPlayer: ExoPlayer? = null
    protected var mMediaSource: MediaSource? = null

    private val okHttpClient = OkHttpUtil.generateCustomSslOkHttpClient(context)

    @OptIn(UnstableApi::class)
    private val dataSourceFactory =
        OkHttpDataSource.Factory(okHttpClient).apply {
            options.userAgent?.let { setUserAgent(it) }
            options.referer?.let { setDefaultRequestProperties(mapOf("referer" to it)) }
        }

    private val parallelClient by lazy { sharedParallelClient(okHttpClient) }
    private var scoreboard: CdnScoreboard? = null

    init {
        initPlayer()
    }

    @OptIn(UnstableApi::class)
    override fun initPlayer() {
        val renderersFactory = DefaultRenderersFactory(context).apply {
            setExtensionRendererMode(
                when (options.enableFfmpegAudioRenderer) {
                    true -> DefaultRenderersFactory.EXTENSION_RENDERER_MODE_ON
                    false -> DefaultRenderersFactory.EXTENSION_RENDERER_MODE_OFF
                }
            )
        }
        mPlayer = ExoPlayer
            .Builder(context)
            .setRenderersFactory(renderersFactory)
            .setSeekForwardIncrementMs(1000 * 10)
            .setSeekBackIncrementMs(1000 * 5)
            .build()

        initListener()
    }

    private fun initListener() {
        mPlayer?.addListener(this)
    }

    @OptIn(UnstableApi::class)
    override fun setHeader(headers: Map<String, String>) {

    }

    @OptIn(UnstableApi::class)
    override fun playUrl(
        videoUrl: String?,
        audioUrl: String?,
        videoBackupUrls: List<String>,
        audioBackupUrls: List<String>
    ) {
        scoreboard = options.parallelDownload?.let { CdnScoreboard() }
        val videoMediaSource = videoUrl?.let {
            ProgressiveMediaSource.Factory(dataSourceFactoryFor(it, videoBackupUrls, audio = false))
                .createMediaSource(MediaItem.fromUri(it))
        }
        val audioMediaSource = audioUrl?.let {
            ProgressiveMediaSource.Factory(dataSourceFactoryFor(it, audioBackupUrls, audio = true))
                .createMediaSource(MediaItem.fromUri(it))
        }

        val mediaSources = listOfNotNull(videoMediaSource, audioMediaSource)
        mMediaSource = MergingMediaSource(*mediaSources.toTypedArray())
    }

    @OptIn(UnstableApi::class)
    private fun dataSourceFactoryFor(url: String, backupUrls: List<String>, audio: Boolean): DataSource.Factory {
        val parallel = options.parallelDownload ?: return dataSourceFactory
        val urls = BiliCdn.candidateUrls(listOf(url) + backupUrls, parallel.cdnMode)
            .ifEmpty { listOf(url) }
        logger.info { "Parallel download from ${urls.map(BiliCdn::hostOf)} with ${parallel.threads} threads" }
        val headers = buildMap {
            options.userAgent?.let { put("User-Agent", it) }
            options.referer?.let { put("Referer", it) }
        }
        // Audio is a small fraction of the bytes; a few threads and a small window keep up.
        return if (audio) {
            ParallelRangeDataSource.Factory(parallelClient, urls, headers, scoreboard!!, minOf(parallel.threads, 4), 4L * 1024 * 1024)
        } else {
            ParallelRangeDataSource.Factory(parallelClient, urls, headers, scoreboard!!, parallel.threads, ParallelRangeLoader.DEFAULT_WINDOW_BYTES)
        }
    }

    @OptIn(UnstableApi::class)
    override fun prepare() {
        mPlayer?.setMediaSource(mMediaSource!!)
        mPlayer?.prepare()
    }

    override fun start() {
        mPlayer?.play()
    }

    override fun pause() {
        mPlayer?.pause()
    }

    override fun stop() {
        mPlayer?.stop()
    }

    override fun reset() {
        TODO("Not yet implemented")
    }

    override val isPlaying: Boolean
        get() = mPlayer?.isPlaying == true

    override fun seekTo(time: Long) {
        mPlayer?.seekTo(time)
    }

    override fun release() {
        mPlayer?.release()
    }

    override val currentPosition: Long
        get() = mPlayer?.currentPosition ?: 0
    override val duration: Long
        get() = mPlayer?.duration ?: 0
    override val bufferedPercentage: Int
        get() = mPlayer?.bufferedPercentage ?: 0

    override fun setOptions() {
        mPlayer?.playWhenReady = true
    }

    override var speed: Float
        get() = mPlayer?.playbackParameters?.speed ?: 1f
        set(value) {
            mPlayer?.setPlaybackSpeed(value)
        }
    override val tcpSpeed: Long
        get() = 0L

    override fun onPlaybackStateChanged(playbackState: Int) {
        logger.debug { "Playback state: $playbackState at ${mPlayer?.currentPosition} ms, buffered to ${mPlayer?.bufferedPosition} ms" }
        when (playbackState) {
            Player.STATE_IDLE -> mPlayerEventListener?.onIdle()
            Player.STATE_BUFFERING -> mPlayerEventListener?.onBuffering()
            Player.STATE_READY -> mPlayerEventListener?.onReady()
            Player.STATE_ENDED -> mPlayerEventListener?.onEnd()
        }
    }

    override fun onIsPlayingChanged(isPlaying: Boolean) {
        if (isPlaying) {
            mPlayerEventListener?.onPlay()
        } else {
            mPlayerEventListener?.onPause()
        }
    }

    override fun onSeekBackIncrementChanged(seekBackIncrementMs: Long) {
        mPlayerEventListener?.onSeekBack(seekBackIncrementMs)
    }

    override fun onSeekForwardIncrementChanged(seekForwardIncrementMs: Long) {
        mPlayerEventListener?.onSeekForward(seekForwardIncrementMs)
    }

    override val debugInfo: String
        get() {
            return """
                player: ${androidx.media3.common.MediaLibraryInfo.VERSION_SLASHY}
                time: ${currentPosition.formatHourMinSec()} / ${duration.formatHourMinSec()}
                buffered: $bufferedPercentage%
                resolution: ${mPlayer?.videoSize?.width} x ${mPlayer?.videoSize?.height}
                audio: ${mPlayer?.audioFormat?.bitrate ?: 0} kbps
                video codec: ${mPlayer?.videoFormat?.sampleMimeType ?: "null"}
                audio codec: ${mPlayer?.audioFormat?.sampleMimeType ?: "null"} (${getAudioRendererName()})
            """.trimIndent() + parallelDebugInfo()
        }

    private fun parallelDebugInfo(): String {
        val nodes = scoreboard?.status() ?: return ""
        val parallel = options.parallelDownload ?: return ""
        return "\nparallel: ${parallel.threads} threads, ${parallel.cdnMode}" + nodes.joinToString("") {
            "\n  ${it.host}: ${it.state}, ${it.bps * 8 / 1_000_000} Mbps"
        }
    }

    private fun getAudioRendererName(): String {
        val rendererCount = mPlayer?.rendererCount ?: return "UnknownRenderer"
        for (i in 0 until rendererCount) {
            val renderer = mPlayer!!.getRenderer(i)
            if (renderer.trackType == C.TRACK_TYPE_AUDIO && renderer.state == Renderer.STATE_STARTED) {
                return renderer.name
            }
        }
        return "UnknownRenderer"
    }

    override val videoWidth: Int
        get() = mPlayer?.videoSize?.width ?: 0
    override val videoHeight: Int
        get() = mPlayer?.videoSize?.height ?: 0

    companion object {
        private val logger = KotlinLogging.logger("ExoMediaPlayer")
        private var sharedClient: OkHttpClient? = null

        // One connection per request, since a CDN node limits each connection and HTTP/2 would
        // multiplex a node's pieces onto one. Shared by every player so idle sockets are bounded.
        @Synchronized
        private fun sharedParallelClient(base: OkHttpClient): OkHttpClient =
            sharedClient ?: base.newBuilder()
                .protocols(listOf(Protocol.HTTP_1_1))
                .connectionPool(ConnectionPool(32, 1, TimeUnit.MINUTES))
                .connectTimeout(5, TimeUnit.SECONDS)
                .readTimeout(5, TimeUnit.SECONDS)
                .build()
                .also { sharedClient = it }
    }

    override fun onPlayerError(error: PlaybackException) {
        mPlayerEventListener?.onError(error)
    }
}
