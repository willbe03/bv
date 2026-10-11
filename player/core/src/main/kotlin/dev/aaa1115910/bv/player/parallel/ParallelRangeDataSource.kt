package dev.aaa1115910.bv.player.parallel

import android.net.Uri
import androidx.annotation.OptIn
import androidx.media3.common.C
import androidx.media3.common.PlaybackException
import androidx.media3.common.util.UnstableApi
import androidx.media3.datasource.BaseDataSource
import androidx.media3.datasource.DataSource
import androidx.media3.datasource.DataSourceException
import androidx.media3.datasource.DataSpec
import androidx.media3.datasource.HttpDataSource
import okhttp3.OkHttpClient
import java.io.IOException

/** Hands ExoPlayer one media file that [ParallelRangeLoader] downloads from several nodes. */
@OptIn(UnstableApi::class)
class ParallelRangeDataSource(
    private val client: OkHttpClient,
    private val urls: List<String>,
    private val headers: Map<String, String>,
    private val scoreboard: CdnScoreboard,
    private val threads: Int,
    private val windowBytes: Long
) : BaseDataSource(true) {
    private var loader: ParallelRangeLoader? = null
    private var uri: Uri? = null
    private var opened = false
    private var dataSpec: DataSpec? = null

    private fun wrap(e: IOException, dataSpec: DataSpec): IOException =
        if (e is ParallelRangeLoader.HttpStatusException) {
            HttpDataSource.InvalidResponseCodeException(e.code, null, e, emptyMap(), dataSpec, ByteArray(0))
        } else {
            DataSourceException(e, PlaybackException.ERROR_CODE_IO_NETWORK_CONNECTION_FAILED)
        }

    override fun open(dataSpec: DataSpec): Long {
        uri = dataSpec.uri
        transferInitializing(dataSpec)
        val loader = ParallelRangeLoader(client, urls, headers, scoreboard, threads, windowBytes)
        this.loader = loader
        val length = try {
            loader.open(dataSpec.position, dataSpec.length)
        } catch (e: IOException) {
            throw wrap(e, dataSpec)
        }
        this.dataSpec = dataSpec
        opened = true
        transferStarted(dataSpec)
        return length
    }

    override fun read(buffer: ByteArray, offset: Int, length: Int): Int {
        if (length == 0) return 0
        val count = try {
            loader!!.read(buffer, offset, length)
        } catch (e: IOException) {
            throw wrap(e, dataSpec!!)
        }
        if (count < 0) return C.RESULT_END_OF_INPUT
        bytesTransferred(count)
        return count
    }

    override fun getUri(): Uri? = uri

    override fun close() {
        loader?.close()
        loader = null
        uri = null
        dataSpec = null
        if (opened) {
            opened = false
            transferEnded()
        }
    }

    class Factory(
        private val client: OkHttpClient,
        private val urls: List<String>,
        private val headers: Map<String, String>,
        private val scoreboard: CdnScoreboard,
        private val threads: Int,
        private val windowBytes: Long
    ) : DataSource.Factory {
        override fun createDataSource() = ParallelRangeDataSource(client, urls, headers, scoreboard, threads, windowBytes)
    }
}
