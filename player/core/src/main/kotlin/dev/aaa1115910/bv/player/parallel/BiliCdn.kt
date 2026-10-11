package dev.aaa1115910.bv.player.parallel

import okhttp3.HttpUrl.Companion.toHttpUrlOrNull

enum class CdnMode { Mainland, Overseas }

/**
 * Bilibili's signed media addresses are accepted by every ordinary upos node, so one address
 * can be asked of many nodes at once.
 */
object BiliCdn {
    val mainlandHosts = listOf(
        "upos-sz-mirrorali.bilivideo.com",
        "upos-sz-mirrorhw.bilivideo.com",
        "upos-sz-mirrorbos.bilivideo.com",
        "upos-sz-mirror08c.bilivideo.com",
        "upos-sz-mirrorbd.bilivideo.com",
        "upos-sz-mirror14b.bilivideo.com",
        "upos-sz-estgoss.bilivideo.com",
        "upos-sz-mirrorcos.bilivideo.com"
    )

    val overseasHosts = listOf(
        "upos-sz-mirrorcosov.bilivideo.com",
        "upos-sz-mirroraliov.bilivideo.com",
        "cn-hk-eq-01-01.bilivideo.com",
        "cn-hk-eq-01-03.bilivideo.com"
    )

    private val mediaHost = Regex(
        """(?:^|\.)(?:bilivideo\.(?:com|cn|net)|akamaized\.net|szbdyd\.com|hdslb\.com|xycdn\.com|mountaintoys\.cn|nexusedgeio\.com|ahdohpiechei\.com)$""",
        RegexOption.IGNORE_CASE
    )
    private val mediaPath = Regex("""\.(?:m4s|mp4|flv)$""", RegexOption.IGNORE_CASE)

    fun isMediaUrl(url: String): Boolean {
        val parsed = url.toHttpUrlOrNull() ?: return false
        return parsed.isHttps && mediaHost.containsMatchIn(parsed.host) && mediaPath.containsMatchIn(parsed.encodedPath)
    }

    private val hosts = java.util.concurrent.ConcurrentHashMap<String, String>()

    fun hostOf(url: String): String {
        hosts[url]?.let { return it }
        if (hosts.size > 256) hosts.clear()
        return url.toHttpUrlOrNull()?.host.orEmpty().also { hosts[url] = it }
    }

    fun isAkamai(url: String) = hostOf(url).endsWith(".akamaized.net")

    /** A upos node, whose addresses every other upos node accepts; PCDN and MCDN ones differ. */
    fun isUpos(url: String) = hostOf(url).let { it.startsWith("upos-") || it in overseasHosts }

    fun withHost(url: String, host: String): String? =
        url.toHttpUrlOrNull()?.newBuilder()?.host(host)?.port(443)?.build()?.toString()

    /**
     * The addresses Bilibili handed out, then one upos address moved to every node of [mode].
     * Bilibili's own pick comes first, so the first piece starts where a single connection
     * would have; the downloader then favours whichever nodes prove fastest. Some overseas
     * accounts get nothing but akamaized.net upos addresses; the nodes accept those
     * signatures too, so only then are they moved instead.
     */
    fun candidateUrls(originals: List<String>, mode: CdnMode): List<String> {
        val urls = originals.filter(::isMediaUrl).distinct()
        val hosts = if (mode == CdnMode.Overseas) overseasHosts else mainlandHosts
        val upos = urls.filter(::isUpos)
        val donor = upos.firstOrNull { !isAkamai(it) }
        val moved = if (donor != null) hosts.mapNotNull { withHost(donor, it) }
        else hosts.flatMap { host -> upos.mapNotNull { withHost(it, host) } }
        return (urls + moved).distinct()
    }
}
