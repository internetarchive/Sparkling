package org.archive.webservices.sparkling.util

import java.net.URL

import org.archive.url.WaybackURLKeyMaker

import scala.util.Try

object SurtUtil {
  val DefaultPorts = Set(80, 443)

  private lazy val keyMaker = new WaybackURLKeyMaker()
  private lazy val validHostPattern = "[a-z]+\\,[\\p{L}\\p{M}0-9\\-\\,]+".r

  def fromUrl(url: String): String = {
    val bracketIdx = url.indexOf(')')
    if (bracketIdx > -1 && bracketIdx < url.indexOf('/')) return url
    Try(keyMaker.makeKey(UrlUtil.decode(url))).getOrElse(url)
  }

  def fromUrl(url: String, baseUrl: String): String = {
    val resolved = new URL(new URL(toUrl(baseUrl)), url).toString
    fromUrl(resolved)
  }

  def toUrl(surt: String): String = {
    if (RegexUtil.matchesAbsoluteUrlStart(surt)) return surt
    val hostEnd = surt.indexOf(')')
    val (host, path) = if (hostEnd >= 0) surt.splitAt(hostEnd) else (surt, "")
    "http://" + host.split(',').reverse.mkString(".") + path.drop(1)
  }

  def urlToSurtPrefixes(url: String, subdomains: Boolean = true, subpaths: Boolean = true, urlInSurtFormat: Boolean = false): Set[String] = {
    val surt = if (urlInSurtFormat) url else SurtUtil.fromUrl(if (RegexUtil.matchesAbsoluteUrlStart(url)) url else "http://" + url)
    val hostPath = surt.split("\\)", 2)
    val site = hostPath(0).trim
    if (site.isEmpty) Set.empty
    else {
      val pathOpt = hostPath.drop(1).headOption.map(_.trim.stripPrefix("/").stripSuffix("/")).filter(_.nonEmpty)
      val hasPort = site.contains(":")
      val ports = Set("") ++ (if (hasPort) Set.empty else DefaultPorts.map(":" + _))
      pathOpt match {
        case Some(path) =>
          ports.flatMap { p =>
            Set(site + p + ")/" + path + " ") ++ {
              if (path.contains("?") || path.contains("#")) Set.empty
              else Set(site + p + ")/" + path + "?", site + p + ")/" + path + "#") ++ {
                if (subpaths) Set(site + p + ")/" + path + "/") else Set(site + p + ")/" + path + "/ ")
              }
            }
          }
        case None =>
          ports.map(site + _ + ")") ++ (if (subdomains && !hasPort) Set(site + ",") else Set.empty)
      }
    }
  }

  def host(surt: String): String = {
    val slash = surt.indexOf('/')
    val withPort = (if (slash < 0) surt else surt.take(slash)).stripSuffix(")")
    val port = withPort.indexOf(':')
    if (port < 0) withPort else withPort.take(port)
  }

  def validateHost(surt: String): Option[String] = {
    Some(host(surt)).filter { host =>
      validHostPattern.pattern.matcher(host).matches && ! { host.contains("--") || host.contains(",,") || host.split(',').exists(p => p.isEmpty || p.startsWith("-") || p.endsWith("-")) }
    }
  }

  def validate(url: String, urlInSurtFormat: Boolean = false): Option[String] = Some(if (urlInSurtFormat) url else fromUrl(url)).filter(validateHost(_).isDefined)
}
