package org.archive.webservices.sparkling.warc

import java.io.InputStream

import org.apache.commons.io.input.BoundedInputStream
import org.archive.webservices.sparkling.cdx.CdxRecord
import org.archive.webservices.sparkling.compression.{Compression, DecompressionContext, Gzip, Zstd}
import org.archive.webservices.sparkling.http.HttpMessage
import org.archive.webservices.sparkling.io.IOUtil
import org.archive.webservices.sparkling.logging.LogContext
import org.archive.webservices.sparkling.util.{DigestUtil, RegexUtil, StringUtil, SurtUtil}

import scala.util.Try

class WarcRecord(val versionStr: String, val headers: Seq[(String, String)], val in: InputStream) {
  import WarcRecord._

  lazy val headerMap: Map[String, String] = headers.map { case (k, v) => (k.toLowerCase, v) }.toMap

  def contentLength: Option[Long] = headerMap.get("content-length").flatMap(l => Try { l.trim.toLong }.toOption)
  def url: Option[String] = headerMap.get("warc-target-uri").map(_.trim)
  def contentType: Option[String] = headerMap.get("content-type").map(_.split(';').head.trim)
  def timestamp: Option[String] = headerMap.get("warc-date").map(RegexUtil.r("[^\\d]").replaceAllIn(_, "").take(14))
  def warcType: Option[String] = headerMap.get("warc-type").map(_.trim.toLowerCase)
  def payloadDigest: Option[String] = headerMap.get("warc-payload-digest").map(_.trim.toLowerCase)
  def isRevisit: Boolean = warcType.contains("revisit")
  def isResponse: Boolean = warcType.contains("response")

  lazy val payload: InputStream = IOUtil.supportMark(contentLength match {
    case Some(length) =>
      val bounded = new BoundedInputStream(in, length)
      bounded.setPropagateClose(false)
      bounded
    case None => in
  })

  def close(): Unit = if (contentLength.isDefined) IOUtil.readToEnd(payload)

//  lazy val isHttp: Boolean = contentType.contains("application/http") // found a number of records with mime types, such as text/html, in content-type header
  lazy val http: Option[HttpMessage] = HttpMessage.get(payload) //if (isHttp) HttpMessage.get(payload) else None

  def digestPayload(hash: InputStream => String = defaultDigestHash): String = hash(http.map(_.payload).getOrElse(payload))

  def toCdx(
      compressedSize: Long,
      digest: InputStream => String = s => payloadDigest.getOrElse(defaultDigestHash(s)),
      handleRevisits: Boolean = true,
      handleOthers: Boolean = false
  ): Option[CdxRecord] = {
    if (isResponse || (handleRevisits && isRevisit) || handleOthers) url.map(SurtUtil.fromUrl).map { surt =>
      val mime = if (isResponse) http.flatMap(_.mime).getOrElse("-") else warcType.map("warc/" + _).getOrElse("-")
      val status = http.map(_.status).getOrElse(-1)
      val redirectUrl = http.flatMap(_.redirectLocation).getOrElse("-")
      CdxRecord(surt, timestamp.getOrElse("-"), url.getOrElse("-"), mime, status, digestPayload(digest), redirectUrl, "-", compressedSize)
    }
    else None
  }
}

object WarcRecord {
  implicit val logContext: LogContext = LogContext(this)

  val Charset = "UTF-8"
  val WarcRecordStart = "WARC/"

  def defaultDigestHash(in: InputStream): String = "sha1:" + DigestUtil.sha1Base32(in)

  def get(in: InputStream, handleArc: Boolean = true, compressed: Boolean = true)(implicit context: DecompressionContext = null): Option[WarcRecord] = {
    next(if (compressed) {
      if (context == null) Compression.decompress(in)
      else context.decompress(in)
    } else in, handleArc)
  }

  def next(in: InputStream, handleArc: Boolean = true): Option[WarcRecord] = {
    var line = StringUtil.readLine(in, Charset)
    while (
      line != null && !{
        if (line.startsWith(WarcRecordStart)) {
          val versionStr = line
          val headers = collection.mutable.Buffer.empty[(String, String)]
          line = StringUtil.readLine(in, Charset)
          while (line != null && line.trim.nonEmpty) {
            val split = line.split(":", 2)
            if (split.length == 2) headers += ((split(0).trim, split(1).trim))
            line = StringUtil.readLine(in, Charset)
          }
          return Some(new WarcRecord(versionStr, headers, in))
        }
        false
      } && (!handleArc || !{
        if (RegexUtil.matchesAbsoluteUrlStart(line) && !line.startsWith("filedesc:")) {
          val split = line.split(" ")
          // https://archive.org/web/researcher/ArcFileFormat.php
          if (split.length == 5) {
            val versionStr = "ARC/1"
            /*
URL-record-v1 == <url><sp>
<ip-address><sp>
<archive-date><sp>
<content-type><sp>
<length><nl>
             */
            val headers = Seq(
              "WARC-Type" -> "response",
              "WARC-Target-URI" -> split(0),
              "WARC-Date" -> split(2),
              "WARC-IP-Address" -> split(1),
              "Content-Type" -> split(3),
              "Content-Length" -> split(4)
            )
            return Some(new WarcRecord(versionStr, headers, in))
          } else if (split.length == 10) {
            val versionStr = "ARC/2"
            /*
URL-record-v2 == <url><sp>
<ip-address><sp>
<archive-date><sp>
<content-type><sp>
<result-code><sp>
<checksum><sp>
<location><sp>
<offset><sp>
<filename><sp>
<length><nl>
             */
            val headers = Seq(
              "WARC-Type" -> "response",
              "WARC-Target-URI" -> split(0),
              "WARC-Date" -> split(2),
              "WARC-IP-Address" -> split(1),
              "WARC-Payload-Digest" -> split(5),
              "ARC-Record-Location" -> (split(8) + ":" + split(7)),
              "Location" -> split(6),
              "Content-Type" -> split(3),
              "Result-Code" -> split(4),
              "Content-Length" -> split(9)
            )
            return Some(new WarcRecord(versionStr, headers, in))
          }
        }
        false
      })
    ) line = StringUtil.readLine(in, Charset)
    None
  }
}
