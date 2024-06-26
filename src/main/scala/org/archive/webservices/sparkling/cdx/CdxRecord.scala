package org.archive.webservices.sparkling.cdx

import org.archive.webservices.sparkling.util.{RegexUtil, Time14Util}
import org.joda.time.DateTime

import scala.util.Try

object CdxRecord {
  def fromString(str: String): Option[CdxRecord] = {
    val split = str.trim.split("[ \t]")
    if (split.length < 7 || split.length == 8) return None
    val (url, timestamp, fullUrl, mimeType, statusStr, checksum, redirectUrl, meta, conpressedSizeStr) =
      if (split.length == 7) {
        (split(0), split(1), split(2), split(3), split(4), split(5), "-", "-", split(6)) // CDX server
      } else (split(0), split(1), split(2), split(3), split(4), split(5), split(6), split(7), split(8))
    try {
      val status = Try(statusStr.toInt).getOrElse(-1)
      Some(CdxRecord(
        url,
        RegexUtil.r("[^\\d]").replaceAllIn(timestamp, "").take(14),
        fullUrl,
        mimeType,
        status,
        checksum,
        redirectUrl,
        meta,
        conpressedSizeStr.toLong,
        split.drop(9)
      ))
    } catch { case e: Exception =>
      e.printStackTrace()
      None
    }
  }
}

case class CdxRecord(
    surtUrl: String,
    timestamp: String,
    originalUrl: String,
    mime: String,
    status: Int,
    digest: String,
    redirectUrl: String,
    meta: String,
    compressedSize: Long,
    additionalFields: Seq[String] = Seq.empty
) {
  def time: DateTime = Time14Util.parse(timestamp, fix = true)

  def toCdxString(additionalFields: Seq[String]): String = {
    val statusStr = if (status < 0) "-" else status.toString
    val additionalStr = if (additionalFields.nonEmpty) additionalFields.mkString(" ") else ""
    val escapeRedirectUrl = redirectUrl.replace(" ", "%20")
    s"$surtUrl $timestamp $originalUrl $mime $statusStr $digest $escapeRedirectUrl $meta $compressedSize $additionalStr".trim
  }

  def toCdxString(includeAdditionalFields: Boolean = true): String = toCdxString(if (includeAdditionalFields) additionalFields else Seq.empty)

  def toCdxString: String = toCdxString()

  def locationFromAdditionalFields: (String, Long) = {
    val (offset, offsetIdx) = additionalFields.zipWithIndex.reverse.drop(1).flatMap { case (field, i) =>
      Try(field.toLong).toOption.map((_, i))
    }.head
    (additionalFields.drop(offsetIdx + 1).mkString(" "), offset)
  }
}
