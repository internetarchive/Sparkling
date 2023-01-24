package org.archive.webservices.sparkling.compression
import java.io.InputStream

class DecompressionContext private (val format: Int, val filename: Option[String] = None) {
  import DecompressionContext._

  def isUncompressed: Boolean = format == Uncompressed
  def isGzip: Boolean = format == GzipFormat
  def isZstd: Boolean = format == ZstdFormat

  private lazy val zstd = new Zstd
  def zstd[R](get: Zstd => R): Option[R] = if (isZstd) Some(get(zstd)) else None

  def decompress(in: InputStream): InputStream = {
    format match {
      case Uncompressed => in
      case GzipFormat => Gzip.decompress(in)
      case ZstdFormat => zstd.decompress(in)
    }
  }
}

object DecompressionContext {
  val Uncompressed = 0
  val GzipFormat = 1
  val ZstdFormat = 2

  lazy val uncompressed: DecompressionContext = new DecompressionContext(Uncompressed)
  lazy val gzip: DecompressionContext = new DecompressionContext(GzipFormat)
  lazy val zstd: DecompressionContext = new DecompressionContext(ZstdFormat)

  def uncompressed(filename: Option[String]): DecompressionContext = new DecompressionContext(Uncompressed, filename)
  def gzip(filename: Option[String]): DecompressionContext = new DecompressionContext(GzipFormat, filename)
  def zstd(filename: Option[String]): DecompressionContext = new DecompressionContext(ZstdFormat, filename)

  def apply(format: Int, filename: Option[String]): DecompressionContext = if (filename.isDefined) new DecompressionContext(format, filename) else format match {
    case GzipFormat => gzip
    case ZstdFormat => zstd
    case _ => uncompressed
  }
}