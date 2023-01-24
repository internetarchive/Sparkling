package org.archive.webservices.sparkling.compression

import java.io.InputStream

import org.archive.webservices.sparkling.io.IOUtil

object Compression {

  def detect(in: InputStream, filename: Option[String] = None, checkFile: Boolean = true): DecompressionContext = {
    val format = {
      val format = if (filename.isDefined && checkFile) {
        if (Gzip.isCompressed(filename.get)) DecompressionContext.GzipFormat
        else if (Zstd.isCompressed(filename.get)) DecompressionContext.ZstdFormat
        else DecompressionContext.Uncompressed
      } else DecompressionContext.Uncompressed
      if (format > DecompressionContext.Uncompressed) format else {
        val buffered = IOUtil.supportMark(in)
        if (Gzip.isCompressed(buffered)) DecompressionContext.GzipFormat
        else if (Zstd.isCompressed(buffered)) DecompressionContext.ZstdFormat
        else DecompressionContext.Uncompressed
      }
    }
    DecompressionContext(format, filename)
  }

  def decompress(in: InputStream, filename: Option[String] = None, checkFile: Boolean = false): InputStream = if (checkFile && filename.isEmpty) in else {
    detect(in, filename, checkFile).decompress(in)
  }
}
