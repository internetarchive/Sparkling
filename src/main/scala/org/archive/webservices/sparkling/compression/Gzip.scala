package org.archive.webservices.sparkling.compression

import java.io.{InputStream, OutputStream}
import com.google.common.io.CountingInputStream
import org.apache.commons.compress.compressors.gzip.{GzipCompressorInputStream, GzipCompressorOutputStream}
import org.apache.commons.io.output.CountingOutputStream
import org.archive.webservices.sparkling.io.{IOUtil, NonClosingInputStream, NonClosingOutputStream}
import org.archive.webservices.sparkling.util.IteratorUtil

import scala.util.Try

object Gzip extends Decompressor {
  import org.archive.webservices.sparkling.Sparkling._

  val Magic0 = 31
  val Magic1 = 139

  def isCompressed(in: InputStream): Boolean = {
    in.mark(2)
    val (b0, b1) = (in.read, in.read)
    in.reset()
    b0 == Magic0 && b1 == Magic1
  }

  def isCompressed(filename: String): Boolean = filename.toLowerCase.endsWith(GzipExt)

  def decompressConcatenated(in: InputStream): Iterator[InputStream] = decompressConcatenatedWithPosition(in).map { case (pos, s) => s }

  def decompressConcatenatedWithPosition(in: InputStream, appendNull: Boolean = false): Iterator[(Long, InputStream)] = {
    val stream = new CountingInputStream(IOUtil.supportMark(new NonClosingInputStream(in)))
    var current: Option[InputStream] = None
    var nullAppended = false
    IteratorUtil.whileDefined {
      if (current.isDefined) IOUtil.readToEnd(current.get, close = true)
      if (IOUtil.eof(stream)) {
        stream.close()
        if (appendNull && !nullAppended) {
          nullAppended = true
          Some((stream.getCount, null))
        } else None
      } else Try {
        val pos = stream.getCount
        current = Some(IOUtil.supportMark(new GzipCompressorInputStream(new NonClosingInputStream(stream), false)))
        current.map((pos, _))
      }.getOrElse(None)
    }
  }

  def estimateCompressionFactor(in: InputStream, readUncompressedBytes: Long): Double = {
    val stream = new CountingInputStream(IOUtil.supportMark(new NonClosingInputStream(in)))
    val uncompressed = new GzipCompressorInputStream(stream, true)
    var read = IOUtil.skip(uncompressed, readUncompressedBytes)
    val decompressed = stream.getCount
    while (decompressed == stream.getCount && !IOUtil.eof(uncompressed, markReset = false)) read += 1
    val factor = read.toDouble / decompressed
    uncompressed.close()
    factor
  }

  def decompress(in: InputStream, filename: Option[String] = None, checkFile: Boolean = false): InputStream = {
    if ((filename.isEmpty && !checkFile) || (filename.isDefined && filename.get.toLowerCase.endsWith(GzipExt))) {
      val buffered = IOUtil.supportMark(in)
      if (!IOUtil.eof(buffered)) new GzipCompressorInputStream(buffered, true)
      else buffered
    } else in
  }

  def compressOut(out: OutputStream)(gzip: GzipCompressorOutputStream => Unit): Unit = {
    val compressed = new GzipCompressorOutputStream(new NonClosingOutputStream(out))
    gzip(compressed)
    compressed.close()
  }

  def countCompressOut(out: OutputStream)(gzip: GzipCompressorOutputStream => Unit): Long = {
    val counting = new CountingOutputStream(new NonClosingOutputStream(out))
    val compressed = new GzipCompressorOutputStream(counting)
    gzip(compressed)
    compressed.close()
    counting.getByteCount
  }
}
