package org.archive.webservices.sparkling.compression

import java.io.{EOFException, InputStream, OutputStream}
import com.google.common.io.CountingInputStream
import org.apache.commons.compress.compressors.gzip.{GzipCompressorInputStream, GzipCompressorOutputStream}
import org.apache.commons.io.output.CountingOutputStream
import org.archive.webservices.sparkling.io.{ChainedInputStream, IOUtil, NonClosingInputStream, NonClosingOutputStream}
import org.archive.webservices.sparkling.util.IteratorUtil

import scala.util.Try

object Gzip extends Decompressor {
  import org.archive.webservices.sparkling.Sparkling._

  val Magic0 = 31
  val Magic1 = 139

  def isCompressed(in: InputStream): Boolean = {
    in.mark(2)
    try {
      val (b0, b1) = (in.read, in.read)
      b0 == Magic0 && b1 == Magic1
    } finally {
      in.reset()
    }
  }

  def seekGz(in: InputStream): Boolean = {
    try {
      var isGz = false
      in.mark(2)
      while (!isGz) {
        val b0 = in.read
        if (b0 == Magic0) {
          val b1 = in.read
          if (b1 == Magic1) {
            isGz = true
          } else {
            if (b1 == -1) return false
            in.mark(2)
          }
        } else {
          if (b0 == -1) return false
          in.mark(2)
        }
      }
      isGz
    } catch {
      case _: EOFException => false
    } finally {
      in.reset()
    }
  }

  def isCompressed(filename: String): Boolean = filename.toLowerCase.endsWith(GzipExt)

  def decompressConcatenated(in: InputStream): Iterator[InputStream] = decompressConcatenatedWithPosition(in).map(_._2)

  def decompressConcatenatedWithPosition(in: InputStream, appendNull: Boolean = false): Iterator[(Long, InputStream)] = {
    val stream = new CountingInputStream(IOUtil.supportMark(new NonClosingInputStream(in)))
    var current: Option[InputStream] = None
    var nullAppended = false
    IteratorUtil.whileDefined {
      if (nullAppended) None
      else {
        if (current.isDefined) IOUtil.readToEnd(current.get, close = true)
        val isGz = seekGz(stream)
        val pos = stream.getCount
        if (!isGz) {
          stream.close()
          if (appendNull) {
            nullAppended = true
            Some((pos, null))
          } else None
        } else Try {
          current = Some(IOUtil.supportMark(new GzipCompressorInputStream(new NonClosingInputStream(stream), false)))
          current.map((pos, _))
        }.getOrElse(None)
      }
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
      if (!IOUtil.eof(buffered)) new ChainedInputStream(decompressConcatenated(in)) // new GzipCompressorInputStream(buffered, true)
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
