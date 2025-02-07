package org.archive.webservices.sparkling.warc

import java.io._
import com.google.common.io.CountingInputStream
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.archive.webservices.sparkling.cdx.CdxRecord
import org.archive.webservices.sparkling.compression.Gzip
import org.archive.webservices.sparkling.io.{ByteArray, ChainedInputStream, IOUtil, MemoryBufferInputStream}
import org.archive.webservices.sparkling.util.{IteratorUtil, RddUtil}

import scala.util.Try

object WarcLoader {
  def loadBytes(in: InputStream): Iterator[(Long, ByteArray)] = {
    var pos = 0L
    val buffered = new MemoryBufferInputStream(in)
    val counting = new CountingInputStream(buffered)
    if (Gzip.isCompressed(counting)) {
      Gzip.decompressConcatenated(counting).map { s =>
        IOUtil.readToEnd(s, close = true)
        val offset = pos
        pos = counting.getCount
        (offset, buffered.resetBuffer())
      }
    } else {
      IteratorUtil.whileDefined { WarcRecord.next(counting) }.map { warc =>
        warc.close()
        val offset = pos
        pos = counting.getCount
        (offset, buffered.resetBuffer())
      }
    }
  }

  def load(in: InputStream): Iterator[WarcRecord] = {
    var current: Option[WarcRecord] = None
    val s = IOUtil.supportMark(in)
    if (Gzip.isCompressed(s)) {
      Gzip.decompressConcatenated(s).flatMap { s =>
        IteratorUtil.whileDefined {
          if (current.isDefined) current.get.close()
          current = Try(WarcRecord.next(s)).getOrElse(None)
          current
        }
      }
    } else {
      IteratorUtil.whileDefined {
        if (current.isDefined) current.get.close()
        current = WarcRecord.next(s)
        current
      }
    }
  }

  def load(hdfsPath: String): RDD[WarcRecord] = RddUtil.loadBinary(hdfsPath, decompress = false, close = false) { (_, in) => IteratorUtil.cleanup(load(in), in.close) }
}
