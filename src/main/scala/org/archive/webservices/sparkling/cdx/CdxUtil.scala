package org.archive.webservices.sparkling.cdx

import java.io.InputStream

import org.apache.hadoop.fs.Path
import org.archive.webservices.sparkling.io.{GzipUtil, HdfsIO}
import org.archive.webservices.sparkling.warc.WarcRecord

object CdxUtil {
  def fromWarcGzStream(path: String, stream: InputStream, filter: WarcRecord => Boolean = _ => true, digest: InputStream => String = WarcRecord.defaultDigestHash): Iterator[CdxRecord] = {
    val filename = new Path(path).getName
    val length = HdfsIO.length(path)
    (GzipUtil.decompressConcatenatedWithPosition(stream).map { case (pos, s) =>
      val warc = WarcRecord.next(s).filter(filter)
      (pos, warc.flatMap(_.toCdx(0).map(_.copy(additionalFields = Seq(pos.toString, filename)))))
    } ++ Iterator((length, None))).sliding(2).filter(_.size == 2).flatMap { case Seq((pos1, cdx1), (pos2, cdx2)) => cdx1.map(_.copy(compressedSize = pos2 - pos1)) }
  }
}
