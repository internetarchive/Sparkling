package org.archive.webservices.sparkling.cdx

import org.apache.commons.io.input.ClosedInputStream

import java.io.{IOException, InputStream}
import org.apache.hadoop.fs.Path
import org.archive.webservices.sparkling.Sparkling.executionContext
import org.archive.webservices.sparkling.compression.Gzip
import org.archive.webservices.sparkling.io.{HdfsIO, InputStreamForker}
import org.archive.webservices.sparkling.warc.WarcRecord

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Try

object CdxUtil {
  def fromWarcGzStream(path: String, stream: InputStream, filter: WarcRecord => Boolean = _ => true, digest: (WarcRecord, InputStream) => String = (r, s) => r.payloadDigest.getOrElse(WarcRecord.defaultDigestHash(s))): Iterator[CdxRecord] = {
    val filename = new Path(path).getName
    Gzip.decompressConcatenatedWithPosition(stream, appendNull = true).map { case (pos, in) =>
      val warc = if (in == null) None else WarcRecord.next(in).filter(filter)
      (pos, warc.flatMap{r => r.toCdx(0, s => digest(r, s)).map(_.copy(additionalFields = Seq(pos.toString, filename)))})
    }.sliding(2).filter(_.size == 2).flatMap { case Seq((pos1, cdx1), (pos2, _)) => cdx1.map(_.copy(compressedSize = pos2 - pos1)) }
  }

  def warcGzStreamWithCdx[R](path: String, stream: InputStream, filter: WarcRecord => Boolean = _ => true, digest: (WarcRecord, InputStream) => String = (r, s) => r.payloadDigest.getOrElse(WarcRecord.defaultDigestHash(s)))(action: WarcRecord => R): Iterator[(R, Option[CdxRecord])] = {
    val filename = new Path(path).getName
    Gzip.decompressConcatenatedWithPosition(stream, appendNull = true).map { case (pos, in) =>
      if (in == null) {
        (pos, None)
      } else {
        val forker = InputStreamForker(in)
        val Array(cdxIn, warcIn) = forker.fork(2).map(Future(_))
        val Seq(cdx: Option[CdxRecord]@unchecked, warc: Option[R]@unchecked) = {
          try {
            Await.result(
              Future.sequence(
                Seq(
                  cdxIn.map { s =>
                    try {
                      WarcRecord.next(s).filter(filter).flatMap(r => r.toCdx(0, s => digest(r, s)).map(_.copy(additionalFields = Seq(pos.toString, filename))))
                    } finally {
                      s.close()
                    }
                  },
                  warcIn.map { s =>
                    try {
                      WarcRecord.next(s).filter(filter).map(action)
                    } finally {
                      s.close()
                    }
                  })),
              Duration.Inf)
          } finally {
            Try(in.close())
          }
        }
        (pos, warc.map(r => (r, cdx.map(_.copy(additionalFields = Seq(pos.toString, filename))))))
      }
    }.sliding(2).filter(_.size == 2).flatMap { case Seq((pos1, warcCdx), (pos2, _)) =>
      warcCdx.map { case (warc, cdx) =>
        (warc, cdx.map(_.copy(compressedSize = pos2 - pos1)))
      }
    }
  }
}
