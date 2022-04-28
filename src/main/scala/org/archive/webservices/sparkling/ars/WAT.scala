package org.archive.webservices.sparkling.ars

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}

import org.apache.hadoop.fs.Path
import org.archive.extract.{ExtractingResourceFactoryMapper, ExtractingResourceProducer, WATExtractorOutput}
import org.archive.format.gzip.GZIPMemberSeries
import org.archive.resource.TransformingResourceProducer
import org.archive.resource.arc.ARCResourceFactory
import org.archive.resource.generic.GenericResourceProducer
import org.archive.resource.gzip.GZIPResourceContainer
import org.archive.resource.warc.WARCResourceFactory
import org.archive.streamcontext.SimpleStream
import org.archive.webservices.sparkling.Sparkling
import org.archive.webservices.sparkling.io.{GzipUtil, HdfsIO, IOUtil}
import org.archive.webservices.sparkling.util.StringUtil

import scala.util.Try

object WAT {
  def fromWarcFile[R](warcPath: String, watFilename: Option[String] = None): InputStream = {
    val in = HdfsIO.open(warcPath, decompress = false)
    fromWarcStream(in, new Path(warcPath).getName, watFilename, bubbleClose = true)
  }

  def fromWarcStream[R](stream: InputStream, inFilename: String, watFilename: Option[String] = None, bubbleClose: Boolean = false): InputStream = {
    val in = IOUtil.supportMark(stream)
    val isArc = StringUtil.stripSuffix(inFilename, Sparkling.GzipExt).toLowerCase.endsWith(Sparkling.ArcExt)

    val inputProducer =
      if (GzipUtil.isCompressed(in)) {
        val series = new GZIPMemberSeries(new SimpleStream(in), inFilename, 0, false)
        new GZIPResourceContainer(series)
      } else new GenericResourceProducer(new SimpleStream(in), inFilename)

    val transformingProducer = new TransformingResourceProducer(inputProducer, if (isArc) new ARCResourceFactory else new WARCResourceFactory)

    val extractingProducer = new ExtractingResourceProducer(transformingProducer, new ExtractingResourceFactoryMapper)

    new GzipCompressedWatInputStream(extractingProducer, watFilename, if (bubbleClose) Some(stream) else None)
  }

  class GzipCompressedWatInputStream(producer: ExtractingResourceProducer, watFilename: Option[String], in: Option[InputStream]) extends InputStream {
    private val buffer = new ByteArrayOutputStream()
    private val out = new WATExtractorOutput(buffer, watFilename.orNull)
    private var current: Option[ByteArrayInputStream] = None

    private def next: Option[ByteArrayInputStream] = {
      if (current.exists(_.available > 0)) current
      else {
        Try(producer.getNext).toOption.filter(_ != null) match {
          case Some(next) =>
            out.output(next)
            current = Some(new ByteArrayInputStream(buffer.toByteArray))
            buffer.reset()
          case None => current = None
        }
        current
      }
    }

    override def read(): Int = next.map(_.read).getOrElse(-1)

    override def read(b: Array[Byte]): Int = next.map(_.read(b)).getOrElse(-1)

    override def read(b: Array[Byte], off: Int, len: Int): Int = next.map(_.read(b, off, len)).getOrElse(-1)

    override def skip(n: Long): Long = next.map(_.skip(n)).getOrElse(0L)

    override def available(): Int = next.map(_.available()).getOrElse(0)

    override def markSupported(): Boolean = false

    override def close(): Unit = for (s <- in) s.close()
  }
}
