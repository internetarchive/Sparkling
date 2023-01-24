package org.archive.webservices.sparkling.ars

import java.io.InputStream

import org.apache.commons.io.input.BoundedInputStream
import org.apache.hadoop.fs.Path
import org.archive.extract.{ExtractingResourceFactoryMapper, ExtractingResourceProducer, WATExtractorOutput}
import org.archive.format.gzip.GZIPMemberSeries
import org.archive.resource.arc.ARCResourceFactory
import org.archive.resource.generic.GenericResourceProducer
import org.archive.resource.gzip.GZIPResourceContainer
import org.archive.resource.http.HTTPResponseResource
import org.archive.resource.warc.WARCResourceFactory
import org.archive.resource.{MetaData, Resource, ResourceConstants, ResourceContainer, ResourceProducer, TransformingResourceProducer}
import org.archive.streamcontext.SimpleStream
import org.archive.webservices.sparkling.Sparkling
import org.archive.webservices.sparkling.compression.Gzip
import org.archive.webservices.sparkling.io.{HdfsIO, IOUtil, PipingOutputStream}
import org.archive.webservices.sparkling.util._
import scala.collection.JavaConverters._

import scala.util.Try

object WAT {
  private val extractingResourceFactoryMapper = new ExtractingResourceFactoryMapper
  private val arcFactory = new ARCResourceFactory
  private val warcFactory = new WARCResourceFactory

  def fromWarcFile[R](warcPath: String, watFilename: Option[String] = None, maxHtmlContentLength: Int = -1): InputStream = {
    val in = HdfsIO.open(warcPath, decompress = false)
    fromWarcStream(in, new Path(warcPath).getName, watFilename, maxHtmlContentLength, bubbleClose = true)
  }

  def fromWarcStream[R](stream: InputStream, inFilename: String, watFilename: Option[String] = None, maxHtmlContentLength: Int = -1, bubbleClose: Boolean = false): InputStream = {
    val in = IOUtil.supportMark(stream)
    val isArc = StringUtil.stripSuffix(inFilename.toLowerCase, Sparkling.GzipExt).endsWith(Sparkling.ArcExt)

    val inputProducer =
      if (Gzip.isCompressed(in)) {
        val series = new GZIPMemberSeries(new SimpleStream(in), inFilename, 0, false)
        new GZIPResourceContainer(series)
      } else new GenericResourceProducer(new SimpleStream(in), inFilename)

    val transformingProducer = new TransformingResourceProducer(inputProducer, if (isArc) arcFactory else warcFactory)

    val filterProducer = if (maxHtmlContentLength < 0) transformingProducer else new MaxHtmlContentLengthResourceProducer(transformingProducer, maxHtmlContentLength)

    val extractingProducer = new ExtractingResourceProducer(filterProducer, extractingResourceFactoryMapper)

    new GzipCompressedWatInputStream(extractingProducer, watFilename, if (bubbleClose) Some(stream) else None)
  }

  class MaxHtmlContentLengthResourceProducer(producer: ResourceProducer, maxHtmlContentLength: Int) extends ResourceProducer {
    val EvenlopeProperty = "Envelope"
    val WarcHeaderMetadataProperty = "WARC-Header-Metadata"
    val ContentLengthProperty = "Content-Length"
    val HttpContentTypeProperty = "content-type"
    val HtmlMime = "text/html"

    override def getNext: Resource = Iterator.continually(producer.getNext).map { resource =>
      lazy val factory = extractingResourceFactoryMapper.mapResourceToFactory(resource)
      if (resource == null || factory == null) resource else {
        factory.getResource(resource.getInputStream, resource.getMetaData, resource.getContainer)
      }
    }.find { resource =>
      resource == null || !resource.isInstanceOf[HTTPResponseResource] || {
        val meta = resource.getMetaData
        meta == null || {
          val topMeta = meta.getTopMetaData
          topMeta == null || !topMeta.has(EvenlopeProperty) || {
            val envelope = topMeta.getJSONObject(EvenlopeProperty)
            !envelope.has(WarcHeaderMetadataProperty) || {
              val warcMeta = envelope.getJSONObject(WarcHeaderMetadataProperty)
              !warcMeta.has(ContentLengthProperty) || warcMeta.getLong(ContentLengthProperty) < maxHtmlContentLength || {
                !meta.has(ResourceConstants.HTTP_HEADERS_LIST) || {
                  val headers = meta.getJSONObject(ResourceConstants.HTTP_HEADERS_LIST)
                  val lowerCase = headers.keys.asScala.filter(_.isInstanceOf[String]).map(_.asInstanceOf[String]).map(k => k.toLowerCase -> k).toMap
                  !lowerCase.contains(HttpContentTypeProperty) || {
                    val contentType = headers.getString(lowerCase(HttpContentTypeProperty))
                    StringUtil.prefixBySeparator(contentType, ";").trim.toLowerCase != HtmlMime
                  }
                }
              }
            }
          }
        }
      }
    }.orNull

    override def close(): Unit = producer.close()
    override def getContext: String = producer.getContext
  }

  class GzipCompressedWatInputStream(producer: ExtractingResourceProducer, watFilename: Option[String], in: Option[InputStream]) extends InputStream {
    private val pipe = new PipingOutputStream()
    private val out = new WATExtractorOutput(pipe, watFilename.orNull)
    private var current: Option[InputStream] = None

    private def next: Option[InputStream] = {
      if (current.exists(_.available > 0)) current
      else {
        for (s <- current) Try(s.close())
        Try(producer.getNext).toOption.filter(_ != null) match {
          case Some(next) =>
            current = IOUtil.buffer(lazyEval = false) { b =>
              pipe.set(b)
              out.output(next)
            }.option.map(_.get)
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

    override def close(): Unit = {
      Try(producer.close())
      for (s <- current) Try(s.close())
      for (s <- in) s.close()
    }
  }
}
