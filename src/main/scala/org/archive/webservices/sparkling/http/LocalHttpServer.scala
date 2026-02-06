package org.archive.webservices.sparkling.http

import fi.iki.elonen.NanoHTTPD
import fi.iki.elonen.NanoHTTPD._
import org.archive.webservices.sparkling.io.{CatchingInputStream, CleanupInputStream, IOUtil}
import org.archive.webservices.sparkling.warc.WarcRecord

import java.io.{ByteArrayInputStream, FilterOutputStream, InputStream, OutputStream}
import java.nio.charset.Charset
import scala.util.Try

object LocalHttpServer {
  def findFreePort(): Int = {
    val socket = new java.net.ServerSocket(0)
    val port = socket.getLocalPort
    socket.close()
    port
  }

  def start(port: Int = 0, timeout: Int = 0): LocalHttpServer = {
    val p = if (port == 0) findFreePort() else port
    val server = new LocalHttpServer(p)
    server.start(timeout)
    server
  }
}

class LocalHttpServer(val port: Int) extends NanoHTTPD(port) {
  private var serveMethod: NanoHTTPD.IHTTPSession => Response = super.serve
  private val exceptions = collection.mutable.ArrayBuffer.empty[Exception]

  def collectExceptions: Seq[Exception] = exceptions.synchronized {
    try {
      exceptions.toList
    } finally {
      exceptions.clear()
    }
  }

  def setServe(method: NanoHTTPD.IHTTPSession => Response): Unit = serveMethod = method

  def setServeWarc(warc: String => Option[WarcRecord]): Unit = setServeWarc(warc, close = false)

  def setServeWarc(warc: String => Option[WarcRecord], close: Boolean): Unit = {
    serveMethod = { s =>
      val q = Option(s.getQueryParameterString).map(_.trim).filter(_.nonEmpty).map("?" + _).getOrElse("")
      val url = s.getUri.stripPrefix("/") + q
      Try(warc(url)).toOption.flatten match {
        case Some(warc) =>
          warc.http match {
            case Some(http) =>
              Option(Response.Status.lookup(http.status)).map { status =>
                val mimeType = http.contentType.orNull
                val data = new CleanupInputStream(http.body, () => {
                  if (close) warc.in.close() else warc.close()
                })
                val response = new LocalHttpServerResponse(status, mimeType, data, -1) //new CatchingInputStream(data), -1) // -1 == chunked

                for (location <- http.headerMap.get("location")) response.addHeader("Location", location)

                response.addHeader("Access-Control-Allow-Origin", "*")
                response.addHeader("Cache-Control", "no-store, no-cache, must-revalidate")
                response.addHeader("Pragma", "no-cache")
                response.addHeader("Expires", "0")

                response
              }.getOrElse(notFound)
            case None => notFound
          }
        case None => notFound
      }
    }
  }

  override def serve(session: NanoHTTPD.IHTTPSession): Response = serveMethod(session)

  def notFound: Response = newFixedLengthResponse(Response.Status.NOT_FOUND, NanoHTTPD.MIME_PLAINTEXT, "Not Found")

  def newFixedLengthResponse(status: Response.IStatus, mimeType: String, txt: String): NanoHTTPD.Response = {
    // copied from NanoHTTPD#newFixedLengthResponse
    var contentType = new NanoHTTPD.ContentType(mimeType)
    if (txt == null) new LocalHttpServerResponse(status, mimeType, new ByteArrayInputStream(new Array[Byte](0)), 0)
    else {
      var bytes: Array[Byte] = null
      try {
        val newEncoder = Charset.forName(contentType.getEncoding).newEncoder
        if (!newEncoder.canEncode(txt)) contentType = contentType.tryUTF8
        bytes = txt.getBytes(contentType.getEncoding)
      } catch {
        case e: Exception =>
          exceptions.synchronized {
            exceptions += e
          }
          bytes = new Array[Byte](0)
      }
      new LocalHttpServerResponse(status, contentType.getContentTypeHeader, new ByteArrayInputStream(bytes), bytes.length)
    }
  }

  private class WriteException(val cause: Exception) extends Exception(cause)

  class CatchingOutputStream(outputStream: OutputStream) extends FilterOutputStream(outputStream) {
    def doCatch(action: => Unit): Unit = {
      try {
        action
      } catch {
        case e: Exception =>
          throw new WriteException(e) // wrap it to not get catched by Response#send
      }
    }

    override def write(b: Int): Unit = doCatch(super.write(b))
    override def write(b: Array[Byte]): Unit = doCatch(super.write(b))
    override def write(b: Array[Byte], off: Int, len: Int): Unit = doCatch(super.write(b, off, len))
    override def flush(): Unit = doCatch(super.flush())
    override def close(): Unit = doCatch(super.close())
  }

  class LocalHttpServerResponse (status: Response.IStatus, mimeType: String, data: InputStream, totalBytes: Long = -1) extends Response(status, mimeType, data, totalBytes) {
    override def send(outputStream: OutputStream): Unit = try {
      super.send(new CatchingOutputStream(outputStream))
    } catch {
      case e: WriteException =>
        exceptions.synchronized {
          exceptions += e.cause
        }
      case e: Exception =>
        exceptions.synchronized {
          exceptions += e
        }
    }
  }
}