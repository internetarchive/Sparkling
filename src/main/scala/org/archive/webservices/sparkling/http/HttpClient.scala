package org.archive.webservices.sparkling.http

import java.io.{BufferedInputStream, IOException, InputStream}
import java.net.{HttpURLConnection, URL, URLConnection}
import org.archive.webservices.sparkling.io.CleanupInputStream
import org.archive.webservices.sparkling.logging.{Log, LogContext}
import org.archive.webservices.sparkling.util.Common

import scala.collection.JavaConverters._
import scala.util.Try

object HttpClient {
  val DefaultRetries: Int = 30
  val DefaultSleepMillis: Int = 1000
  val DefaultTimeoutMillis: Int = -1

  implicit val logContext: LogContext = LogContext(this)

  def request[R](
      url: String,
      headers: Map[String, String] = Map.empty,
      retries: Int = DefaultRetries,
      sleepMillis: Int = DefaultSleepMillis,
      timeoutMillis: Int = DefaultTimeoutMillis,
      close: Boolean = true
  )(action: InputStream => R): R = rangeRequest(url, headers, retries = retries, sleepMillis = sleepMillis, timeoutMillis = timeoutMillis, close = close)(action)

  def rangeRequest[R](
      url: String,
      headers: Map[String, String] = Map.empty,
      offset: Long = 0,
      length: Long = -1,
      retries: Int = DefaultRetries,
      sleepMillis: Int = DefaultSleepMillis,
      timeoutMillis: Int = DefaultTimeoutMillis,
      close: Boolean = true
  )(action: InputStream => R): R = {
    rangeRequestConnection(url, headers, offset, length, retries, sleepMillis, timeoutMillis, disconnect = false) { case connection: HttpURLConnection =>
      val stream = Common.timeout(timeoutMillis)(connection.getInputStream)
      val in = new BufferedInputStream(new CleanupInputStream(stream, connection.disconnect))
      Common.cleanup(action(in))(() => if (close) in.close())
    }
  }

  def requestMessage[R](
      url: String,
      headers: Map[String, String] = Map.empty,
      retries: Int = DefaultRetries,
      sleepMillis: Int = DefaultSleepMillis,
      timeoutMillis: Int = DefaultTimeoutMillis,
      close: Boolean = true,
      retryOnHttpError: Boolean = true
  )(action: HttpMessage => R): R = {
    rangeRequestMessage(url, headers, retries = retries, sleepMillis = sleepMillis, timeoutMillis = timeoutMillis, close = close, retryOnHttpError = retryOnHttpError)(action)
  }

  def rangeRequestMessage[R](
      url: String,
      headers: Map[String, String] = Map.empty,
      offset: Long = 0,
      length: Long = -1,
      retries: Int = DefaultRetries,
      sleepMillis: Int = DefaultSleepMillis,
      timeoutMillis: Int = DefaultTimeoutMillis,
      close: Boolean = true,
      retryOnHttpError: Boolean = true
  )(action: HttpMessage => R): R = {
    rangeRequestConnection(url, headers, offset, length, retries, sleepMillis, timeoutMillis, disconnect = false, retryOnHttpError = retryOnHttpError) { case connection: HttpURLConnection =>
      val stream = Common.timeout(timeoutMillis)(connection.getInputStream)
      val in = new BufferedInputStream(new CleanupInputStream(stream, connection.disconnect))
      Common.cleanup({
        val responseHeaders = connection.getHeaderFields.asScala.toSeq.flatMap { case (k, v) => v.asScala.map((if (k == null) "" else k) -> _) }
        val message = new HttpMessage(connection.getHeaderField(0), responseHeaders, in)
        action(message)
      })(() => if (close) in.close())
    }
  }

  def requestConnection[R](
      url: String,
      headers: Map[String, String] = Map.empty,
      retries: Int = DefaultRetries,
      sleepMillis: Int = DefaultSleepMillis,
      timeoutMillis: Int = DefaultTimeoutMillis,
      disconnect: Boolean = true,
      retryOnHttpError: Boolean = true
  )(action: URLConnection => R): R = {
    rangeRequestConnection(url, headers, retries = retries, sleepMillis = sleepMillis, timeoutMillis = timeoutMillis, disconnect = disconnect, retryOnHttpError = retryOnHttpError)(action)
  }

  def rangeRequestConnection[R](
      url: String,
      headers: Map[String, String] = Map.empty,
      offset: Long = 0,
      length: Long = -1,
      retries: Int = DefaultRetries,
      sleepMillis: Int = DefaultSleepMillis,
      timeoutMillis: Int = DefaultTimeoutMillis,
      disconnect: Boolean = true,
      retryOnHttpError: Boolean = true
  )(action: URLConnection => R): R = {
    val descriptor = url + " (" + offset + "-" +
      (if (length >= 0) length else "") + ")"
    Log.info(s"Requesting: $descriptor...")
    val connection = Common.retryObj(new URL(url).openConnection.asInstanceOf[HttpURLConnection])(
      retries,
      sleepMillis,
      _.disconnect,
      (_, retry, e) => {
        "Request failed (" + retry + "/" + retries + "): " + descriptor +
          " - " + e.getClass.getCanonicalName + Option(e.getMessage).map(_.trim).filter(_.nonEmpty).map(" - " + _).getOrElse("")
      }
    ) { (connection, _) =>
      val redirect = Common.timeout(timeoutMillis * 2) {
        if (timeoutMillis >= 0) connection.setConnectTimeout(timeoutMillis)
        for ((key, value) <- headers) connection.addRequestProperty(key, value)
        if (offset > 0 || length >= 0) connection.addRequestProperty("Range", "bytes=" + offset + "-" + (if (length >= 0) offset + length - 1 else ""))
        connection.setInstanceFollowRedirects(false)
        val status = connection.getResponseCode
        Log.info(s"Status for $descriptor: $status")
        if (status / 100 == 3) {
          Log.info(s"Detected redirect for $descriptor, status: $status...")
          connection.getHeaderFields.asScala.filter(_._1 != null).find(_._1.toLowerCase == "location").flatMap {
            _._2.asScala.find(_ != null)
          }
        } else if (retryOnHttpError && status / 100 > 2) {
          throw new IOException(s"Unsuccessful status code for $descriptor: $status")
        } else None
      }
      for (location <- redirect) {
        Log.info(s"Redirecting $descriptor to $location...")
        Try(connection.disconnect())
        return rangeRequestConnection(location, headers, offset, length, retries, sleepMillis, timeoutMillis, disconnect, retryOnHttpError)(action)
      }
      Log.info(s"Successfully connected: $descriptor")
      connection
    }
    if (disconnect) Common.cleanup(action(connection))(connection.disconnect) else action(connection)
  }
}
