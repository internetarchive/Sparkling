package org.archive.webservices.sparkling.io

import org.archive.webservices.sparkling.logging.{Log, LogContext}
import org.archive.webservices.sparkling.util.{ConcurrencyUtil, IteratorUtil, StringUtil}

import java.io._
import java.time.Instant
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class SystemProcess private (
    val process: Process,
    private var _supportsEcho: Boolean) {
  implicit val logContext: LogContext = LogContext(this)

  private val _lock = new AnyRef()
  def lock[R](action: => R): R = _lock.synchronized(action)

  private val out: PrintStream = IOUtil.print(process.getOutputStream, autoFlush = true)
  private val in: BufferedInputStream = new BufferedInputStream(process.getInputStream)

  private var _locked = false
  private var _destroyed = false

  private var _lastError = Seq.empty[String]
  def lastError: Seq[String] = _lastError

  private val stderrFuture: Future[Boolean] = ConcurrencyUtil.future {
    val error = new BufferedReader(new InputStreamReader(process.getErrorStream))
    var line = error.readLine()
    while (line != null) {
      Log.error(s"Error: $line")
      _lastError :+= line
      line = error.readLine()
    }
    true
  }

  private var stdoutFuture: Option[Future[String]] = None
  private def stdoutLine: Future[String] = stdoutFuture.getOrElse(nextStdout())
  private def nextStdout(): Future[String] = {
    val future = ConcurrencyUtil.future {
      val line = StringUtil.readLine(in)
      Log.info("> " + line)
      line
    }
    stdoutFuture = Some(future)
    future
  }

  def destroyed: Boolean = _destroyed

  def destroy(): Unit = {
    process.destroy()
    in.close()
    out.close()
    _destroyed = true
  }

  def supportsEcho: Boolean = _supportsEcho

  def consumeAllInput(supportsEcho: Boolean = _supportsEcho): Unit = if (supportsEcho) synchronized {
    val endLine = s"${SystemProcess.CommandEndToken} ${Instant.now.toEpochMilli}"
    out.println(s"echo $endLine")
    consumeToLine(endLine)
  }

  def readAllInput(supportsEcho: Boolean = _supportsEcho): Iterator[String] = if (supportsEcho) synchronized {
    val endLine = s"${SystemProcess.CommandEndToken} ${Instant.now.toEpochMilli}"
    out.println(s"echo $endLine")
    readToLine(endLine, includeEnd = false)
  } else Iterator.empty

  private def readLine(
      pipe: Option[InputStream] = None,
      keepMaxBytes: Int = -1): String = synchronized {
    val lineFuture = pipe match {
      case Some(p) => Future(StringUtil.readLine(p, maxLength = keepMaxBytes))
      case None => stdoutLine
    }
    while (!lineFuture.isCompleted || (pipe.isDefined && stdoutLine.isCompleted)) {
      if (pipe.isDefined && stdoutLine.isCompleted) nextStdout()
      Thread.`yield`()
    }
    if (pipe.isEmpty) nextStdout()
    lineFuture.value.get.get
  }

  def readToLine(
      endLine: String,
      prefix: Boolean = false,
      includeEnd: Boolean = true,
      keepMaxBytes: Int = -1,
      pipe: Option[InputStream] = None): Iterator[String] = synchronized {
    var stop = false
    var remaining = keepMaxBytes
    IteratorUtil.whileDefined {
      if (!stop) {
        val line = readLine(pipe, if (keepMaxBytes < 0) -1 else if (remaining < 0) 0 else remaining)
        if (line == null) None
        else {
          stop = if (prefix) line.startsWith(endLine) else line == endLine
          if (!stop || includeEnd) Some {
            if (keepMaxBytes < 0) Some(line)
            else {
              if (remaining > 0) {
                remaining -= line.length
                Some(line)
              } else None
            }
          } else None
        }
      } else None
    }.flatten
  }

  def consumeToLine(endLine: String, prefix: Boolean = false, pipe: Option[InputStream] = None): Unit = synchronized {
    IteratorUtil.consume(readToLine(endLine, prefix, pipe = pipe))
  }

  def demandLine(cmd: String, line: String, sleepMillis: Int = 100, pipe: Option[InputStream] = None): Unit = synchronized {
    var stop = false
    while (!stop) {
      var reading = pipe.isEmpty
      val lineFuture = pipe match {
        case Some(p) => Future {
          reading = true
          StringUtil.readLine(p)
        }
        case None => stdoutLine
      }
      while (!reading) Thread.`yield`()
      while (!lineFuture.isCompleted || (pipe.isDefined && stdoutLine.isCompleted)) {
        if (pipe.isDefined && stdoutLine.isCompleted) nextStdout()
        Thread.`yield`()
        if (!lineFuture.isCompleted) {
          out.println(cmd)
          Thread.sleep(sleepMillis)
        }
      }
      stop = lineFuture.value.get.get == line
      if (pipe.isEmpty) nextStdout()
    }
  }

  def exec(
      cmd: String,
      clearInput: Boolean = true,
      blocking: Boolean = false,
      supportsEcho: Boolean = _supportsEcho,
      waitForLine: Option[String] = None,
      waitForPrefix: Boolean = false,
      pipe: Option[InputStream] = None): Unit = synchronized {
    if (clearInput) consumeAllInput()
    _lastError = Seq.empty
    out.println(cmd)
    _supportsEcho = supportsEcho
    if (blocking) consumeAllInput()
    for (waitLine <- waitForLine) consumeToLine(waitLine, waitForPrefix, pipe)
  }
}

object SystemProcess {
  val CommandEndToken = "END"

  def apply(process: Process, supportsEcho: Boolean = false): SystemProcess = {
    new SystemProcess(process, supportsEcho)
  }

  def bash: SystemProcess = exec("/bin/bash", supportsEcho = true)

  def exec(cmd: String, supportsEcho: Boolean = false): SystemProcess = {
    apply(Runtime.getRuntime.exec(cmd), supportsEcho)
  }
}
