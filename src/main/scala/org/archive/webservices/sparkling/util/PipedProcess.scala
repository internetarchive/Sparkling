package org.archive.webservices.sparkling.util

import java.io.{PrintStream, PrintWriter}

import org.apache.spark.rdd.RDD
import org.archive.webservices.sparkling.att.AttachmentWriter.perRecordTimeoutMillis
import org.archive.webservices.sparkling.io.IOUtil
import org.archive.webservices.sparkling.logging.{Log, LogContext}

import scala.io.Source
import scala.util.Try

class PipedProcess(cmd: String, in: PrintStream => Unit, err: Iterator[String] => Unit, timeoutMillis: Long) {
  import PipedProcess._
  import org.archive.webservices.sparkling.Sparkling.executionContext

  private val proc: Process = Runtime.getRuntime.exec(cmd)
  def get: Process = proc

  private val procOut = IOUtil.print(proc.getOutputStream, true)
  private val procErr = proc.getErrorStream
  private val procIn = proc.getInputStream

  def close(): Unit = {
    procIn.close()
    procOut.close()
    procErr.close()
    if (Try(proc.exitValue).isFailure) proc.destroy()
  }

  private val inputFuture = ConcurrencyUtil.future {
    in(procOut)
    procOut.close()
  }

  inputFuture.onComplete { attempt => if (attempt.isFailure) close() }

  private val errFuture = ConcurrencyUtil.future {
    err(Source.fromInputStream(procErr).getLines)
    procErr.close()
  }

  errFuture.onComplete { attempt => if (attempt.isFailure) close() }

  private val dependencies = Seq(inputFuture, errFuture)
  private val inLines = Source.fromInputStream(procIn).getLines

  val out: Iterator[String] = IteratorUtil.cleanup(
    Iterator.continually { ConcurrencyUtil.await[Option[String]]({ if (inLines.hasNext) Some(inLines.next) else None }, timeoutMillis, dependencies) }.takeWhile(_.isDefined).map(_.get),
    close
  )
}

object PipedProcess {
  implicit val logContext: LogContext = LogContext(this)

  def apply(cmd: String, in: PrintStream => Unit = _ => {}, err: Iterator[String] => Unit = _ => {}, timeoutMillis: Long = -1): PipedProcess = new PipedProcess(cmd, in, err, timeoutMillis)

  def pipe[A](iter: TraversableOnce[(A, String)], commands: Seq[String]): Iterator[(A, String, String)] = {
    val filtered = commands.map(_.trim).filter(_.nonEmpty)
    pipe(iter, filtered.dropRight(1), filtered.takeRight(1))
  }

  def pipe[A](iter: TraversableOnce[(A, String)], setup: Seq[String], commands: Seq[String]): Iterator[(A, String, String)] = {
    val setupClean = setup.map(_.trim).filter(_.nonEmpty)
    val commandsClean = commands.map(_.trim).filter(_.nonEmpty)
    val (recordsIn, recordsOut) = iter.toIterator.duplicate

    val script = IOUtil.tmpFile(".sh")
    val scriptOut = new PrintWriter(script)
    if (setupClean.nonEmpty) {
      scriptOut.println("{")
      setupClean.foreach(scriptOut.println)
      scriptOut.println("} < /dev/null 1>&2") // redirect stdout to stderr and prevent stdin to be redirected to setup lines
    }
    commandsClean.foreach(scriptOut.println)
    scriptOut.close()

    val proc = PipedProcess(
      "/bin/bash " + script.getCanonicalPath,
      { in =>
        for ((_, input) <- recordsIn) {
          Log.debug(input)
          in.println(input)
          in.flush()
        }
      },
      _.foreach(Log.errln(_)),
      timeoutMillis = perRecordTimeoutMillis
    )

    IteratorUtil.cleanup(
      recordsOut.map { case (r, in) => (r, in, if (proc.out.hasNext) proc.out.next() else "") },
      () => {
        val exitCode = proc.get.waitFor()
        script.delete()
        if (exitCode != 0) throw new RuntimeException("pipeAttach failed: exit code " + exitCode)
      }
    )
  }

  def pipe[A](rdd: RDD[(A, String)], commands: Seq[String]): RDD[(A, String, String)] = {
    val filtered = commands.map(_.trim).filter(_.nonEmpty)
    pipe(rdd, filtered.dropRight(1), filtered.takeRight(1))
  }

  def pipe[A](rdd: RDD[(A, String)], setup: Seq[String], commands: Seq[String]): RDD[(A, String, String)] = rdd.mapPartitions { records => pipe(records, setup, commands) }
}
