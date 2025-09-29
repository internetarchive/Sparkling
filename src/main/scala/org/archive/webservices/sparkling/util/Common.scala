package org.archive.webservices.sparkling.util

import org.archive.webservices.sparkling.io.IOUtil
import org.archive.webservices.sparkling.logging.{Log, LogContext}

import java.io.{File, PrintStream, PrintWriter, StringWriter}
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.util.concurrent.{TimeUnit, TimeoutException}
import scala.util.Try

object Common {
  private lazy val CommonLogContext = LogContext(this)

  def lazyValWithCleanup[A](create: => A)(cleanup: A => Unit = (_: A) => {}): ManagedVal[A] = ManagedVal[A](
    create,
    {
      case Right(v) => cleanup(v)
      case Left(_)  =>
    }
  )

  def lazyVal[A](create: => A): ManagedVal[A] = ManagedVal[A](create, lazyEval = true)

  def cleanup[A](action: => A, catchCloseException: Boolean = true)(cleanup: () => Unit): A =
    try { action }
    finally { if (catchCloseException) Try(cleanup()) else cleanup() }

  def printThrow(msg: String): Nothing = {
    println(msg)
    throw new RuntimeException(msg)
  }

  def tryCatch[A](action: => A)(implicit context: LogContext = CommonLogContext): Option[A] = {
    try {
      Some(action)
    } catch {
      case e: Throwable =>
        Log.error(e)
        None
    }
  }

  def retry[R](times: Int = 30, sleepMillis: Int = 1000, log: (Int, Exception) => String)(run: Int => R)(implicit context: LogContext = CommonLogContext): R = {
    retryObj(Unit)(times, sleepMillis, log = (_, i, e) => log(i, e))((o, i) => run(i))
  }

  def retryObj[O, R](
      init: => O
  )(times: Int = 30, sleepMillis: Int = 1000, cleanup: O => Unit = (_: O) => {}, log: (Option[O], Int, Exception) => String)(run: (O, Int) => R)(implicit context: LogContext = CommonLogContext): R = {
    var lastException: Exception = null
    for (retry <- 0 to times) {
      if (retry > 0) Thread.sleep(sleepMillis)
      var obj: Option[O] = None
      try {
        obj = Some(init)
        return run(obj.get, retry)
      } catch {
        case e: Exception =>
          for (o <- obj) Try(cleanup(o))
          Log.error(log(obj, retry, e))
          e.printStackTrace()
          lastException = e
      }
    }
    throw lastException
  }

  class ProcessReporter private[util] () {
    private var _done: Boolean = false
    private var _time: Long = System.currentTimeMillis
    private var _status: Option[String] = None
    private var _idling: Boolean = false
    def isDone: Boolean = _done
    def lastAlive: Long = _time
    def lastStatus: Option[String] = _status
    def idling: Boolean = _idling
    def idle(): Unit = _idling = true
    def alive(): Unit = {
      _time = System.currentTimeMillis
      _idling = false
    }
    def alive(status: String): Unit = {
      _status = Some(status)
      alive()
    }
    def done(): Unit = {
      _done = true
      _time = System.currentTimeMillis
    }
    def done(status: String): Unit = {
      _done = true
      _status = Some(status)
      _time = System.currentTimeMillis
    }
  }

  def iterTimeout[R](millis: Long, iter: Iterator[R])(status: (Long, R) => String)(implicit context: LogContext): Iterator[R] = {
    iterTimeout(iter, millis, (idx: Long, item: R) => Some(status(idx, item)))
  }

  def iterTimeout[R](iter: Iterator[R], millis: Long, status: (Long, R) => Option[String] = (_: Long, _: R) => None)(implicit context: LogContext = CommonLogContext): Iterator[R] = {
    var item: Option[(R, Option[String])] = None
    var lastLog = System.currentTimeMillis
    var idx = -1L
    IteratorUtil.whileDefined {
      idx += 1
      item = timeout(millis, item.flatMap(_._2)) {
        if (iter.hasNext) Some {
          val item = iter.next()
          (item, status(idx, item))
        }
        else None
      }
      if (item.isDefined && System.currentTimeMillis - lastLog > millis) {
        Log.info("Process is alive..." + item.flatMap(_._2).map(status => s" - Last status: $status").getOrElse(""))
        lastLog = System.currentTimeMillis
      }
      item.map(_._1)
    }
  }

  def timeout[R](millis: Long, status: Option[String] = None)(action: => R)(implicit context: LogContext = CommonLogContext): R = {
    if (millis < 0) return action
    val task = ConcurrencyUtil.thread(useExecutionContext = false)(action)
    try { task.get(millis, TimeUnit.MILLISECONDS) }
    catch {
      case e: TimeoutException =>
        Log.info("Timeout after " + millis + " milliseconds" + status.map(status => s" - Last status: $status").getOrElse("") + ".")
        throw e
    } finally {
      while (!task.isDone) task.cancel(true)
      SparkUtil.removeTaskCleanup(task)
    }
  }

  def timeoutOpt[R](millis: Long, status: Option[String] = None)(action: => R)(implicit context: LogContext = CommonLogContext): Option[R] = {
    try { Some(timeout(millis, status)(action)) }
    catch { case _: TimeoutException => None }
  }

  def timeoutWithReporter[R](millis: Long)(action: ProcessReporter => R)(implicit context: LogContext = CommonLogContext): R = {
    val reporter = new ProcessReporter
    if (millis < 0) return action(reporter)
    val thread = ConcurrencyUtil.thread(useExecutionContext = false)(action(reporter))
    var lastAlive = reporter.lastAlive
    while (true) {
      try {
        if (reporter.isDone) {
          val result = thread.get()
          SparkUtil.removeTaskCleanup(thread)
          return result
        } else {
          val wait = lastAlive + millis - System.currentTimeMillis
          val result = thread.get(if (wait <= 0) millis else wait, TimeUnit.MILLISECONDS)
          SparkUtil.removeTaskCleanup(thread)
          return result
        }
      } catch {
        case e: TimeoutException =>
          if (reporter.idling) {
            lastAlive = System.currentTimeMillis
          } else {
            if (lastAlive == reporter.lastAlive) {
              Log.info("Timeout after " + millis + " milliseconds" + reporter.lastStatus.map(status => s" - Last status: $status").getOrElse("") + ".")
              while (!thread.isDone) thread.cancel(true)
              SparkUtil.removeTaskCleanup(thread)
              throw e
            } else {
              Log.info("Process is alive..." + reporter.lastStatus.map(status => s" - Last status: $status").getOrElse(""))
              lastAlive = reporter.lastAlive
            }
          }
        case e: Exception =>
          SparkUtil.removeTaskCleanup(thread)
          throw e
      }
    }
    unreachable
  }

  def touch[A](a: A)(touch: A => Unit): A = {
    touch(a)
    a
  }

  def tryOpt[A](a: => Option[A]): Option[A] = Try(a).toOption.flatten

  def sync[R](file: String)(action: => R): R = sync(new File(file))(action)

  def sync[R](file: File)(action: => R): R = {
    try {
      infinite {
        val channel = FileChannel.open(file.toPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
        try {
          val lock = channel.tryLock()
          if (lock != null) {
            try {
              return action
            } finally {
              lock.release()
            }
          }
        } finally {
          channel.close()
        }
      }
    } finally {
      IOUtil.delete(file)
    }
  }

  def synchronizedIf[R](obj: AnyRef, condition: => Boolean, sleep: => Unit = Thread.`yield`())(action: => R): R = {
    infinite {
      obj.synchronized {
        if (condition) return action
      }
      sleep
    }
    unreachable
  }

  def printStackTrace(print: Array[String] => Unit = _.foreach(println)): Unit = {
    try {
      throw new RuntimeException
    } catch {
      case e: RuntimeException =>
        val sw = new StringWriter
        val pw = new PrintWriter(sw)
        e.printStackTrace(pw)
        print(sw.toString.split('\n').map(_.trim))
    }
  }

  def infinite[R](action: => Unit): R = {
    while (true) {
      action
    }
    unreachable
  }

  def unreachable[R]: R = {
    throw new RuntimeException("this can/should never be reached")
  }
}
