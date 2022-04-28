package org.archive.webservices.sparkling.util

import java.util.concurrent.FutureTask

import org.archive.webservices.sparkling.Sparkling
import org.archive.webservices.sparkling.logging.LogContext

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

object ConcurrencyUtil {
  var executionContext: ExecutionContextExecutor = Sparkling.executionContext

  def thread[A](useExecutionContext: Boolean = true)(action: => A): FutureTask[A] = {
    val parentTaskContext = Sparkling.taskContext
    val future = new FutureTask[A](() => {
      if (parentTaskContext.isDefined) Sparkling.parentTaskContext.set(parentTaskContext.get)
      val a = action
      Sparkling.parentTaskContext.remove()
      a
    })
    if (useExecutionContext) executionContext.execute(future) else new Thread(future).start()
    SparkUtil.cleanupTask(future, () => while (!future.isDone) future.cancel(true))
    future
  }

  def future[A](action: => A): Future[A] = {
    val parentTaskContext = Sparkling.taskContext
    Future {
      if (parentTaskContext.isDefined) Sparkling.parentTaskContext.set(parentTaskContext.get)
      val a = action
      Sparkling.parentTaskContext.remove()
      a
    }(executionContext)
  }

  def mapFuture[A, B](future: Future[A])(map: A => B): Future[B] = {
    val parentTaskContext = Sparkling.taskContext
    future.map { a =>
      if (parentTaskContext.isDefined) Sparkling.parentTaskContext.set(parentTaskContext.get)
      val b = map(a)
      Sparkling.parentTaskContext.remove()
      b
    }(executionContext)
  }

  def await[T](logic: => T, timeoutMillis: Long = -1, dependencies: Seq[Future[_]] = Seq.empty)(implicit context: LogContext): T = {
    val f = future(Common.timeout(timeoutMillis)(logic))
    while (!f.isCompleted) {
      Thread.sleep(0, 1)
      dependencies.filter(_.isCompleted).foreach(_.value.get.get) // throws exception in case any dependency failed
    }
    Await.result(f, Duration.Inf)
  }
}
