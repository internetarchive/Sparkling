package org.archive.webservices.sparkling.util

import java.io.PrintWriter

import org.apache.spark.rdd.RDD
import org.archive.webservices.sparkling.io.IOUtil
import org.archive.webservices.sparkling.logging.{Log, LogContext}

import scala.reflect.ClassTag

object CmdUtil {
  implicit val logContext: LogContext = LogContext(this)

  import org.archive.webservices.sparkling.Sparkling._

  var perRecordTimeoutMillis: Int = prop(1000 * 60 * 60)(perRecordTimeoutMillis, perRecordTimeoutMillis = _) // 1 hour

  def pipe[A: ClassTag](rdd: RDD[A], setup: Seq[String], commands: Seq[String] = Seq.empty)(map: A => Option[String]): RDD[(A, Option[String])] = {
    if (commands.isEmpty) pipe(rdd, map, setup) else pipe(rdd, map, setup, commands)
  }

  def pipe[A: ClassTag](rdd: RDD[A], map: A => Option[String], commands: Seq[String]): RDD[(A, Option[String])] = {
    val filtered = commands.map(_.trim).filter(_.nonEmpty)
    pipe(rdd, map, filtered.dropRight(1), filtered.takeRight(1))
  }

  def pipe[A: ClassTag](rdd: RDD[A], map: A => Option[String], setup: Seq[String], commands: Seq[String]): RDD[(A, Option[String])] = {
    val setupClean = setup.map(_.trim).filter(_.nonEmpty)
    val commandsClean = commands.map(_.trim).filter(_.nonEmpty)
    initPartitions(rdd).mapPartitions { records =>
      val script = IOUtil.tmpFile(".sh")
      val scriptOut = new PrintWriter(script)
      if (setupClean.nonEmpty) {
        scriptOut.println("{")
        setupClean.foreach(scriptOut.println)
        scriptOut.println("} < /dev/null 1>&2") // redirect stdout to stderr and prevent stdin to be redirected to setup lines
      }

      commandsClean.foreach(scriptOut.println)
      scriptOut.close()

      val recordMappings = records.map(r => (r, map(r)))
      val (recordMap, recordMapDup) = recordMappings.duplicate

      val proc = PipedProcess(
        "/bin/bash " + script.getCanonicalPath,
        { in =>
          recordMapDup.foreach { case (r, mapOpt) =>
            if (mapOpt.isDefined) {
              Log.debug(mapOpt.get)
              in.println(mapOpt.get)
            }
          }
        },
        { err => err.foreach(Log.error(_)) },
        timeoutMillis = perRecordTimeoutMillis
      )

      val output = proc.out
      IteratorUtil.cleanup(
        recordMap.map { case (r, mapOpt) =>
          (
            r,
            mapOpt.flatMap { _ =>
              if (output.hasNext) {
                val result = output.next()
                Log.debug(result)
                Some(result)
              } else None
            }
          )
        },
        () => {
          val exitCode = proc.get.waitFor()
          if (exitCode != 0) throw new RuntimeException("pipe failed: exit code " + exitCode)

          script.delete()
        }
      )
    }
  }
}
