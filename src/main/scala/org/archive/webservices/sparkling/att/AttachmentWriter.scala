package org.archive.webservices.sparkling.att

import java.io.{InputStream, PrintWriter}

import org.archive.webservices.sparkling.io.{HdfsIO, IOUtil}
import org.archive.webservices.sparkling.logging.{Log, LogContext}
import org.archive.webservices.sparkling.sam.SplitAndMergeJob
import org.archive.webservices.sparkling.util.{Common, IteratorUtil, PipedProcess, RddUtil}

class AttachmentWriter[R](read: (String, InputStream) => TraversableOnce[Option[R]], checksum: R => String, attPath: (String, String) => String) {
  import AttachmentWriter._

  def attachFiles[A](path: String, id: String, comments: Seq[String], map: (String, Iterator[Option[R]]) => Iterator[Option[A]], repartition: Int = 0, batchLines: Int = DefaultBatchLines)(
      attach: Iterator[A] => Iterator[String]
  ): Long = {
    val (read, checksum, attPath) = (this.read, this.checksum, this.attPath)

    if (repartition > 0) {
      SplitAndMergeJob.run(path, HdfsIO.dir(path), repartition) { filename =>
        val attachmentFile = attPath(filename, id)
        if (HdfsIO.exists(attachmentFile)) Iterator.empty
        else HdfsIO.access(filename, decompress = false, length = -1) { in =>
          val numPartitions = (IteratorUtil.count(read(filename, in)).toDouble / batchLines).ceil.toInt
          (0 until numPartitions).toIterator.map((attachmentFile, _))
        }
      } { (filename, partition, partitionFile) =>
        if (HdfsIO.exists(partitionFile)) 0L
        else HdfsIO.access(filename, decompress = false, length = -1) { in =>
          Log.info("Processing " + filename + " (split " + partition + ")... (" + partitionFile + ")")
          val processed = attachFile(read, checksum, attPath, comments, map, filename, in, partitionFile, partition, batchLines)(attach)
          Log.info("Done.")
          processed
        }
      }
    } else {
      RddUtil.loadBinaryLazy(path, decompress = false, readFully = true) { (filename, in) =>
        val attachmentFile = attPath(filename, id)
        if (HdfsIO.exists(attachmentFile)) None
        else {
          Log.info("Processing " + filename + "...")
          val processed = attachFile(read, checksum, attPath, comments, map, filename, in.get, attachmentFile)(attach)
          Log.info("Done.")
          Some(processed)
        }
      }.reduce(_ + _)
    }
  }

  def attach(path: String, id: String, comments: Seq[String])(map: R => Option[String], repartition: Int = 0, batchLines: Int = DefaultBatchLines): Long =
    attachFiles(path, id, comments, (file, records) => records.map(_.flatMap(map)), repartition, batchLines)(identity)

  def pipeAttachFiles(
      path: String,
      id: String,
      comments: Seq[String],
      map: (String, Iterator[Option[R]]) => Iterator[Option[String]],
      setup: Seq[String],
      commands: Seq[String],
      repartition: Int,
      batchLines: Int
  ): Long = {
    val setupClean = setup.map(_.trim).filter(_.nonEmpty)
    val commandsClean = commands.map(_.trim).filter(_.nonEmpty)
    attachFiles[String](path, id, comments, map, repartition, batchLines) { mapped =>
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
          mapped.foreach { str =>
            Log.debug(str)
            in.println(str)
            in.flush()
          }
        },
        _.foreach(Log.errln(_)),
        timeoutMillis = perRecordTimeoutMillis
      )

      IteratorUtil.cleanup(
        proc.out,
        () => {
          val exitCode = proc.get.waitFor()
          script.delete()
          if (exitCode != 0) throw new RuntimeException("pipeAttach failed: exit code " + exitCode)
        }
      )
    }
  }

  def pipeAttachFiles(
      path: String,
      id: String,
      comments: Seq[String],
      map: (String, Iterator[Option[R]]) => Iterator[Option[String]],
      setup: Seq[String],
      commands: Seq[String],
      repartition: Int
  ): Long = { pipeAttachFiles(path, id, comments, map, setup, commands, repartition, DefaultBatchLines) }

  def pipeAttachFiles(path: String, id: String, comments: Seq[String], map: (String, Iterator[Option[R]]) => Iterator[Option[String]], setup: Seq[String], commands: Seq[String]): Long = {
    pipeAttachFiles(path, id, comments, map, setup, commands, 0)
  }

  def pipeAttachFiles(
      path: String,
      id: String,
      comments: Seq[String],
      attach: (String, Iterator[Option[R]]) => Iterator[Option[String]],
      commands: Seq[String],
      repartition: Int = 0,
      batchLines: Int = DefaultBatchLines
  ): Long = {
    val filtered = commands.map(_.trim).filter(_.nonEmpty)
    pipeAttachFiles(path, id, comments, attach, filtered.dropRight(1), filtered.takeRight(1), repartition, batchLines)
  }

  def pipeAttach(path: String, id: String, comments: Seq[String], attach: R => Option[String], commands: Seq[String], repartition: Int = 0, batchLines: Int = DefaultBatchLines): Long = {
    val filtered = commands.map(_.trim).filter(_.nonEmpty)
    pipeAttach(path, id, comments, attach, filtered.dropRight(1), filtered.takeRight(1), repartition, batchLines)
  }

  def pipeAttach(path: String, id: String, comments: Seq[String], attach: R => Option[String], setup: Seq[String], commands: Seq[String], repartition: Int): Long = {
    pipeAttach(path, id, comments, attach, setup, commands, repartition, DefaultBatchLines)
  }

  def pipeAttach(path: String, id: String, comments: Seq[String], attach: R => Option[String], setup: Seq[String], commands: Seq[String]): Long = {
    pipeAttach(path, id, comments, attach, setup, commands, 0)
  }

  def pipeAttach(path: String, id: String, comments: Seq[String], attach: R => Option[String], setup: Seq[String], commands: Seq[String], repartition: Int, batchLines: Int): Long = {
    pipeAttachFiles(path, id, comments, (file, records) => records.map(_.flatMap(attach)), setup, commands, repartition, batchLines)
  }
}

object AttachmentWriter {
  implicit val logContext: LogContext = LogContext(this)

  import org.archive.webservices.sparkling.Sparkling._

  val DefaultBatchLines = 100000

  var perRecordTimeoutMillis: Int = prop(1000 * 60 * 60)(perRecordTimeoutMillis, perRecordTimeoutMillis = _) // 1 hour

  def apply[R](read: (String, InputStream) => TraversableOnce[Option[R]], checksum: R => String, attPath: (String, String) => String): AttachmentWriter[R] =
    new AttachmentWriter[R](read, checksum, attPath)

  def forTextLines(ext: String = AttachmentExt): AttachmentWriter[String] =
    new AttachmentWriter[String]((filename, in) => IOUtil.lines(in, Some(filename)).map(l => Some(l)), _.hashCode.toString, AttachmentLoader.attachmentPath(_, _, ext))

  private def attachFile[R, A](
      read: (String, InputStream) => TraversableOnce[Option[R]],
      checksum: R => String,
      attPath: (String, String) => String,
      comments: Seq[String],
      map: (String, Iterator[Option[R]]) => Iterator[Option[A]],
      filename: String,
      in: InputStream,
      outFile: String,
      partition: Long = -1,
      batchLines: Int = 0
  )(attach: Iterator[A] => Iterator[String]): Long = {
    val (records, recordsDup) = read(filename, in).toIterator.duplicate
    val mappedIter = records.zip(map(filename, recordsDup)).map { case (r, m) => r.flatMap { record => m.map((checksum(record), _)) } }
    val (mapped, mappedDup) = (if (partition >= 0 && batchLines > 0) IteratorUtil.drop(mappedIter, partition * batchLines).take(batchLines) else mappedIter).duplicate
    val attached = attach(mapped.flatten.map(_._2))

    val out = IOUtil.print(HdfsIO.out(outFile))

    if (partition <= 0) for (c <- comments.flatMap(_.split("\\n"))) out.println(AttachmentLoader.CommentPrefix + " " + c)

    val processed = mappedDup.map {
      case Some((digest, record)) => Common.timeoutOpt(perRecordTimeoutMillis, Some(digest)) { if (attached.hasNext) Some(attached.next) else None }.flatten match {
          case Some(att) =>
            out.println(digest + AttachmentLoader.Separator + att)
            1L
          case None =>
            out.println("")
            0L
        }
      case None =>
        out.println("")
        0L
    }.sum

    out.close()

    processed
  }
}
