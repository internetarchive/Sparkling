package org.archive.webservices.sparkling.att

import java.io.InputStream

import org.apache.spark.rdd.RDD
import org.archive.webservices.sparkling.att
import org.archive.webservices.sparkling.io.{HdfsIO, IOUtil}
import org.archive.webservices.sparkling.util.{IteratorUtil, RddUtil, StringUtil}

import scala.util.Try

class AttachmentLoader[A] private (val filePath: String, val id: String, val attPath: String, parse: String => A) {
  import AttachmentLoader._

  lazy val exists: Boolean = HdfsIO.exists(attPath)

  private var attachmentIn: Option[InputStream] = None
  private lazy val lines = (if (exists) {
                              val in = HdfsIO.open(attPath)
                              attachmentIn = Some(in)
                              val lines = IOUtil.lines(in).filter(!_.trim.startsWith(CommentPrefix))
                              IteratorUtil.cleanup(lines, in.close)
                            } else Iterator.empty).buffered

  def skip(): Unit = if (lines.hasNext) lines.next()

  private val unavailable = Some(Attachment[A](null, null, None, Attachment.Status.Unavailable))
  def next(checksum: Option[String]): Option[Attachment[A]] =
    if (!exists) unavailable
    else {
      checksum match {
        case Some(c) =>
          if (lines.hasNext) {
            val line = lines.head
            if (line.trim.nonEmpty) Some {
              Try {
                val Array(lineChecksum, content) = line.split(Separator, 2)
                val parsed = Try(parse(content))
                if (parsed.isSuccess) {
                  if (lineChecksum == c) {
                    lines.next()
                    att.Attachment[A](c, content, parsed.toOption, Attachment.Status.Available)
                  } else {
                    if (advanceOnChecksumMismatch) lines.next()
                    att.Attachment[A](lineChecksum, content, parsed.toOption, Attachment.Status.ChecksumMismatch)
                  }
                } else {
                  lines.next()
                  att.Attachment[A](lineChecksum, content, None, Attachment.Status.NotParsable)
                }
              }.getOrElse {
                lines.next()
                Attachment[A](null, line, None, Attachment.Status.Malformed)
              }
            }
            else {
              lines.next()
              unavailable
            }
          } else unavailable
        case None =>
          skip()
          None
      }
    }

  def nextGet(checksum: Option[String]): Option[A] = next(checksum).flatMap(_.toOption)

  def close(): Unit = for (in <- attachmentIn) Try(in.close())
}

object AttachmentLoader {
  import org.archive.webservices.sparkling.Sparkling._

  val CommentPrefix = "#"
  val Separator = " "

  var advanceOnChecksumMismatch: Boolean = prop(false)(advanceOnChecksumMismatch, advanceOnChecksumMismatch = _)

  def attachmentPath(filePath: String, id: String, ext: String = AttachmentExt): String = { StringUtil.stripSuffix(filePath, GzipExt) + "." + id.replace("\\W+", "_") + ext + GzipExt }

  def apply(filePath: String, id: String) = new AttachmentLoader[String](filePath, id, attachmentPath(filePath, id), identity)
  def apply[A](filePath: String, id: String, parse: String => A) = new AttachmentLoader[A](filePath, id, attachmentPath(filePath, id), parse)
  def apply[A](filePath: String, id: String, attPath: String, parse: String => A) = new AttachmentLoader[A](filePath, id, attPath, parse)
  def apply[A](filePath: String, id: String, attPath: String) = new AttachmentLoader[String](filePath, id, attPath, identity)

  def load[R](records: Iterator[Option[R]], filename: String, map: Map[String, String => _], checksum: R => String = (r: R) => r.toString): Iterator[Option[(R, Map[String, Attachment[Any]])]] = {
    load(records, filename, map, checksum, attachmentPath(filename, _))
  }

  def load[R](records: Iterator[Option[R]], filename: String, map: Map[String, String => _], checksum: R => String, attPath: String => String): Iterator[Option[(R, Map[String, Attachment[Any]])]] = {
    val loaders = map.map { case (id, parse) => id -> AttachmentLoader[Any](filename, id, attPath(id), parse) }
    records.map { opt => opt.map { r => r -> loaders.map { case (id, loader) => id -> loader.next(opt.map(checksum)).get } } }
  }

  def load[R](
      path: String,
      map: Map[String, String => _],
      read: (String, InputStream) => Iterator[Option[R]],
      checksum: R => String,
      attPath: (String, String) => String
  ): RDD[Option[(R, Map[String, Attachment[Any]])]] = {
    RddUtil.loadBinary(path, decompress = false, close = false) { (filename, in) =>
      val records = read(filename, in)
      IteratorUtil.cleanup(load(records, filename, map, checksum, attPath(filename, _)), in.close)
    }
  }

  def loadTextLines(path: String, map: Map[String, String => _], ext: String = AttachmentExt): RDD[Option[(String, Map[String, Attachment[Any]])]] = {
    load[String](
      path,
      map,
      (filename: String, in: InputStream) => IOUtil.lines(in, Some(filename)).map(l => Some(l)),
      (s: String) => s.hashCode.toString,
      (filename: String, id: String) => AttachmentLoader.attachmentPath(filename, id, ext)
    )
  }
}
