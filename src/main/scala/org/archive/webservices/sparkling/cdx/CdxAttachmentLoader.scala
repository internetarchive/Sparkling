package org.archive.webservices.sparkling.cdx

import java.io.InputStream

import org.apache.spark.rdd.RDD
import org.archive.webservices.sparkling.att
import org.archive.webservices.sparkling.att.{Attachment, AttachmentLoader, AttachmentWriter}
import org.archive.webservices.sparkling.io.IOUtil
import org.archive.webservices.sparkling.logging.LogContext

import scala.reflect.ClassTag

object CdxAttachmentLoader {
  implicit val logContext: LogContext = LogContext(this)

  import org.archive.webservices.sparkling.Sparkling._

  def attachmentPath(cdxPath: String, id: String): String = AttachmentLoader.attachmentPath(cdxPath, id, CdxAttachmentExt)

  lazy val writer: AttachmentWriter[CdxRecord] = att.AttachmentWriter[CdxRecord]((filename, in) => IOUtil.lines(in, Some(filename)).map(CdxRecord.fromString), _.digest, attachmentPath)
  def loader[A](filePath: String, id: String, parse: String => A): AttachmentLoader[A] = AttachmentLoader(filePath, id, attachmentPath(filePath, id), parse)
  def loader(filePath: String, id: String): AttachmentLoader[String] = loader(filePath, id, identity)

  def load(path: String, id1: String, id2: String, ids: String*): RDD[Option[(CdxRecord, Map[String, Attachment[String]])]] = load(path, Set(id1, id2) ++ ids)

  def load(path: String, id: String): RDD[Option[(CdxRecord, Attachment[String])]] = { load(path, Set(id)).map(_.map { case (record, attachments) => (record, attachments(id)) }) }

  def load(path: String, ids: Set[String]): RDD[Option[(CdxRecord, Map[String, Attachment[String]])]] = {
    load(path, ids.toSeq.map(id => (id, (s: String) => s)).toMap).map(_.map { case (record, attachments) => (record, attachments.mapValues(_.asInstanceOf[Attachment[String]])) })
  }

  def load[A: ClassTag](path: String, id: String, map: String => A): RDD[Option[(CdxRecord, Attachment[A])]] = {
    load(path, Map(id -> map)).map(_.map { case (record, attachments) => (record, attachments(id).asInstanceOf[Attachment[A]]) })
  }

  def load(path: String, map: Map[String, String => _]): RDD[Option[(CdxRecord, Map[String, Attachment[Any]])]] = {
    AttachmentLoader.load(path, map, (file, in) => IOUtil.lines(in, Some(file)).map(CdxRecord.fromString), _.digest, attachmentPath)
  }

  def load(filename: String, in: InputStream, map: Map[String, String => _]): Iterator[Option[(CdxRecord, Map[String, Attachment[Any]])]] = {
    val cdx = IOUtil.lines(in, Some(filename)).map(CdxRecord.fromString)
    AttachmentLoader.load(cdx, filename, map, (cdx: CdxRecord) => cdx.digest, attachmentPath(filename, _))
  }

  def loadGet(path: String, id1: String, id2: String, ids: String*): RDD[(CdxRecord, Map[String, String])] = loadGet(path, Set(id1, id2) ++ ids)

  def loadGet(path: String, id: String): RDD[(CdxRecord, Option[String])] = { loadGet(path, Set(id)).map { case (record, attachments) => (record, attachments.get(id)) } }

  def loadGet(path: String, ids: Set[String]): RDD[(CdxRecord, Map[String, String])] = {
    load(path, ids).flatMap(_.map { case (record, attachments) => (record, attachments.flatMap { case (id, att) => att.map(v => (id, v)) }) })
  }

  def loadGet[A: ClassTag](path: String, id: String, map: String => A): RDD[(CdxRecord, Option[A])] = {
    load(path, id, map).flatMap(_.map { case (record, attachment) => (record, attachment.toOption) })
  }

  def loadGet(filename: String, in: InputStream, id: String): Iterator[(CdxRecord, Option[String])] = {
    loadGet(filename, in, Set(id)).map { case (record, attachments) => (record, attachments.get(id)) }
  }

  def loadGet(filename: String, in: InputStream, id1: String, id2: String, ids: String*): Iterator[(CdxRecord, Map[String, String])] = { loadGet(filename, in, Set(id1, id2) ++ ids) }

  def loadGet(filename: String, in: InputStream, ids: Set[String]): Iterator[(CdxRecord, Map[String, String])] = {
    load(filename, in, ids.toSeq.map(id => id -> ((str: String) => str)).toMap).flatMap { opt =>
      opt.map { case (record, attachments) => (record, attachments.flatMap { case (id, att) => att.asInstanceOf[Attachment[String]].map(v => (id, v)) }) }
    }
  }
}
