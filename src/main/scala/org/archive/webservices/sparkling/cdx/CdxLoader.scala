package org.archive.webservices.sparkling.cdx

import java.io.InputStream

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.archive.webservices.sparkling.{Sparkling, _}
import org.archive.webservices.sparkling.att.AttachmentLoader
import org.archive.webservices.sparkling.io.{HdfsIO, IOUtil}
import org.archive.webservices.sparkling.util.{Common, IteratorUtil, RddUtil, StringUtil}
import org.archive.webservices.sparkling.warc.WarcRecord

import scala.reflect.ClassTag

object CdxLoader {
  import org.archive.webservices.sparkling.Sparkling._

  def load(path: String, sorted: Boolean = false): RDD[CdxRecord] = loadFiles(path, sorted).flatMap { case (_, records) => records.flatten }

  def loadFiles(path: String, sorted: Boolean = false): RDD[(String, Iterator[Option[CdxRecord]])] = RddUtil.loadTextFiles(path, sorted = sorted).map { case (file, lines) =>
    (file, lines.map(CdxRecord.fromString))
  }

  def save(rdd: RDD[CdxRecord], path: String): Long = {
    HdfsIO.ensureOutDir(path)
    val filePrefix = StringUtil.stripSuffixes(new Path(path).getName, GzipExt, CdxExt)
    rdd.mapPartitionsWithIndex { case (idx, records) =>
      val filename = filePrefix + "-" + StringUtil.padNum(idx, 5) + CdxExt + GzipExt
      val cdxPath = new Path(path, filename).toString
      val cdxOut = Common.lazyValWithCleanup(IOUtil.print(HdfsIO.out(cdxPath)))(_.close)
      val processed = records.map { record =>
        cdxOut.get.println(record.toCdxString)
        1L
      }.sum
      cdxOut.clear(true)
      Iterator(processed)
    }.reduce(_ + _)
  }

  def saveWithAttachments[A: ClassTag](
      rdd: RDD[(CdxRecord, A)],
      path: String,
      attachments: Map[String, A => Option[String]],
      comments: Map[String, Seq[String]] = Map.empty,
      maxBatchSize: Long = 1.gb
  ): Long = {
    HdfsIO.ensureOutDir(path)
    val filePrefix = StringUtil.stripSuffixes(new Path(path).getName, GzipExt, CdxExt)
    rdd.mapPartitions { records =>
      val batches = if (maxBatchSize < 0) Iterator(records) else IteratorUtil.grouped(records, maxBatchSize)(_._1.toCdxString.length)
      batches.zipWithIndex.map { case (batch, batchIdx) =>
        val batchSuffix = if (maxBatchSize < 0) "" else "-" + StringUtil.padNum(batchIdx, 5)
        val cdxPath = new Path(path, Sparkling.getTaskOutFile(idx => filePrefix + "-" + StringUtil.padNum(idx, 5) + batchSuffix + CdxExt + GzipExt)).toString
        val cdxOut = Common.lazyValWithCleanup(IOUtil.print(HdfsIO.out(cdxPath)))(_.close)
        val attachmentsOut = attachments.map { case (id, _) =>
          val attachmentPath = CdxAttachmentLoader.attachmentPath(cdxPath, id)
          id -> Common.lazyValWithCleanup({
            val out = IOUtil.print(HdfsIO.out(attachmentPath))
            comments.getOrElse(id, Set.empty).flatMap(_.split("\\n")).map(AttachmentLoader.CommentPrefix + " " + _).foreach(out.println)
            out
          })(_.close)
        }
        val processed = batch.map { case (record, attachmentData) =>
          cdxOut.get.println(record.toCdxString)
          for ((id, map) <- attachments) {
            map(attachmentData) match {
              case Some(a) => attachmentsOut(id).get.println(record.digest + " " + a)
              case None    => attachmentsOut(id).get.println("")
            }
          }
          1L
        }.sum
        attachmentsOut.foreach { case (id, out) => out.clear(true) }
        cdxOut.clear(true)
        processed
      }
    }.reduce(_ + _)
  }

  def loadFromWarcGz(path: String, filter: WarcRecord => Boolean = _ => true, digest: (WarcRecord, InputStream) => String = (r, s) => r.payloadDigest.getOrElse(WarcRecord.defaultDigestHash(s))): RDD[CdxRecord] = {
    RddUtil.loadBinary(path, decompress = false, close = false) { (file, in) => IteratorUtil.cleanup(CdxUtil.fromWarcGzStream(file, in, filter, digest), in.close) }
  }

  def saveFromWarcGz(
      path: String,
      outPath: Option[String],
      filterWarc: WarcRecord => Boolean = _ => true,
      filterCdx: CdxRecord => Boolean = _ => true,
      digest: (WarcRecord, InputStream) => String = (r, s) => r.payloadDigest.getOrElse(WarcRecord.defaultDigestHash(s))
  ): Long = {
    if (outPath.isDefined) HdfsIO.ensureOutDir(outPath.get)
    RddUtil.loadBinary(path, decompress = false, close = false) { (file, in) =>
      val filePrefix = StringUtil.stripSuffixes(new Path(file).getName, GzipExt)
      val cdxPath = new Path(outPath.getOrElse(new Path(file).getParent.toString), filePrefix + CdxExt + GzipExt).toString
      val cdxOut = Common.lazyValWithCleanup(IOUtil.print(HdfsIO.out(cdxPath)))(_.close)
      val processed = IteratorUtil.cleanup(CdxUtil.fromWarcGzStream(file, in, filterWarc, digest).filter(filterCdx), in.close).map { record =>
        cdxOut.get.println(record.toCdxString)
        1L
      }.sum
      cdxOut.clear(true)
      Iterator(processed)
    }.reduce(_ + _)
  }
}
