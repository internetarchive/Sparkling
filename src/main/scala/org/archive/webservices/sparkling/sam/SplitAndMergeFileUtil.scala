package org.archive.webservices.sparkling.sam

import org.apache.hadoop.fs.Path
import org.archive.webservices.sparkling.Sparkling
import org.archive.webservices.sparkling.io.HdfsIO
import org.archive.webservices.sparkling.util.{RddUtil, StringUtil}

object SplitAndMergeFileUtil {
  val SplitExtPrefix = ".split-"

  def splitPath(path: String, idx: Int, outDir: String): String = {
    val file = new Path(path).getName
    val parts = file.split("\\.", 2)
    val (filePrefix, fileExt) = (parts.head, parts.drop(1).headOption)
    new Path(outDir, filePrefix + SplitExtPrefix + StringUtil.padNum(idx, 5) + fileExt.map("." + _).getOrElse("")).toString
  }

  def mergePath(path: String, outDir: String): String = {
    val file = new Path(path).getName
    val parts = file.split(SplitExtPrefix, 2)
    val (filePrefix, fileExt) = (parts.head, parts.drop(1).headOption.flatMap(_.split("\\.", 2).drop(1).headOption))
    new Path(outDir, filePrefix + fileExt.map("." + _).getOrElse("")).toString
  }

  def split(path: String, outDir: String, createSplits: String => Iterator[(Long, Long)], write: (String, Long, Long, String) => Unit, outPath: (String, Int, String) => String = splitPath): Long = {
    val splits = RddUtil.loadFilesLocality(path).flatMap(file => createSplits(file).zipWithIndex.map { case ((offset, length), idx) => (file, offset, length, idx) })
    RddUtil.shuffle(splits, Sparkling.parallelism).map { case (file, offset, length, idx) =>
      write(file, offset, length, outPath(file, idx, outDir))
      1L
    }.reduce(_ + _)
  }

  def merge(path: String, outDir: String, outPath: (String, String) => String = mergePath, mergeFiles: (Seq[String], String) => Unit = HdfsIO.concat(_, _)): Long = {
    RddUtil.loadFilesLocality(path).map(file => (outPath(file, outDir), file)).groupByKey(Sparkling.parallelism).map { case (outFile, splits) =>
      val parts = splits.toList.sorted
      mergeFiles(parts, outFile)
      parts.size
    }.reduce(_ + _)
  }
}
