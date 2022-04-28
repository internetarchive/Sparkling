package org.archive.webservices.sparkling.sam

import org.archive.webservices.sparkling.Sparkling
import org.archive.webservices.sparkling.io.HdfsIO
import org.archive.webservices.sparkling.util.RddUtil

import scala.reflect.ClassTag

object SplitAndMergeJob {
  def checkpointId(outDir: String): String = "sam-" + outDir.split("/").last

  def run[A: ClassTag](
      path: String,
      outDir: String,
      partitions: Int = 0,
      splitPath: (String, Int, String) => String = SplitAndMergeFileUtil.splitPath,
      outPath: (String, String) => String = SplitAndMergeFileUtil.mergePath,
      merge: (Seq[String], String) => Unit = HdfsIO.concat(_, _)
  )(partition: String => Iterator[(String, Int)])(run: (String, Int, String) => Long): Long = {
    HdfsIO.ensureOutDir(outDir, ensureNew = false)
    Sparkling.checkpointOrTmpDir(checkpointId(outDir)) { tmpOutDir =>
      val processed = RddUtil.loadPartitions(path: String)(partition) { case (filename, (outFile, p)) =>
        val splitFile = splitPath(outFile, p, tmpOutDir)
        Iterator(run(filename, p, splitFile))
      }.coalesce(partitions).reduce(_ + _)
      SplitAndMergeFileUtil.merge(tmpOutDir, outDir, outPath, merge)
      processed
    }
  }

  def runSplits[A](
      path: String,
      outDir: String,
      createSplits: String => Iterator[(Long, Long)],
      readSplit: (String, Long, Long) => Iterator[A],
      splitPath: (String, Int, String) => String = SplitAndMergeFileUtil.splitPath,
      outPath: (String, String) => String = SplitAndMergeFileUtil.mergePath,
      merge: (Seq[String], String) => Unit = HdfsIO.concat(_, _)
  )(run: (Iterator[A], String) => String): Long = {
    HdfsIO.ensureOutDir(outDir, ensureNew = false)
    Sparkling.checkpointOrTmpDir(checkpointId(outDir)) { tmpOutDir =>
      val splits = RddUtil.loadFilesLocality(path).flatMap(file => createSplits(file).zipWithIndex.map { case ((offset, length), idx) => (file, offset, length, idx) })
      RddUtil.shuffle(splits, Sparkling.parallelism).map { case (file, offset, length, idx) =>
        val split = readSplit(file, offset, length)
        val splitOutFile = run(split, splitPath(file, idx, tmpOutDir))
        (outPath(splitOutFile, outDir), splitOutFile)
      }.groupByKey(Sparkling.parallelism).map { case (outFile, files) =>
        val parts = files.toList.sorted
        merge(parts, outFile)
        parts.size
      }.reduce(_ + _)
    }
  }

  def runWithSplitFiles(
      path: String,
      outDir: String,
      createSplits: String => Iterator[(Long, Long)],
      writeSplit: (String, Long, Long, String) => Unit,
      splitPath: (String, Int, String) => String = SplitAndMergeFileUtil.splitPath,
      outPath: (String, String) => String = SplitAndMergeFileUtil.mergePath,
      merge: (Seq[String], String) => Unit = HdfsIO.concat(_, _)
  )(run: (String, String) => String): Long = {
    HdfsIO.ensureOutDir(outDir, ensureNew = false)
    Sparkling.checkpointOrTmpDir(checkpointId(outDir)) { tmpSplitDir =>
      SplitAndMergeFileUtil.split(path, tmpSplitDir, createSplits, writeSplit, splitPath)
      HdfsIO.tmpPath { tmpOutDir =>
        val tmpOutPath = run(tmpSplitDir, tmpOutDir)
        SplitAndMergeFileUtil.merge(tmpOutPath, outDir, outPath, merge)
      }
    }
  }
}
