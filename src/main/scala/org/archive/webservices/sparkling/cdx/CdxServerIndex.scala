package org.archive.webservices.sparkling.cdx

import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.archive.webservices.sparkling._
import org.archive.webservices.sparkling.compression.Gzip
import org.archive.webservices.sparkling.indexed.{HdfsIndexBackedMap, PrefixIndexLoader, SortedPrefixSeq}
import org.archive.webservices.sparkling.io.{HdfsBackedMap, HdfsIO, IOUtil}
import org.archive.webservices.sparkling.util._

import scala.math.pow
import scala.util.Random

object CdxServerIndex {
  import org.archive.webservices.sparkling.Sparkling._

  def loadFromFiles(
      indexDirPath: String,
      prefixes: SortedPrefixSeq,
      mergePointers: Int = 0,
      filter: String => Boolean = _ => true,
      repartition: Int = parallelism,
      sorted: Boolean = false,
      cache: Boolean = false,
      shuffle: Boolean = true,
      indexLinesPerPartition: Int = -1
  )(implicit accessContext: AccessContext = AccessContext.default): ManagedVal[RDD[CdxRecord]] = {
    PrefixIndexLoader.loadTextLinesFromIndex(
      new Path(indexDirPath, "part-*-idx").toString,
      prefixes,
      { idxLine =>
        val split = RegexUtil.split(idxLine, "\t")
        (split(1) + ".gz", split(2).toLong, split(3).toLong) // filename, offset, length
      },
      mergePointers = mergePointers,
      filter = filter,
      repartition = repartition,
      sorted = sorted,
      cache = cache,
      shuffle = shuffle,
      indexLinesPerPartition = indexLinesPerPartition
    ).map(_.flatMap(CdxRecord.fromString))
  }

  def loadFromFilesGroupedByPrefix(
      indexDirPath: String,
      prefixes: SortedPrefixSeq,
      getPrefix: (String, String) => String = (prefix, line) => StringUtil.prefixBySeparator(line, " "),
      mergePointers: Int = 0,
      filter: String => Boolean = _ => true,
      repartition: Int = parallelism,
      partitionByPrefix: Boolean = false,
      sorted: Boolean = false,
      cache: Boolean = false,
      shuffle: Boolean = true,
      indexLinesPerPartition: Int = -1
  )(implicit accessContext: AccessContext = AccessContext.default): ManagedVal[RDD[(String, Iterator[CdxRecord])]] = {
    PrefixIndexLoader.loadTextLinesGroupedByPrefixFromIndex(
      new Path(indexDirPath, "part-*-idx").toString,
      prefixes,
      { idxLine =>
        val split = RegexUtil.split(idxLine, "\t")
        (split(1) + ".gz", split(2).toLong, split(3).toLong) // filename, offset, length
      },
      getPrefix,
      mergePointers = mergePointers,
      filter = filter,
      repartition = repartition,
      partitionByPrefix = partitionByPrefix,
      sorted = sorted,
      cache = cache,
      Some(new Path(indexDirPath, "part-*.gz").toString),
      shuffle = shuffle,
      indexLinesPerPartition = indexLinesPerPartition
    ).map(_.map{ case (prefix, lines) => (prefix, lines.flatMap(CdxRecord.fromString)) })
  }

  def groupByPrefix(
      cdxPath: String,
      getPrefix: String => Option[String] = line => Some(StringUtil.prefixBySeparator(line, " ")),
      filter: (String, Iterator[String]) => Iterator[String] = (prefix, records) => records
  )(implicit accessContext: AccessContext = AccessContext.default): RDD[(String, Iterator[CdxRecord])] = { RddUtil.loadTextLinesGroupedAcrossFiles(cdxPath, getPrefix).map { case (group, records) => group -> filter(group, records).flatMap(CdxRecord.fromString) } }

  def lookupGrouped[A](indexDirPath: String, lookupSorted: RDD[A])(surt: A => String)(implicit accessContext: AccessContext = AccessContext.default): ManagedVal[RDD[(String, Iterator[A], Iterator[CdxRecord])]] = {
    val grouped = RddUtil.groupSorted(lookupSorted, surt)
    lookup(indexDirPath, grouped)(_._1).map(_.map{ case (s, (_, a), cdx) => (s, a, cdx) })
  }

  def lookup[A](indexDirPath: String, lookupSorted: RDD[A])(surt: A => String)(implicit accessContext: AccessContext = AccessContext.default): ManagedVal[RDD[(String, A, Iterator[CdxRecord])]] = {
    val blockSize = HdfsIO.fs.getDefaultBlockSize(new Path(indexDirPath))
    lazy val filesBroadcast = {
      val files = HdfsIO.files(new Path(indexDirPath, "part-*.gz").toString).map(_.split('/').last).toSeq.sorted
      sc.broadcast(files)
    }
    lazy val cdxIndexBroadcast = {
      val cdxIndex = HdfsBackedMap(new Path(indexDirPath, "part-*-idx").toString, key = StringUtil.prefixBySeparator(_, " "), cache = false, preloadLength = false)
      sc.broadcast(cdxIndex)
    }
    ManagedVal({
      val filesBc = filesBroadcast
      val cdxIndexBc = cdxIndexBroadcast
      lookupSorted.mapPartitions { records =>
        val files = filesBc.value
        val cdxIndex = cdxIndexBc.value
        val buffered = records.map(r => (surt(r), r)).buffered
        buffered.headOption.toIterator.flatMap { case (from, _) =>
          val pointers = cdxIndex.before(from).chain(_.flatMap { case (_, idxLines) =>
            idxLines.get.map(RegexUtil.split(_, "\t")).filter(_.length > 2).map { split =>
              val (idxSurt, filename, offset) = (StringUtil.prefixBySeparator(split.head, " "), split(1) + ".gz", split(2).toLong)
              (idxSurt, filename, offset)
            }
          }).chain(IteratorUtil.zipNext)

          var skipOffset: Option[Long] = None
          var groupedCdx: Option[CleanupIterator[(String, Iterator[String])]] = None
          var prevSurt: Option[String] = None
          pointers.map { case ((idxSurt, filename, offset), nextOpt) =>
            buffered.headOption.map { _ =>
              IteratorUtil.takeWhile(buffered)(_._1 < idxSurt).map { case (s, r) => (s, r, Seq.empty[CdxRecord].toIterator) } ++ {
                buffered.headOption.toIterator.flatMap { case (nextSurt, _) =>
                  if (nextOpt.isEmpty || nextSurt <= nextOpt.get._1) {
                    prevSurt = Some(nextSurt)
                    skipOffset = None
                    if (groupedCdx.isEmpty) {
                      groupedCdx = Some {
                        val fileOffsets = Iterator((filename, offset)) ++ files.dropWhile(_ <= filename).map((_, 0L))
                        CleanupIterator.flatten(fileOffsets.map { case (f, o) =>
                          val in = HdfsIO.open(new Path(indexDirPath, f).toString, offset = o)
                          CleanupIterator(IOUtil.lines(in), in.close).chain(IteratorUtil.groupSortedBy(_)(StringUtil.prefixBySeparator(_, " ")))
                        })
                      }
                    }
                    val candidates =
                      if (nextOpt.isEmpty) buffered
                      else {
                        val until = nextOpt.get._1
                        IteratorUtil.takeWhile(buffered) { case (s, _) => s <= until }
                      }
                    val cdx = groupedCdx.get
                    candidates.map { case (s, r) =>
                      IteratorUtil.dropWhile(cdx)(_._1 < s)
                      if (cdx.headOption.exists(_._1 == s)) { (s, r, IteratorUtil.takeWhile(cdx)(_._1 == s).flatMap(_._2.flatMap(CdxRecord.fromString))) }
                      else { (s, r, Seq.empty[CdxRecord].toIterator) }
                    }
                  } else if (prevSurt.contains(idxSurt)) { Iterator.empty }
                  else {
                    if (groupedCdx.isDefined) {
                      if (skipOffset.isDefined) {
                        if (offset - skipOffset.get >= blockSize) {
                          for (iter <- groupedCdx) iter.clear()
                          groupedCdx = None
                          skipOffset = None
                        }
                      } else skipOffset = Some(offset)
                    }
                    Iterator.empty
                  }
                }
              }
            }
          }.takeWhile(_.isDefined).flatMap(_.get)
        }
      }
    }, _ => {
      filesBroadcast.unpersist()
      cdxIndexBroadcast.unpersist()
    })
  }

  def primitiveHdfsBackedStrMap(indexDirPath: String)(implicit accessContext: AccessContext = AccessContext.default): HdfsIndexBackedMap = {
    new HdfsIndexBackedMap(new Path(indexDirPath, "part-*.gz").toString, StringUtil.prefixBySeparator(_, " "), { case (file, key) =>
      val indexFile = file.stripSuffix(".gz") + "-idx"
      HdfsIO.iterLines(indexFile).iter { iter =>
        val smallerKeyOffsets = iter.map { idxLine =>
          val split = RegexUtil.split(idxLine, "\t") // surt ts, filename, offset, length
          (split.head.split(' ').head, split(2).toLong)
        }.takeWhile(_._1 < key)
        IteratorUtil.last(smallerKeyOffsets).map(_._2).getOrElse(0L)
      }
    })
  }

  def hdfsBackedStrMap(indexDirPath: String)(implicit accessContext: AccessContext = AccessContext.default): HdfsBackedMap[String] = {
    new HdfsBackedMap[String](primitiveHdfsBackedStrMap(indexDirPath), identity)
  }

  def hdfsBackedMap(indexDirPath: String)(implicit accessContext: AccessContext = AccessContext.default): HdfsBackedMap[CdxRecord] = {
    new HdfsBackedMap[CdxRecord](primitiveHdfsBackedStrMap(indexDirPath), str => CdxRecord.fromString(str).get)
  }

  def loadRandomSampleStr(indexDirPath: String, num: Int, fromWhile: Option[(String, String => Boolean)] = None, filter: Option[String => Boolean] = None, spread: Int = 5)(implicit accessContext: AccessContext = AccessContext.default): ManagedVal[RDD[String]] = {
    val indexRdd = RddUtil.loadTextFiles(indexDirPath + "/part-*-idx")
    val managed = if (fromWhile.isDefined) {
      val (from, whileCond) = fromWhile.get
      lazy val filesBroadcast = {
        val filesRelevant = indexRdd.map { case (file, lines) =>
          (file, lines.iter(_.headOption).exists(_ >= from))
        }.collect.sortBy(_._1)
        val files = IteratorUtil.dropButLast(filesRelevant.toIterator.buffered)(!_._2).map(_._1).toSet
        sc.broadcast(files)
      }
      ManagedVal[RDD[String]]({
        val filesBc = filesBroadcast
        indexRdd.filter { case (file, lines) =>
          filesBc.value.contains(file) || {
            lines.clear()
            false
          }
        }.flatMap { case (_, lines) =>
          val (head, tail) = if (lines.headOption.exists(_ < from)) {
            val candidates = lines.chain(IteratorUtil.dropButLast(_)(_ < from))
            (candidates.headOption.toIterator, candidates.chain(_.drop(1)))
          } else (Iterator.empty, lines)
          head ++ tail.chain(_.takeWhile(l => whileCond(StringUtil.prefixBySeparator(l, " "))))
        }
      }, _ => filesBroadcast.unpersist())
    } else ManagedVal(indexRdd.flatMap(_._2))
    managed.map { indexLines =>
      val indexLinesCount = indexLines.count
      val indexLinesNum = ((1.0 - pow(Random.nextDouble, spread)) * num).min(indexLinesCount).max(1)
      val indexRnd = indexLinesNum / indexLinesCount
      val indexSampled = if (indexRnd < 0) indexLines else indexLines.mapPartitions { partition =>
        partition.filter(_ => Random.nextDouble < indexRnd)
      }
      val cdxPerIndexLine = num.toDouble / indexLinesNum
      indexSampled.flatMap { idxLine =>
        val split = RegexUtil.split(idxLine, "\t")
        val (filename, offset, length) = (split(1) + ".gz", split(2).toLong, split(3).toLong) // filename, offset, length
        val path = indexDirPath + "/" + filename
        val compressionFactor = HdfsIO.access(path, offset = offset, length = length, decompress = false)(Gzip.estimateCompressionFactor(_, 1.mb))
        val size = length * compressionFactor
        val in = IOUtil.supportMark(HdfsIO.open(path, offset = offset, length = length))
        val relevantCdxLines = if (fromWhile.isDefined) {
          val (from, whileCond) = fromWhile.get
          IOUtil.lines(in).dropWhile(_ < from).takeWhile(l => whileCond(StringUtil.prefixBySeparator(l, " ")))
        } else IOUtil.lines(in)
        var selected = 0L
        var processedBytes = 0L
        var processedLines = 0L
        val sample = relevantCdxLines.filter { line =>
          processedBytes += line.length
          if (filter.isEmpty || filter.exists(_(line))) {
            processedLines += 1
            val totalCdx = size / processedBytes * processedLines
            if (Random.nextDouble < cdxPerIndexLine / totalCdx) {
              selected += 1
              true
            } else false
          } else false
        }
        IteratorUtil.cleanup(sample, in.close)
      }
    }
  }

  def loadPartitions(indexDirPath: String, numPartitions: Int, fromWhile: Option[(String, String => Boolean)] = None, surtPrefix: String => String = identity)(implicit accessContext: AccessContext = AccessContext.default): ManagedVal[RDD[(String, Iterator[CdxRecord])]] = {
    loadStrPartitions(indexDirPath, numPartitions, fromWhile, surtPrefix).map(_.map{ case (p, str) => (p, str.flatMap(CdxRecord.fromString)) })
  }

  def loadStrPartitions(indexDirPath: String, numPartitions: Int, fromWhile: Option[(String, String => Boolean)] = None, surtPrefix: String => String = identity)(implicit accessContext: AccessContext = AccessContext.default): ManagedVal[RDD[(String, Iterator[String])]] = {
    var filesBc: Option[Broadcast[Seq[String]]] = None
    ManagedVal({
      val files = RddUtil.loadTextFiles(indexDirPath + "/part-*-idx").map { case (file, lines) =>
        val (beginning, relevant) = if (fromWhile.isDefined) {
          val (from, whileCond) = fromWhile.get
          (lines.headOption.map(StringUtil.prefixBySeparator(_, " ")).exists(s => s >= from),
            lines.chain(_.dropWhile(_ < from).takeWhile(l => whileCond(StringUtil.prefixBySeparator(l, " ")))))
        } else (true, lines)
        (file, relevant.iter(IteratorUtil.count), beginning)
      }.sortBy(_._1).collect
      val totalLines = files.map(_._2).sum + (if (!files.head._3) 1 else 0)
      val linesPerPartition = (totalLines.toDouble / numPartitions).ceil.toLong
      val prevFile = files.zipWithIndex.find { case ((_, lines, b), _) => lines > 0 || b }.filter { case ((_, _, beginning), i) => i > 0 && beginning }.map { case (_, i) => files(i - 1)._1 }
      val partitions = files.flatMap { case (file, lines, beginning) =>
        if (lines > 0) {
          (0 until ((lines + (if (!beginning) 1 else 0)).toDouble / linesPerPartition).ceil.toInt).map((file, _))
        } else if (prevFile.contains(file)) {
          Iterator((file, 0))
        } else Iterator.empty
      }
      filesBc = Some(sc.broadcast(IteratorUtil.distinctOrdered(partitions.map(_._1).toIterator).toSeq))
      RddUtil.parallelize(partitions).flatMap { case (file, p) =>
        val files = filesBc.get.value
        val beginning = files.headOption.contains(file) && p == 0
        val filesIter = files.toIterator.dropWhile(_ < file).buffered
        val f = filesIter.next
        val (offset, startPrefix, endPrefix) = HdfsIO.access(f) { in =>
          val lines = if (fromWhile.isDefined) {
            val (from, _) = fromWhile.get
            val relevant = IteratorUtil.dropButLast(IOUtil.lines(in).buffered)(StringUtil.prefixBySeparator(_, " ") < from)
            IteratorUtil.drop(relevant, p * linesPerPartition).buffered
          } else IteratorUtil.drop(IOUtil.lines(in), p * linesPerPartition).buffered
          val split = lines.head.split('\t')
          val startPrefix = surtPrefix(split.head.split(' ').head)
          val offset = split(2).toLong
          val endPrefix = IteratorUtil.drop(lines, linesPerPartition).buffered.headOption.orElse {
            filesIter.headOption.flatMap(HdfsIO.lines(_, n = 1).headOption)
          }.map(StringUtil.prefixBySeparator(_, " ")).map(surtPrefix)
          (offset, startPrefix, endPrefix)
        }
        if (!beginning && endPrefix.contains(startPrefix)) Iterator.empty
        else {
          val lines = CleanupIterator.flatten(files.toIterator.dropWhile(_ < file).zipWithIndex.map { case (f, i) =>
            val in = HdfsIO.open(f.stripSuffix("-idx") + ".gz", offset = if (i == 0) offset else 0)
            IteratorUtil.cleanup(IOUtil.lines(in), in.close)
          })
          val groups = lines.chain { l =>
            val grouped = IteratorUtil.groupSortedBy(l)(s => surtPrefix(StringUtil.prefixBySeparator(s, " ")))
            if (endPrefix.isDefined) {
              val s = endPrefix.get
              grouped.takeWhile(_._1 <= s)
            } else grouped
          }
          val scoped = if (fromWhile.isDefined) {
            val (from, whileCond) = fromWhile.get
            groups.dropWhile(_._1 < from).takeWhile { case (surt, _) => whileCond(surt) }
          } else groups
          if (beginning) scoped else scoped.drop(1)
        }
      }
    }, _ => {
      for (b <- filesBc) b.unpersist()
    })
  }

  def urlToPrefix(url: String, subdomains: Boolean = true, subpaths: Boolean = true, urlInSurtFormat: Boolean = false): Set[String] = {
    SurtUtil.urlToSurtPrefixes(url, subdomains, subpaths, urlInSurtFormat)
  }
}
