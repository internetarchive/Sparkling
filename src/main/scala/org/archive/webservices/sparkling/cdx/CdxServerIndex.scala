package org.archive.webservices.sparkling.cdx

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.archive.webservices.sparkling.indexed.{PrefixIndexLoader, SortedPrefixSeq}
import org.archive.webservices.sparkling.io.{HdfsBackedMap, HdfsIO, IOUtil}
import org.archive.webservices.sparkling.util._

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
  ): RDD[CdxRecord] = {
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
    ).flatMap(CdxRecord.fromString)
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
  ): RDD[(String, Iterator[CdxRecord])] = {
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
    ).map { case (prefix, lines) => (prefix, lines.flatMap(CdxRecord.fromString)) }
  }

  def groupByPrefix(
      cdxPath: String,
      getPrefix: String => Option[String] = line => Some(StringUtil.prefixBySeparator(line, " ")),
      filter: (String, Iterator[String]) => Iterator[String] = (prefix, records) => records
  ): RDD[(String, Iterator[CdxRecord])] = { RddUtil.loadTextLinesGroupedAcrossFiles(cdxPath, getPrefix).map { case (group, records) => group -> filter(group, records).flatMap(CdxRecord.fromString) } }

  def lookupGrouped[A](indexDirPath: String, lookupSorted: RDD[A])(surt: A => String): RDD[(String, Iterator[A], Iterator[CdxRecord])] = {
    val grouped = RddUtil.groupSorted(lookupSorted, surt)
    lookup(indexDirPath, grouped)(_._1).map { case (s, (_, a), cdx) => (s, a, cdx) }
  }

  def lookup[A](indexDirPath: String, lookupSorted: RDD[A])(surt: A => String): RDD[(String, A, Iterator[CdxRecord])] = {
    val blockSize = HdfsIO.fs.getDefaultBlockSize(new Path(indexDirPath))
    val files = HdfsIO.files(new Path(indexDirPath, "part-*.gz").toString).map(_.split('/').last).toSeq.sorted
    val filesBc = sc.broadcast(files)
    val cdxIndex = HdfsBackedMap(new Path(indexDirPath, "part-*-idx").toString, key = StringUtil.prefixBySeparator(_, " "), cache = false, preloadLength = false)
    val cdxIndexBc = sc.broadcast(cdxIndex)
    lookupSorted.mapPartitions { records =>
      val files = filesBc.value
      val cdxIndex = cdxIndexBc.value
      val buffered = records.map(r => (surt(r), r)).buffered
      buffered.headOption.toIterator.flatMap { case (from, _) =>
        val pointers = cdxIndex.before(from).chain(_.flatMap { case (_, idxLines) =>
          idxLines.get.map { idxLine =>
            val split = RegexUtil.split(idxLine, "\t")
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
  }

  def urlToPrefix(url: String, subdomains: Boolean = true, subpaths: Boolean = true, urlInSurtFormat: Boolean = false): Set[String] = SurtUtil
    .urlToSurtPrefixes(url, subdomains, subpaths, urlInSurtFormat)
}
