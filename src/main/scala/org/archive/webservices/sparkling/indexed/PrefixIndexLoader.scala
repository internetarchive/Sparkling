package org.archive.webservices.sparkling.indexed

import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.archive.webservices.sparkling.AccessContext
import org.archive.webservices.sparkling.io.{HdfsIO, IOUtil}
import org.archive.webservices.sparkling.util.{CleanupIterator, IteratorUtil, ManagedVal, MultiBufferedIterator, RddUtil}

import java.io.InputStream
import scala.reflect.ClassTag
import scala.util.Try

object PrefixIndexLoader {
  import org.archive.webservices.sparkling.Sparkling._

  val DefaultMergePointers = 10

//  sc.getConf.registerKryoClasses(Array(
//    classOf[PositionPointer],
//    classOf[RecordPointer],
//    classOf[SortedPrefixSeq],
//    classOf[BasicSortedPrefixSeq],
//    classOf[HdfsSortedPrefixSeq]
//  ))

  def loadIndexWithPrefixesBroadcast(
      path: String,
      prefixBroadcast: Broadcast[SortedPrefixSeq],
      getFileOffsetLength: String => (String, Long, Long),
      mergePointers: Int,
      mergeTolerance: Long,
      groupByPrefix: Boolean = false,
      sorted: Boolean = false,
      cache: Boolean = true,
      linesPerPartition: Int = -1
  )(implicit accessContext: AccessContext = AccessContext.default): ManagedVal[RDD[RecordPointer]] = {
    var firstLinesBroadcast: Option[Broadcast[Array[(String, String)]]] = None
    var numFiles = 1
    ManagedVal({
      val rdd = (if (linesPerPartition > 0) {
        RddUtil.loadTextPartitionsByLinesWithFilenames(path, linesPerPartition = linesPerPartition)
      } else {
        val firstLines = RddUtil.loadBinary(path) { (indexFile, in) =>
          val lines = IOUtil.lines(in)
          if (lines.hasNext) Some((indexFile, lines.next)) else None
        }.coalesce(parallelism).collect.sortBy(_._1)

        val firstLinesBuffered = IteratorUtil.zipNext(firstLines.toIterator).buffered
        val prefixes = prefixBroadcast.value.iter
        val relevantFiles = if (firstLinesBuffered.hasNext) {
          IteratorUtil.whileDefined(firstLinesBuffered.map { case ((file, line), next) =>
            IteratorUtil.dropWhile(prefixes) { prefix => prefix < line && !line.startsWith(prefix) }
            if (prefixes.hasNext) {
              Some(if (next.isEmpty || next.get._2 >= prefixes.head) Some(file) else None)
            } else None
          }).flatten.toSet
        } else Set.empty[String]

        numFiles = relevantFiles.size

        firstLinesBroadcast = Some(sc.broadcast(firstLines))

        RddUtil.loadFilesLocality(path).filter(f => relevantFiles.contains(f)).map(f => (f, HdfsIO.iterLines(f)))
      }).coalesce(parallelism).flatMap { case (indexFile, lines) =>
          val nextFileFirstLine = firstLinesBroadcast.flatMap(_.value.dropWhile(_._1 <= indexFile).headOption.map(_._2))
          lines.chain { l =>
            val pointers = IndexUtil.lineCandidates(l, prefixBroadcast.value, nextFileFirstLine).flatMap { case (line, prefixStart) =>
              Try {
                val (file, offset, length) = getFileOffsetLength(line)
                val filePath = new Path(new Path(indexFile, ".."), file).toString
                RecordPointer(filePath, PositionPointer(offset, length), prefixStart)
              }.toOption
            }
            IndexUtil.mergeRecordPointers(pointers, mergePointers * numFiles, mergeTolerance, groupByPrefix)
          }
        }
      if (cache) rdd.persist(StorageLevel.MEMORY_AND_DISK) else rdd
    }, _ => {
      for (b <- firstLinesBroadcast) b.unpersist()
    })
  }

  def loadIndex(path: String, prefixes: SortedPrefixSeq, getFileOffsetLength: String => (String, Long, Long), mergePointers: Int, sorted: Boolean = false)
               (implicit accessContext: AccessContext = AccessContext.default): ManagedVal[RDD[RecordPointer]] = {
    loadIndex(path, prefixes, getFileOffsetLength, if (mergePointers == 0) DefaultMergePointers else mergePointers, HdfsIO.fs.getDefaultBlockSize(new Path(path)), sorted)
  }

  def loadIndex(
      path: String,
      prefixes: SortedPrefixSeq,
      getFileOffsetLength: String => (String, Long, Long),
      mergePointers: Int,
      mergeTolerance: Long,
      sorted: Boolean
  )(implicit accessContext: AccessContext): ManagedVal[RDD[RecordPointer]] = {
    val prefixBroadcast = sc.broadcast(prefixes)
    loadIndexWithPrefixesBroadcast(path, prefixBroadcast, getFileOffsetLength, mergePointers, mergeTolerance, sorted).map(identity, _ => prefixBroadcast.unpersist())
  }

  def loadTextLinesFromIndex(
      indexPath: String,
      prefixes: SortedPrefixSeq,
      getFileOffsetLength: String => (String, Long, Long),
      mergePointers: Int = 0,
      filter: String => Boolean = _ => true,
      repartition: Int = parallelism,
      sorted: Boolean = false,
      cache: Boolean = false,
      shuffle: Boolean = true,
      indexLinesPerPartition: Int = -1
  )(implicit accessContext: AccessContext = AccessContext.default): ManagedVal[RDD[String]] = {
    val prefixBroadcast = sc.broadcast(prefixes)
    loadIndexWithPrefixesBroadcast(
      indexPath,
      prefixBroadcast,
      getFileOffsetLength,
      if (mergePointers == 0) if (repartition > 0) DefaultMergePointers else -1 else mergePointers,
      if (repartition == 0) HdfsIO.fs.getDefaultBlockSize(new Path(indexPath)) else 0,
      sorted = sorted,
      cache = cache,
      linesPerPartition = indexLinesPerPartition
    ).map({ index =>
      val repartitioned =
        if (repartition > 0) {
          if (shuffle) RddUtil.shuffle(index, repartition)
          else {
            if (indexLinesPerPartition > 0) {
              index.coalesce(repartition)
            }
            else {
              index.repartition(repartition)
            }
          }
        } else index
      repartitioned.flatMap { pointer =>
        val in = HdfsIO.open(pointer.path, pointer.position.offset, pointer.position.length)
        IteratorUtil.cleanup(IndexUtil.filter(IOUtil.lines(in), MultiBufferedIterator(prefixBroadcast.value.drop(pointer.prefixIdx))).map(_._1).filter(filter), in.close)
      }
    }, _ => prefixBroadcast.unpersist())
  }

  def loadBinary[D: ClassTag](
      path: String,
      prefixes: SortedPrefixSeq,
      getIndexPath: String => String,
      getOffsetLength: String => (Long, Long),
      load: Iterator[InputStream] => Iterator[D],
      sorted: Boolean = false
  )(implicit accessContext: AccessContext = AccessContext.default): ManagedVal[RDD[D]] = {
    lazy val prefixBroadcast = sc.broadcast(prefixes)
    ManagedVal(loadBinaryWithPrefixBroadcast(path, prefixBroadcast, getIndexPath, getOffsetLength, load, sorted = sorted), _ => prefixBroadcast.unpersist())
  }

  def loadBinaryWithPrefixBroadcast[D: ClassTag](
      path: String,
      prefixBroadcast: Broadcast[SortedPrefixSeq],
      getIndexPath: String => String,
      getOffsetLength: String => (Long, Long),
      load: Iterator[InputStream] => Iterator[D],
      sorted: Boolean = false
  )(implicit accessContext: AccessContext = AccessContext.default): RDD[D] = {
    RddUtil.loadBinary(path, decompress = false, close = false, sorted = sorted) { (file, in) =>
      val indexPath = getIndexPath(file)
      val splitStreams = IndexUtil.splitStream(in, HdfsIO.lines(indexPath), prefixBroadcast.value, getOffsetLength)
      if (file.toLowerCase.endsWith(GzipExt)) {
        val unzipped = splitStreams.map(stream => IOUtil.decompress(stream))
        IteratorUtil.cleanup(load(unzipped), in.close)
      } else { IteratorUtil.cleanup(load(splitStreams), in.close) }
    }
  }

  def loadTextLines(
      path: String,
      prefixes: SortedPrefixSeq,
      getIndexPath: String => String,
      getOffsetLength: String => (Long, Long),
      filter: String => Boolean = _ => true,
      sorted: Boolean = false
  )(implicit accessContext: AccessContext = AccessContext.default): ManagedVal[RDD[String]] = {
    lazy val prefixBroadcast = sc.broadcast(prefixes)
    ManagedVal(loadBinaryWithPrefixBroadcast(
      path,
      prefixBroadcast,
      getIndexPath,
      getOffsetLength,
      { streams =>
        streams.flatMap { stream =>
          val buffered = IOUtil.lines(stream).buffered
          if (buffered.hasNext) {
            prefixBroadcast.value.from(buffered.head).chain { prefixes =>
              IndexUtil.filter(IOUtil.lines(stream), MultiBufferedIterator(prefixes)).map(_._1).filter(filter)
            }
          } else Iterator.empty
        }
      },
      sorted = sorted
    ), _ => prefixBroadcast.unpersist())
  }

  def loadTextLinesGroupedByPrefixFromIndex[D: ClassTag](
      indexPath: String,
      prefixes: SortedPrefixSeq,
      getFileOffsetLength: String => (String, Long, Long),
      getPrefix: (String, String) => String = (prefix, line) => prefix,
      mergePointers: Int = 0,
      filter: String => Boolean = _ => true,
      repartition: Int = parallelism,
      partitionByPrefix: Boolean = false,
      sorted: Boolean = false,
      cache: Boolean = true,
      dataPath: Option[String] = None,
      shuffle: Boolean = true,
      indexLinesPerPartition: Int = -1
  )(implicit accessContext: AccessContext = AccessContext.default): ManagedVal[RDD[(String, Iterator[String])]] = {
    def groupKey(prefix: String, line: String)(implicit accessContext: AccessContext = AccessContext.default): String = Try(getPrefix(prefix, line)).getOrElse(line)

    lazy val prefixBroadcast = sc.broadcast(prefixes)
    var filesBroadcast: Option[Broadcast[Seq[String]]] = None

    loadIndexWithPrefixesBroadcast(
      indexPath,
      prefixBroadcast,
      getFileOffsetLength,
      if (mergePointers == 0) if (repartition > 0) DefaultMergePointers else -1 else mergePointers,
      if (repartition == 0) HdfsIO.fs.getDefaultBlockSize(new Path(indexPath)) else 0,
      groupByPrefix = partitionByPrefix,
      sorted = sorted,
      cache = cache,
      linesPerPartition = indexLinesPerPartition
    ).map({ index =>
      val prefixBc = prefixBroadcast

      val files = dataPath.map(HdfsIO.files(_)).getOrElse(RddUtil.collectDistinct(RddUtil.distinctSorted(index.map(_.path)))).toSeq.sorted
      filesBroadcast = Some(sc.broadcast(files))

      val repartitioned =
        if (repartition > 0) {
          if (shuffle) RddUtil.shuffle(index, repartition)
          else {
            if (indexLinesPerPartition > 0) {
              index.coalesce(repartition)
            }
            else {
              index.repartition(repartition)
            }
          }
        } else index

      repartitioned.flatMap { pointer =>
        val files = filesBroadcast.get.value
        val filePath = pointer.path
        val fileIdx = files.indexOf(filePath)
        prefixBc.value.drop(pointer.prefixIdx).chain { p =>
          if (fileIdx < 0 || !p.hasNext) Iterator.empty
          else {
            val prefixes = MultiBufferedIterator(p)
            val in = HdfsIO.open(filePath, offset = pointer.position.offset, length = pointer.position.length, decompress = false)

            val lines = IteratorUtil.cleanup(IOUtil.lines(in, Some(filePath)), in.close)

            val overLines = CleanupIterator.lazyIter {
              val in = HdfsIO.open(filePath, offset = pointer.position.offset + pointer.position.length)
              CleanupIterator.combine(
                IteratorUtil.cleanup(IOUtil.lines(in), in.close),
                CleanupIterator.flatten(files.drop(fileIdx + 1).toIterator.map { file =>
                  val in = HdfsIO.open(file)
                  IteratorUtil.cleanup(IOUtil.lines(in), in.close)
                })
              )
            }

            var firstLine = lines.headOption
            val relevant =
              (IndexUtil.filter(lines, prefixes) ++ IteratorUtil.lazyIter {
                if (overLines.hasNext) overLines.chain { lines =>
                  val overLine = lines.head
                  if (firstLine.isEmpty) firstLine = Some(overLine)
                  IteratorUtil.dropWhile(prefixes)(p => p < overLine && !overLine.startsWith(p))
                  if (prefixes.hasNext && overLine.startsWith(prefixes.head)) {
                    val prefix = prefixes.head
                    val group = groupKey(prefix, overLine)
                    IteratorUtil.takeWhile(lines)(l => l.startsWith(prefix) && groupKey(prefix, l) == group).map((_, prefix))
                  } else Iterator.empty
                }
                else Iterator.empty
              }).buffered

            if (relevant.hasNext) {
              val skipFirst = (fileIdx > 0 || pointer.position.offset > 0) && relevant.head._1 == firstLine.get
              val grouped = IteratorUtil.groupSortedBy(relevant.filter { case (line, prefix) => filter(line) }) { case (line, prefix) => groupKey(prefix, line) }.map { case (group, records) =>
                group -> records.map(_._1)
              }
              if (skipFirst) grouped.drop(1) else grouped
            } else Iterator.empty
          }
        }
      }
    }, _ => {
      prefixBroadcast.unpersist()
      for (b <- filesBroadcast) b.unpersist()
    })
  }
}
