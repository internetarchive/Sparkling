package org.archive.webservices.sparkling.indexed

import java.io.InputStream
import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.archive.webservices.sparkling.io.{HdfsIO, IOUtil}
import org.archive.webservices.sparkling.util.{CleanupIterator, IteratorUtil, MultiBufferedIterator, RddUtil}

import scala.reflect.ClassTag
import scala.util.Try

object PrefixIndexLoader {
  import org.archive.webservices.sparkling.Sparkling._

  val DefaultMergePointers = 100

//  sc.getConf.registerKryoClasses(Array(
//    classOf[PositionPointer],
//    classOf[RecordPointer],
//    classOf[SortedPrefixSeq],
//    classOf[BasicSortedPrefixSeq],
//    classOf[HdfsSortedPrefixSeq]
//  ))

  private def loadIndexWithPrefixesBroadcast(
      path: String,
      prefixBroadcast: Broadcast[SortedPrefixSeq],
      getFileOffsetLength: String => (String, Long, Long),
      mergePointers: Int,
      mergeTolerance: Long,
      groupByPrefix: Boolean = false,
      sorted: Boolean = false,
      cache: Boolean = true,
      linesPerPartition: Int = -1
  ): RDD[RecordPointer] = {
    val rdd = (if (linesPerPartition > 0) { RddUtil.loadTextPartitionsByLinesWithFilenames(path, linesPerPartition = linesPerPartition) }
               else { RddUtil.loadBinary(path, decompress = false, close = false, sorted = sorted) { (indexFile, in) => Iterator((indexFile, IteratorUtil.cleanup(IOUtil.lines(in), in.close))) } })
      .flatMap { case (indexFile, lines) =>
        lines.chain { l =>
          val pointers = IndexUtil.lineCandidates(l, prefixBroadcast.value).flatMap { case (line, prefixStart) =>
            Try {
              val (file, offset, length) = getFileOffsetLength(line)
              val filePath = new Path(new Path(indexFile, ".."), file).toString
              RecordPointer(filePath, PositionPointer(offset, length), prefixStart)
            }.toOption
          }
          IndexUtil.mergeRecordPointers(pointers, mergePointers, mergeTolerance, groupByPrefix)
        }
      }
    if (cache) rdd.persist(StorageLevel.MEMORY_AND_DISK) else rdd
  }

  def loadIndex(path: String, prefixes: SortedPrefixSeq, getFileOffsetLength: String => (String, Long, Long), mergePointers: Int): RDD[RecordPointer] = {
    loadIndex(path, prefixes, getFileOffsetLength, if (mergePointers == 0) DefaultMergePointers else mergePointers, HdfsIO.fs.getDefaultBlockSize(new Path(path)))
  }

  def loadIndex(
      path: String,
      prefixes: SortedPrefixSeq,
      getFileOffsetLength: String => (String, Long, Long),
      mergePointers: Int = DefaultMergePointers,
      mergeTolerance: Long = 0,
      sorted: Boolean = false
  ): RDD[RecordPointer] = { loadIndexWithPrefixesBroadcast(path, sc.broadcast(prefixes), getFileOffsetLength, mergePointers, mergeTolerance) }

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
  ): RDD[String] = {
    val prefixBroadcast = sc.broadcast(prefixes)
    val index = loadIndexWithPrefixesBroadcast(
      indexPath,
      prefixBroadcast,
      getFileOffsetLength,
      if (mergePointers == 0) if (repartition > 0) DefaultMergePointers else -1 else mergePointers,
      if (repartition == 0) HdfsIO.fs.getDefaultBlockSize(new Path(indexPath)) else 0,
      sorted = sorted,
      cache = cache,
      linesPerPartition = indexLinesPerPartition
    )
    val repartitioned =
      if (repartition > 0) {
        if (shuffle) RddUtil.shuffle(index, repartition)
        else {
          if (indexLinesPerPartition > 0) { index.coalesce(repartition) }
          else { index.repartition(repartition) }
        }
      } else index
    repartitioned.flatMap { pointer =>
      val in = HdfsIO.open(pointer.path, pointer.position.offset, pointer.position.length)
      IteratorUtil.cleanup(IndexUtil.filter(IOUtil.lines(in), MultiBufferedIterator(prefixBroadcast.value.drop(pointer.prefixIdx))).map(_._1).filter(filter), in.close)
    }
  }

  def loadBinary[D: ClassTag](
      path: String,
      prefixes: SortedPrefixSeq,
      getIndexPath: String => String,
      getOffsetLength: String => (Long, Long),
      load: Iterator[InputStream] => Iterator[D],
      sorted: Boolean = false
  ): RDD[D] = { loadBinaryWithPrefixBroadcast(path, sc.broadcast(prefixes), getIndexPath, getOffsetLength, load, sorted = sorted) }

  def loadBinaryWithPrefixBroadcast[D: ClassTag](
      path: String,
      prefixBroadcast: Broadcast[SortedPrefixSeq],
      getIndexPath: String => String,
      getOffsetLength: String => (Long, Long),
      load: Iterator[InputStream] => Iterator[D],
      sorted: Boolean = false
  ): RDD[D] = {
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
  ): RDD[String] = {
    val prefixBroadcast = sc.broadcast(prefixes)
    loadBinaryWithPrefixBroadcast(
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
    )
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
  ): RDD[(String, Iterator[String])] = {
    def groupKey(prefix: String, line: String): String = Try(getPrefix(prefix, line)).getOrElse(line)

    val prefixBroadcast = sc.broadcast(prefixes)
    val index = loadIndexWithPrefixesBroadcast(
      indexPath,
      prefixBroadcast,
      getFileOffsetLength,
      if (mergePointers == 0) if (repartition > 0) DefaultMergePointers else -1 else mergePointers,
      if (repartition == 0) HdfsIO.fs.getDefaultBlockSize(new Path(indexPath)) else 0,
      groupByPrefix = partitionByPrefix,
      sorted = sorted,
      cache = cache,
      linesPerPartition = indexLinesPerPartition
    )

    val files = dataPath.map(HdfsIO.files(_)).getOrElse(RddUtil.collectDistinct(RddUtil.distinctSorted(index.map(_.path)))).toSeq.sorted
    val filesBroadcast = sc.broadcast(files)

    val repartitioned =
      if (repartition > 0) {
        if (shuffle) RddUtil.shuffle(index, repartition)
        else {
          if (indexLinesPerPartition > 0) { index.coalesce(repartition) }
          else { index.repartition(repartition) }
        }
      } else index
    repartitioned.flatMap { pointer =>
      val files = filesBroadcast.value
      val filePath = pointer.path
      val fileIdx = files.indexOf(filePath)
      prefixBroadcast.value.drop(pointer.prefixIdx).chain { p =>
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
  }
}
