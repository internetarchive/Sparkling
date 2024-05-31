package org.archive.webservices.sparkling.util

import java.io.{File, FileInputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import org.apache.commons.io.output.CountingOutputStream
import org.apache.hadoop.fs.Path
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.archive.webservices.sparkling.{Sparkling, _}
import org.archive.webservices.sparkling.io.{HdfsIO, IOUtil, NonClosingOutputStream, TypedInOut}
import org.archive.webservices.sparkling.logging.{Log, LogContext}

import scala.reflect.ClassTag

object FileShuffleUtil {
  implicit val logContext: LogContext = LogContext(this)

  import org.archive.webservices.sparkling.Sparkling._

  var logMod: Int = prop(10000000)(logMod, logMod = _)
  var checkpointId: String = prop("file-shuffle")(checkpointId, checkpointId = _)
  var maxSortBytes: Long = prop(500.mb)(maxSortBytes, maxSortBytes = _)
  var taskTimeoutMillis: Int = prop(1000 * 60 * 20)(taskTimeoutMillis, taskTimeoutMillis = _) // 20 minutes

  def repartitionUniformly[A: ClassTag: TypedInOut, P: ClassTag: Ordering](rdd: RDD[A])(partitionBy: A => P): RDD[A] = { repartitionUniformlyAs[A, A, P](rdd)(partitionBy, identity) }

  def repartitionUniformlyAs[A: ClassTag, B: ClassTag: TypedInOut, P: ClassTag: Ordering](rdd: RDD[A])(partitionBy: A => P, as: A => B): RDD[B] = repartitionAs(rdd)(
    as, {
      val counts = rdd.map(r => (partitionBy(r), 1L)).reduceByKey(_ + _).collect
      val maxCount = counts.map(_._2).max
      var partition = 0
      var partitionCount = 0L
      val partitions = counts.sortBy(_._1).map { case (p, c) =>
        if (partitionCount + c <= maxCount) {
          partitionCount += c
          (p, partition)
        } else {
          partition += 1
          partitionCount = c
          (p, partition)
        }
      }.toMap
      val partitionsBroadcast = sc.broadcast(partitions)
      record => partitionsBroadcast.value(partitionBy(record))
    }
  )

  def repartition[A: ClassTag: TypedInOut](rdd: RDD[A])(partition: => A => Int): RDD[A] = { repartitionAs[A, A](rdd)(identity, partition) }

  def repartitionAs[A: ClassTag, B: ClassTag: TypedInOut](rdd: RDD[A])(as: A => B, partition: => A => Int): RDD[B] = {
    val inout = implicitly[TypedInOut[B]]
    val tmpDir = getCheckpointOrTmpDir(checkpointId)
    val partitionPointers = Sparkling.checkpointStrings(
      checkpointId + "_pointers", {
        val mapPartition = partition
        RddUtil.lazyMapPartitions(rdd) { case (idx, records) =>
          val files = collection.mutable.Map.empty[Int, (File, inout.TypedInOutWriter)]
          Common.timeoutWithReporter(taskTimeoutMillis) { reporter =>
            var recordCount = 0L
            for (record <- records) {
              val p = mapPartition(record)
              val (file, out) = files.getOrElseUpdate(
                p, {
                  val file = IOUtil.tmpFile
                  (file, inout.out(new GZIPOutputStream(Sparkling.fileOutPool.get(file))))
                }
              )
              out.write(as(record))
              recordCount += 1
              reporter.alive(s"Records written: $recordCount...")
            }
            val finalMsg = s"All records written. ($recordCount records to ${files.size} files)"
            reporter.alive(finalMsg)
            Log.info(finalMsg)
            for ((_, stream) <- files.values) {
              reporter.alive()
              stream.close()
            }
          }
          val filesCount = files.size
          val tmpPath = tmpDir + "/" + StringUtil.padNum(idx, 5) + GzipExt
          val out = HdfsIO.out(tmpPath, compress = false, overwrite = true, temporary = true)
          var offset = 0L
          var merged = 0L
          IteratorUtil.cleanup(
            files.toIterator.map { case (p, (tmpFile, _)) =>
              val file = tmpFile
              val length = file.length()
              val in = new FileInputStream(file)
              IOUtil.copy(in, out)
              in.close()
              file.delete()
              merged += 1
              Log.info(s"$merged / $filesCount files merged...")
              val pointer = p -> (tmpPath, offset, length)
              offset += length
              pointer
            },
            out.close
          )
        }
      }
    )(
      { case (p, (tmpPath, offset, length)) => Seq(p, tmpPath, offset, length).mkString("\t") },
      str => {
        val split = str.split('\t')
        split(0).toInt -> (split(1), split(2).toLong, split(3).toLong)
      }
    ).cache
    val numPartitions = partitionPointers.keys.max + 1
    partitionPointers.partitionBy(new HashPartitioner(numPartitions)).mapPartitions { pointers =>
      val pointersSeq = pointers.toSeq
      val numPointers = pointersSeq.size
      var processing = 0
      // sort randomly to have parallel executors access different files at a time
      pointersSeq.sortBy(_.hashCode).toIterator.flatMap { case (_, (file, offset, length)) =>
        processing += 1
        Log.info(s"Reading part $processing / $numPointers...")
        val in = HdfsIO.open(file, offset, length)
        IteratorUtil.cleanup(inout.in(in), in.close)
      }
    }
  }

  def sortWithinPartitions[A: ClassTag: TypedInOut, S: ClassTag: Ordering](rdd: RDD[A])(length: A => Long, sortBy: A => S): RDD[A] = {
    val ordering = implicitly[Ordering[S]]
    val inout = implicitly[TypedInOut[A]]
    RddUtil.lazyMapPartitions(rdd) { (_, records) =>
      var writtenChunks = 0L
      var writtenLines = 0L
      val chunks = Common.timeoutWithReporter(taskTimeoutMillis) { reporter =>
        IteratorUtil.grouped(records, maxSortBytes)(length).map { chunk =>
          val sorted = chunk.toSeq.sortBy(sortBy)
          val file = IOUtil.tmpFile
          val out = inout.out(new GZIPOutputStream(Sparkling.fileOutPool.get(file)))
          sorted.foreach(out.write)
          out.close()
          writtenChunks += 1
          writtenLines += sorted.size
          val statusMsg = s"Chunks written out: ${writtenChunks.readable} (${writtenLines.readable} records)..."
          Log.info(statusMsg)
          reporter.alive(statusMsg)
          val in = new GZIPInputStream(new FileInputStream(file))
          IteratorUtil.cleanup(
            inout.in(in),
            () => {
              reporter.alive()
              in.close()
              file.delete()
            }
          )
        }.toArray
      }
      val chunkOrdering = Ordering.by[CleanupIterator[A], S](iter => sortBy(iter.head)).reverse
      val queue = collection.mutable.PriorityQueue(chunks.filter(_.hasNext): _*)(chunkOrdering)
      var writtenOut = 0L
      IteratorUtil.whileDefined {
        if (queue.isEmpty) None
        else Some {
          val chunk = queue.dequeue
          val next = if (queue.isEmpty) None else Some(sortBy(queue.head.head))
          if (next.isEmpty) chunk else IteratorUtil.takeWhile(chunk)(r => ordering.lteq(sortBy(r), next.get)) ++ IteratorUtil.noop { if (chunk.hasNext) queue.enqueue(chunk) }
        }
      }.flatten.map { identity =>
        writtenOut += 1
        if (writtenOut % logMod == 0) Log.info(s"Output processed: ${writtenOut.readable} / ${writtenLines.readable} records...")
        identity
      }
    }
  }

  def sortStringsWithinPartitions(rdd: RDD[String], sortBy: String => String = identity): RDD[String] = { sortWithinPartitions(rdd)(_.length, sortBy) }

  def sortBy[A: ClassTag: TypedInOut, S: ClassTag: Ordering](rdd: RDD[A])(length: A => Long, sortBy: A => S): RDD[A] = { sortWithinPartitions(repartitionUniformly(rdd)(sortBy))(length, sortBy) }

  def sortStringsThroughPartitioning(rdd: RDD[String], delimiter: String = "\t")(partitionBy: String => String): RDD[String] =
    sortStringsThroughPartitioningBy(rdd)(partitionBy, str => partitionBy(str) + delimiter + str)
  def sortStringsThroughPartitioningBy(rdd: RDD[String])(partitionBy: String => String, sortBy: String => String): RDD[String] =
    sortStringsWithinPartitions(repartitionUniformlyAs(rdd)(partitionBy, identity), sortBy)

  def sortStringsThroughIndexing[R](rdd: RDD[String], delimiter: String = "\t")(indexBy: String => String)(action: RDD[String] => R): R =
    sortStringsThroughIndexingBy(rdd)(indexBy, str => indexBy(str) + delimiter + str)(action)
  def sortStringsThroughIndexingBy[R](rdd: RDD[String])(indexBy: String => String, sortBy: String => String)(action: RDD[String] => R): R = {
    Sparkling.checkpointOrTmpDir("sort-index") { checkpointDir =>
      val path = checkpointDir + "/indexed.gz"
      val sorted = sortStringsWithinPartitions(rdd, sortBy)
      RddUtil.saveTextWithIndex(sorted, path, skipIfExists = true, checkPerFile = true, key = indexBy, groupByKey = true)
      action(mergeStringsByIndex(path, sortedBy = sortBy))
    }
  }

  def smartSortStrings[R](rdd: RDD[String], minPartitions: Int = 30, prefixSeparator: Option[String] = None, minPrefixLength: Int = 1, maxPrefixLength: Int = -1, sortBy: String => String = identity)(
      action: RDD[String] => R
  ): R = {
    Sparkling.checkpointOrTmpDir("sort-index") { checkpointDir =>
      val path = checkpointDir + "/indexed.gz"

      val sorted = sortStringsWithinPartitions(rdd, sortBy)
      RddUtil.saveTextWithIndex(
        sorted,
        path,
        skipIfExists = true,
        checkPerFile = true,
        key = { str =>
          val key = prefixSeparator.map(StringUtil.prefixBySeparator(sortBy(str), _)).getOrElse(sortBy(str))
          if (maxPrefixLength > 0) key.take(maxPrefixLength) else key
        },
        groupByKey = true
      )

      action(smartMergeStringsByIndex(path, minPartitions = minPartitions, minPrefixLength = minPrefixLength, sortedBy = sortBy))
    }
  }

  def sortStringsBuckets[R](rdd: RDD[String], numBuckets: Int = 30, candidatesFactor: Int = 3, sortBy: String => String = identity, partitionBy: String => String = identity)(
      action: RDD[String] => R
  ): R = {
    Sparkling.checkpointOrTmpDir("sort-buckets") { checkpointDir =>
      val sortedPartitions = RddUtil.cache(sortStringsWithinPartitions(rdd, sortBy))

      val partitionCounts = sortedPartitions.mapPartitionsWithIndex { case (i, p) => Iterator(i -> IteratorUtil.count(p)) }.collectAsMap
      val partitionCountsBc = sc.broadcast(partitionCounts)
      val numRecords = partitionCounts.values.sum
      val bucketFraction = 1.0 / numBuckets

      val candidates = sortedPartitions.mapPartitionsWithIndex { case (idx, records) =>
        val partitionCounts = partitionCountsBc.value
        val count = partitionCounts(idx)
        val recordsPerBucket = (bucketFraction * count / candidatesFactor).ceil.toLong
        IteratorUtil.groupedGroups(IteratorUtil.groupSorted(records, partitionBy).map(_._2), recordsPerBucket)(_ => 1L).map(_.buffered).map { group =>
          var last = group.head
          val size = group.map { r =>
            last = r
            1L
          }.sum
          (partitionBy(last), size)
        }
      }.collect.toSeq.sortBy(_._1)

      val recordsPerBucket = (bucketFraction * numRecords).ceil.toLong
      val bucketBoundaries = IteratorUtil.grouped(candidates, recordsPerBucket)(_._2).toSeq.flatMap(IteratorUtil.last(_)).map(_._1).take(numBuckets - 1)
      val bucketBoundariesBc = sc.broadcast(bucketBoundaries)

      val buckets = sortedPartitions.mapPartitionsWithIndex { case (idx, records) =>
        val bucketBoundaries = bucketBoundariesBc.value
        val buffered = records.buffered
        val buckets = bucketBoundaries.toIterator.map(b => IteratorUtil.takeWhile(buffered)(partitionBy(_) < b)) ++ Iterator(buffered)
        val partitionOutPath = new Path(checkpointDir, StringUtil.padNum(idx, 5) + Sparkling.GzipExt).toString
        val counting = new CountingOutputStream(HdfsIO.out(partitionOutPath, compress = false, overwrite = true, temporary = true))
        var offset = 0L
        IteratorUtil.cleanup(
          buckets.zipWithIndex.filter(_._1.hasNext).map { case (bucket, i) =>
            val out = new GZIPOutputStream(new NonClosingOutputStream(counting))
            val print = IOUtil.print(out, closing = true)
            for (r <- bucket) print.println(r)
            print.close()
            val length = counting.getByteCount - offset
            val pointer = (i, partitionOutPath, offset, length)
            offset = counting.getByteCount
            pointer
          },
          counting.close
        )
      }.collect.groupBy(_._1).toSeq.sortBy(_._1).map(_._2)

      sortedPartitions.unpersist(true)

      val sorted = RddUtil.parallelize(buckets).mapPartitions { pointers =>
        val ordering = implicitly[Ordering[String]]
        val chunks = pointers.toSeq.flatten.map { case (_, path, offset, length) =>
          val buffered = HdfsIO.access(path, offset = offset, length = length) { in => IOUtil.copyToBuffer(in, bufferSize = 1.mb.toInt).get.get }
          IteratorUtil.cleanup(IOUtil.lines(buffered), buffered.close)
        }
        val chunkOrdering = Ordering.by[CleanupIterator[String], String](iter => sortBy(iter.head)).reverse
        val queue = collection.mutable.PriorityQueue(chunks.filter(_.hasNext): _*)(chunkOrdering)
        IteratorUtil.whileDefined {
          if (queue.isEmpty) None
          else Some {
            val chunk = queue.dequeue
            val next = if (queue.isEmpty) None else Some(sortBy(queue.head.head))
            if (next.isEmpty) chunk else IteratorUtil.takeWhile(chunk)(r => ordering.lteq(sortBy(r), next.get)) ++ IteratorUtil.noop { if (chunk.hasNext) queue.enqueue(chunk) }
          }
        }.flatten
      }

      action(sorted)
    }
  }

  def partitionByAndSortAsStrings[A: ClassTag, P: ClassTag: Ordering](rdd: RDD[A])(partitionBy: A => P, toString: A => String = (a: A) => a.toString): RDD[String] = {
    val strings = repartitionUniformlyAs(rdd)(partitionBy, toString)
    sortStringsWithinPartitions(strings)
  }

  def mergeStringsByIndexPartitions(path: String, partitions: List[(Set[String], Long)], sortedBy: String => String = identity, prefix: String => String = identity): RDD[String] = {
    RddUtil.lazyFlatMap(RddUtil.parallelize(partitions)) { case (keys, count) =>
      val chunks = HdfsIO.files(path + "/*" + Sparkling.IdxExt).map { filename =>
        val dataFile = filename.stripSuffix(Sparkling.IdxExt)
        val pointers = HdfsIO.iterLines(filename, readFully = true).dropWhile { str =>
          val key = prefix(StringUtil.prefixBySeparator(str, "\t"))
          !keys.contains(key)
        }.takeWhile { str =>
          val key = prefix(StringUtil.prefixBySeparator(str, "\t"))
          keys.contains(key)
        }.map(_.split('\t').takeRight(2).map(_.toLong))
        if (pointers.hasNext) {
          val first = pointers.next
          val (offset, length) = (first.head, first(1) + pointers.map(_(1)).sum)
          val buffered = HdfsIO.access(dataFile, offset = offset, length = length) { in => IOUtil.copyToBuffer(in, bufferSize = 1.mb.toInt).get.get }
          IteratorUtil.cleanup(IOUtil.lines(buffered), buffered.close)
        } else CleanupIterator.empty[String]
      }.toList
      val chunkOrdering = Ordering.by[CleanupIterator[String], String](iter => sortedBy(iter.head)).reverse
      val queue = collection.mutable.PriorityQueue(chunks.filter(_.hasNext): _*)(chunkOrdering)
      var writtenOut = 0L
      IteratorUtil.whileDefined {
        if (queue.isEmpty) None
        else Some {
          val chunk = queue.dequeue
          val next = if (queue.isEmpty) None else Some(sortedBy(queue.head.head))
          if (next.isEmpty) chunk else IteratorUtil.takeWhile(chunk)(r => sortedBy(r) <= next.get) ++ IteratorUtil.noop { if (chunk.hasNext) queue.enqueue(chunk) }
        }
      }.flatten.map { identity =>
        writtenOut += 1
        if (writtenOut % logMod == 0) Log.info(s"Output processed: ${writtenOut.readable} / ${count.readable}...")
        identity
      }
    }
  }

  def smartMergeStringsByIndex(path: String, minPartitions: Int = 30, minPrefixLength: Int = 1, sortedBy: String => String = identity): RDD[String] = {
    var partitions = List.empty[(Set[String], Long)]
    var expandingPrefixes = Set.empty[String]
    var expanding = true
    val indexRdd = RddUtil.loadTextLines(path + "/*" + Sparkling.IdxExt).map(_.split('\t')).map(s => (s(0), s(1).toLong)).cache
    while (partitions.length < minPartitions && expanding) {
      val expandingPrefixesBc = sc.broadcast(expandingPrefixes)
      val counts = indexRdd.mapPartitions { partition =>
        val expandingPrefixes = expandingPrefixesBc.value
        val prefixes = partition.map { case (p, c) =>
          var length = minPrefixLength
          var prefix = p.take(length)
          while (expandingPrefixes.contains(prefix) && prefix.length == length) {
            length += 1
            prefix = p.take(length)
          }
          (prefix, c)
        }
        IteratorUtil.groupSortedBy(prefixes)(_._1).map { case (p, iter) => (p, iter.map(_._2).sum) }
      }.reduceByKey(_ + _).collect.sortBy(_._1)
      expandingPrefixesBc.destroy()

      val (maxPrefix, maxCount) = counts.maxBy(_._2)
      var partitionCount = 0L
      var keys = Set.empty[String]
      partitions = List.empty[(Set[String], Long)]
      for ((key, c) <- counts) {
        if (partitionCount + c <= maxCount) {
          partitionCount += c
          keys += key
        } else {
          if (keys.nonEmpty) partitions :+= (keys, partitionCount)
          partitionCount = c
          keys = Set(key)
        }
      }
      if (keys.nonEmpty) partitions :+= (keys, partitionCount)
      if (partitions.length < minPartitions) {
        expanding = !expandingPrefixes.contains(maxPrefix)
        if (expanding) {
          println("Expanding partitions [" + StringUtil.formatNumber(partitions.length) + "/" + StringUtil.formatNumber(minPartitions) + "]: " + maxPrefix)
          expandingPrefixes += maxPrefix
        }
      }
    }
    println("Sorting strings in " + StringUtil.formatNumber(partitions.length) + " partitions...")
    val expandingPrefixesBc = sc.broadcast(expandingPrefixes)
    mergeStringsByIndexPartitions(
      path,
      partitions,
      sortedBy,
      prefix = { p =>
        val expandingPrefixes = expandingPrefixesBc.value
        var length = minPrefixLength
        var prefix = p.take(length)
        while (expandingPrefixes.contains(prefix) && prefix.length == length) {
          length += 1
          prefix = p.take(length)
        }
        prefix
      }
    )
  }

  def mergeStringsByIndex(path: String, combine: Boolean = true, lengthNotCount: Boolean = false, limit: Long = 0, sortedBy: String => String = identity): RDD[String] = {
    val counts = RddUtil.loadTextLines(path + "/*" + Sparkling.IdxExt).map(_.split('\t')).map(s => (s(0), (s(if (lengthNotCount) 3 else 1).toLong, s(1).toLong)))
      .reduceByKey { case ((l1, c1), (l2, c2)) => (l1 + l2, c1 + c2) }.collect.sortBy(_._1)
    val partitions =
      if (combine) {
        val maxLength = if (limit > 0) limit else counts.map(_._2._1).max
        var partitionLength = 0L
        var partitionCount = 0L
        var keys = Set.empty[String]
        var partitions = List.empty[(Set[String], Long)]
        for ((key, (l, c)) <- counts) {
          if (partitionLength + l <= maxLength) {
            partitionLength += l
            partitionCount += c
            keys += key
          } else {
            if (keys.nonEmpty) partitions :+= (keys, partitionCount)
            partitionLength = l
            partitionCount = c
            keys = Set(key)
          }
        }
        if (keys.nonEmpty) partitions :+= (keys, partitionCount)
        partitions
      } else { counts.map { case (key, (_, count)) => (Set(key), count) }.toList }
    mergeStringsByIndexPartitions(path, partitions, sortedBy)
  }
}
