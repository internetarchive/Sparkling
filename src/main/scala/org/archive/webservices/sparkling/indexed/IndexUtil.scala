package org.archive.webservices.sparkling.indexed

import java.io.InputStream
import org.archive.webservices.sparkling.io.IOUtil
import org.archive.webservices.sparkling.util.{IteratorUtil, MultiBufferedIterator, StringUtil}

import scala.util.Try

object IndexUtil {
  def filter(lines: TraversableOnce[String], prefixes: Set[String]): Iterator[(String, String)] = filter(lines, MultiBufferedIterator(prefixes.toSeq.sorted.toIterator))

  def filter(lines: TraversableOnce[String], sortedPrefixes: MultiBufferedIterator[String]): Iterator[(String, String)] = StringUtil.matchPrefixes(lines, sortedPrefixes, strict = false)

  def lineCandidates(index: TraversableOnce[String], prefixes: Set[String]): Iterator[(String, Int)] = lineCandidates(index, SortedPrefixSeq.fromSet(prefixes))

  def lineCandidates(index: TraversableOnce[String], sortedPrefixes: SortedPrefixSeq): Iterator[(String, Int)] = {
    val buffered = IteratorUtil.zipNext(index.toIterator).buffered
    if (buffered.hasNext) {
      var candidates = sortedPrefixes.fromWithIndex(buffered.head._1).buffered
      IteratorUtil.whileDefined(buffered.map { case (line, next) =>
        IteratorUtil.dropWhile(candidates) { case (prefix, _) => prefix < line && !line.startsWith(prefix) }
        if (candidates.hasNext) Some({
          val (prefix, idx) = candidates.head
          if (next.isEmpty || next.get >= prefix) Some(line, idx) else None
        })
        else None
      }).flatten
    } else Iterator.empty
  }

  def mergeRecordPointers(sortedPointers: TraversableOnce[RecordPointer], max: Int = -1, tolerance: Long = 0, groupByPrefix: Boolean = false): Iterator[RecordPointer] = {
    val buffered = sortedPointers.toIterator.buffered
    if (!buffered.hasNext) return buffered
    var currentPointer = buffered.head
    var (currentStart, currentEnd) = (currentPointer.position.offset, currentPointer.position.offset + currentPointer.position.length)
    var currentPrefixIdx = currentPointer.prefixIdx
    var mergeCount = 1
    buffered.drop(1).flatMap { pointer =>
      val (start, end) = (pointer.position.offset, pointer.position.offset + pointer.position.length)
      if (
        (mergeCount < max || max < 0 || (groupByPrefix && currentPrefixIdx == pointer.prefixIdx)) && currentPointer.path == pointer.path &&
        (tolerance < 0 || currentEnd + tolerance >= start)
      ) {
        currentEnd = end
        mergeCount += 1
        None
      } else {
        val mergedPointer = if (mergeCount == 1) currentPointer else currentPointer.copy(position = PositionPointer(currentStart, currentEnd - currentStart), prefixIdx = currentPrefixIdx)
        currentPointer = pointer
        currentStart = start
        currentEnd = end
        currentPrefixIdx = pointer.prefixIdx
        mergeCount = 1
        Some(mergedPointer)
      }
    } ++ IteratorUtil.getLazy(_ => if (mergeCount == 1) currentPointer else currentPointer.copy(position = PositionPointer(currentStart, currentEnd - currentStart), prefixIdx = currentPrefixIdx))
  }

  def lookup(index: TraversableOnce[String], prefixes: Set[String], getOffsetLength: String => (Long, Long)): Iterator[PositionPointer] = {
    lookup(index, SortedPrefixSeq.fromSet(prefixes), getOffsetLength)
  }

  def lookup(index: TraversableOnce[String], sortedPrefixes: SortedPrefixSeq, getOffsetLength: String => (Long, Long)): Iterator[PositionPointer] = {
    var buffered = IteratorUtil.zipNext(index.toIterator).buffered
    val candidates = sortedPrefixes.from(buffered.head._1)
    if (candidates.hasNext) {
      val ranges = candidates.flatMap { prefix =>
        if (buffered.hasNext) {
          val next = buffered.head._1
          if (next > prefix && !next.startsWith(prefix)) None // included in previous range
          else {
            while (
              buffered.hasNext && {
                val (_, next) = buffered.head
                next.isDefined && next.get < prefix
              }
            ) buffered.next()
            val first = buffered.head._1
            while (
              buffered.hasNext && {
                val (_, next) = buffered.head
                next.isDefined && next.get.startsWith(prefix)
              }
            ) buffered.next()
            val last = buffered.head._1
            buffered.next()
            Try {
              val (firstOffset, firstLength) = getOffsetLength(first)
              val (lastOffset, lastLength) = getOffsetLength(last)
              (firstOffset, lastOffset + lastLength)
            }.toOption
          }
        } else None
      }.buffered

      if (ranges.hasNext) {
        var currentRange = ranges.head
        val merged = ranges.drop(1).flatMap { case (start, end) =>
          if (currentRange._2 >= start) {
            currentRange = (currentRange._1, end)
            None
          } else {
            val mergedRange = currentRange
            currentRange = (start, end)
            Some(mergedRange)
          }
        } ++ IteratorUtil.getLazy(_ => currentRange)

        merged.map { case (start, end) => PositionPointer(start, end - start) }
      } else Iterator.empty
    } else Iterator.empty
  }

  def splitStream(stream: InputStream, index: TraversableOnce[String], prefixes: Set[String], getOffsetLength: String => (Long, Long)): Iterator[InputStream] = {
    splitStream(stream, index, SortedPrefixSeq.fromSet(prefixes), getOffsetLength)
  }

  def splitStream(stream: InputStream, index: TraversableOnce[String], sortedPrefixed: SortedPrefixSeq, getOffsetLength: String => (Long, Long)): Iterator[InputStream] = {
    val positions = lookup(index, sortedPrefixed, getOffsetLength)
    IOUtil.splitStream(stream, positions.map(p => (p.offset, p.length)))
  }
}
