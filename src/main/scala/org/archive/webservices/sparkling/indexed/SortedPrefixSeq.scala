package org.archive.webservices.sparkling.indexed

import org.archive.webservices.sparkling.io.HdfsBackedMap
import org.archive.webservices.sparkling.util.CleanupIterator

trait SortedPrefixSeq extends Serializable {
  def iter: CleanupIterator[String]
  def from(from: String): CleanupIterator[String]
  def fromWithIndex(from: String): CleanupIterator[(String, Int)]
  def slice(from: Int, until: Int): CleanupIterator[String] = drop(from).chain(_.take(until - from))
  def drop(n: Int): CleanupIterator[String]
}

class BasicSortedPrefixSeq(sortedSeq: Seq[String]) extends SortedPrefixSeq {
  def iter: CleanupIterator[String] = CleanupIterator(sortedSeq.toIterator)
  def from(from: String): CleanupIterator[String] = CleanupIterator(sortedSeq.dropWhile { prefix => prefix < from && !from.startsWith(prefix) }.toIterator)
  def fromWithIndex(from: String): CleanupIterator[(String, Int)] = CleanupIterator(sortedSeq.zipWithIndex.dropWhile { case (prefix, _) => prefix < from && !from.startsWith(prefix) }.toIterator)
  def drop(n: Int): CleanupIterator[String] = CleanupIterator(sortedSeq.drop(n).toIterator)
}

class HdfsSortedPrefixSeq(val hdfsBackedMap: HdfsBackedMap[_]) extends SortedPrefixSeq {
  def iter: CleanupIterator[String] = hdfsBackedMap.iter.chain(_.map { case (k, _) => k })
  def from(from: String): CleanupIterator[String] = hdfsBackedMap.from(from, true).chain(_.map { case (k, _) => k })
  def fromWithIndex(from: String): CleanupIterator[(String, Int)] = hdfsBackedMap.fromWithIndex(from, true).chain(_.map { case ((k, _), idx) => (k, idx) })
  def drop(n: Int): CleanupIterator[String] = hdfsBackedMap.dropKeys(n).chain(_.map { case (k, _) => k })
}

object SortedPrefixSeq {
  implicit def fromSet(set: Set[String]): SortedPrefixSeq = new BasicSortedPrefixSeq(set.toSeq.sorted)
  def fromSortedFiles(path: String): SortedPrefixSeq = fromSortedFiles(path, identity, true, true)
  def fromSortedFiles(path: String, cache: Boolean): SortedPrefixSeq = fromSortedFiles(path, identity, cache, true)
  def fromSortedFiles(path: String, cache: Boolean, preloadLength: Boolean): SortedPrefixSeq = fromSortedFiles(path, identity, cache, preloadLength)
  def fromSortedFiles(path: String, prefix: String => String, cache: Boolean = true, preloadLength: Boolean = true): SortedPrefixSeq =
    fromHdfsBackedMapKeys(HdfsBackedMap(path, prefix, cache, preloadLength))
  def fromHdfsBackedMapKeys(hdfsBackedMap: HdfsBackedMap[_]): SortedPrefixSeq = new HdfsSortedPrefixSeq(hdfsBackedMap)
}
