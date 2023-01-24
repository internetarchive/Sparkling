package org.archive.webservices.sparkling.indexed

import org.archive.webservices.sparkling.io.{HdfsIO, IOUtil, PrimitiveHdfsBackedMap}
import org.archive.webservices.sparkling.util.{CleanupIterator, IteratorUtil, RddUtil}

class HdfsIndexBackedMap (val path: String, val key: String => String, startOffset: (String, String) => Long, groupFiles: Int = 1) extends PrimitiveHdfsBackedMap {
  override def cache: Boolean = false
  override def preloadLength: Boolean = false

  private val files: Seq[(Seq[String], String)] = {
    RddUtil.loadTextFiles(
      path,
      strategy = HdfsIO.LoadingStrategy.Remote,
      groupFiles = groupFiles,
      sorted = groupFiles > 1
    ).mapPartitions { fileGroup =>
      if (fileGroup.hasNext) {
        val (filename, file) = fileGroup.next
        file.chain(_.filter(_.trim.nonEmpty)).iter { lines => lines.headOption.toIterator.map(key).map { f => (Seq(filename) ++ fileGroup.map(_._1).toList, f) } }
      } else Iterator.empty
    }.collect.sortBy { case (files, first) => (first, files.head) }
  }

  private def iterFiles(files: Seq[String], beforeKey: Option[String] = None): CleanupIterator[(String, Iterator[String])] = {
    CleanupIterator.flatten(files.zipWithIndex.toIterator.map { case (file, idx) =>
      val offset = if (idx == 0 && beforeKey.isDefined) startOffset(file, beforeKey.get) else 0L
      val in = HdfsIO.open(file, offset = offset)
      IteratorUtil.cleanup(IOUtil.lines(in).filter(_.trim.nonEmpty).map(l => (file, key(l), l)), in.close)
    }).chain { lines =>
      val grouped = IteratorUtil.groupSortedBy(lines)(_._2)
      grouped.map { case (k, v) => (k, v.map(_._3)) }
    }
  }

  def get(key: String): Option[CleanupIterator[String]] = {
    IteratorUtil.zipNext(files.toIterator).find { case ((_, first), next) => first <= key && (next.isEmpty || next.get._2 > key) }.map(_._1)
  }.map(_._1).flatMap { files =>
    iterFiles(files, Some(key)).chainOpt { iter => iter.dropWhile(_._1 < key).buffered.headOption.filter(_._1 == key).map(_._2) }
  }

  def iter: CleanupIterator[(String, Iterator[String])] = iterFiles(files.flatMap(_._1))

  def from(key: String, lookupPrefixes: Boolean = false): CleanupIterator[(String, Iterator[String])] = {
    val relevantFiles = IteratorUtil.dropButLast(files.toIterator.buffered) { case (_, first) => first < key && (!lookupPrefixes || !key.startsWith(first)) }.toSeq
    iterFiles(relevantFiles.flatMap(_._1), Some(key)).chain { iter => iter.dropWhile { case (k, _) => k < key && (!lookupPrefixes || !key.startsWith(k)) } }
  }

  def before(key: String): CleanupIterator[(String, Iterator[String])] = {
    val relevantFiles = IteratorUtil.dropButLast(files.toIterator.buffered) { case (_, first) => first < key }.toSeq
    iterFiles(relevantFiles.flatMap(_._1), Some(key)).chain { iter => IteratorUtil.dropButLast(iter.chain(_.map { case (k, v) => (k, v.toArray.toIterator) })) { case (k, _) => k < key } }
  }

  def fromWithIndex(key: String, lookupPrefixes: Boolean = false): CleanupIterator[((String, Iterator[String]), Int)] = {
    iterFiles(files.flatMap(_._1)).chain(_.zipWithIndex.dropWhile { case ((k, _), _) => k < key && (!lookupPrefixes || !key.startsWith(k)) })
  }

  def dropKeys(n: Int): CleanupIterator[(String, Iterator[String])] = iterFiles(files.flatMap(_._1))
}