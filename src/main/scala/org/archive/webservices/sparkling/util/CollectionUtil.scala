package org.archive.webservices.sparkling.util

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable

object CollectionUtil {
  def combineMaps[K, V](maps: Map[K, V]*)(combine: Iterator[V] => Option[V]): Map[K, V] = {
    maps.toIterator.map(_.keySet).reduce(_ ++ _).toIterator.flatMap { key => combine(maps.toIterator.flatMap(_.get(key))).map(key -> _) }.toMap
  }

  def concurrentMap[K, V]: mutable.Map[K, V] = new ConcurrentHashMap[K, V]().asScala
}
