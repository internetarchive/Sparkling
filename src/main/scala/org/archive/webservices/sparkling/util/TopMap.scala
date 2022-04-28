package org.archive.webservices.sparkling.util

class TopMap[K, V: Ordering] private (min: Int, max: Int, elems: Seq[(K, V)]) extends Serializable {
  private val values = collection.mutable.Map[K, V](elems: _*)

  def sorted: Seq[(K, V)] = values.toList.sortBy({ case (k, v) => v })(implicitly[Ordering[V]].reverse)

  def top(n: Int): Seq[(K, V)] = sorted.take(n)

  def top: Option[(K, V)] = sorted.headOption

  private def ensureCapacity(): Unit = if (values.size >= max) {
    val retain = sorted.map { case (k, v) => k }.take(min).toSet
    values.retain { case (k, v) => retain.contains(k) }
  }

  def set(key: K, value: V): V = {
    ensureCapacity()
    values(key) = value
    value
  }

  def set(key: K)(value: Option[V] => V): V = set(key, value(values.get(key)))

  def getOrElseUpdate(key: K, value: => V): V = {
    values.getOrElseUpdate(
      key, {
        ensureCapacity()
        value
      }
    )
  }

  def remove(key: K): Option[V] = values.remove(key)

  def get(key: K): Option[V] = values.get(key)

  def get: Map[K, V] = values.toMap

  ensureCapacity()
}

object TopMap {
  val DefaultMaxFactor = 10

  def apply[K, V: Ordering](min: Int): TopMap[K, V] = new TopMap(min, min * DefaultMaxFactor, Seq.empty)
  def apply[K, V: Ordering](min: Int)(elems: (K, V)*): TopMap[K, V] = new TopMap(min, min * DefaultMaxFactor, elems)
  def apply[K, V: Ordering](map: Map[K, V], min: Int): TopMap[K, V] = new TopMap(min, min * DefaultMaxFactor, map.toSeq)

  def apply[K, V: Ordering](min: Int, max: Int): TopMap[K, V] = new TopMap(min, max, Seq.empty)
  def apply[K, V: Ordering](min: Int, max: Int)(elems: (K, V)*): TopMap[K, V] = new TopMap(min, max, elems)
  def apply[K, V: Ordering](map: Map[K, V], min: Int, max: Int): TopMap[K, V] = new TopMap(min, max, map.toSeq)
}
