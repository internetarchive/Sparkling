package org.archive.webservices.sparkling.util

class PrefixMap[K, V] private (prefixes: Seq[(Seq[K], V)]) extends Serializable {
  private val items = prefixes.groupBy { case (p, v) => p.head }.map { case (next, continue) =>
    val remaining = continue.map { case (p, v) => (p.drop(1), v) }
    next ->
      (remaining.find { case (p, _) => p.isEmpty } match {
        case Some((_, v)) => Right(v)
        case None         => Left(new PrefixMap(remaining))
      })
  }

  def findPrefix(key: Seq[K]): Option[(Seq[K], V)] = {
    if (key.isEmpty || !items.contains(key.head)) None
    else items(key.head) match {
      case Left(continue) => continue.findPrefix(key.drop(1)).map { case (p, v) => (Seq(key.head) ++ p, v) }
      case Right(value)   => Some((Seq(key.head), value))
    }
  }
}

object PrefixMap {
  def apply[K, V](prefixes: Map[Seq[K], V]): PrefixMap[K, V] = new PrefixMap[K, V](prefixes.toSeq)
}
