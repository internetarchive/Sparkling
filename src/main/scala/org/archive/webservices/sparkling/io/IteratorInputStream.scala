package org.archive.webservices.sparkling.io

import org.archive.webservices.sparkling.util.IteratorUtil

import java.io.InputStream

class IteratorInputStream[A](iter: Iterator[A], bytes: A => Seq[Byte]) extends InputStream {
  private var _available = 0
  private val byteIter = iter.flatMap { v =>
    val next = bytes(v)
    _available = next.size
    next
  }

  override def read(): Int = if (byteIter.hasNext) {
    _available -= 1
    byteIter.next
  } else -1

  override def read(b: Array[Byte]): Int = read(b, 0, b.length)
  override def read(b: Array[Byte], off: Int, len: Int): Int = if (byteIter.hasNext) {
    val buffer = byteIter.take(len).toArray
    buffer.copyToArray(b, off)
    val read = buffer.length
    _available -= read
    read
  } else -1

  override def skip(n: Long): Long = IteratorUtil.skip(byteIter, n)

  override def available(): Int = if (byteIter.hasNext) _available else 0
}
