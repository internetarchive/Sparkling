package org.archive.webservices.sparkling.util

import scala.collection.mutable

class MultiBufferedIterator[A](iter: Iterator[A]) extends BufferedIterator[A] {
  val buffer: mutable.Queue[A] = mutable.Queue.empty[A]

  private def nextIter: A = {
    val next = iter.next
    buffer.enqueue(next)
    next
  }

  private class AheadIterator extends Iterator[A] {
    private val ahead = buffer.toIterator
    override def hasNext: Boolean = ahead.hasNext || iter.hasNext
    override def next(): A = if (ahead.hasNext) ahead.next else nextIter
  }

  def ahead: Iterator[A] = new AheadIterator()
  override def head: A = if (buffer.nonEmpty) buffer.head else nextIter
  override def hasNext: Boolean = buffer.nonEmpty || iter.hasNext
  override def next(): A = if (buffer.nonEmpty) buffer.dequeue else iter.next
}

object MultiBufferedIterator {
  def apply[A](iter: Iterator[A]) = new MultiBufferedIterator[A](iter)
}