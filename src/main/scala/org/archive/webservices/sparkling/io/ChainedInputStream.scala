package org.archive.webservices.sparkling.io

import java.io.InputStream
import scala.annotation.tailrec
import scala.util.Try

class ChainedInputStream(in: Iterator[InputStream], nextOnError: Boolean = false) extends InputStream {
  val buffered: BufferedIterator[InputStream] = in.buffered

  @tailrec private def next(eof: Int, action: InputStream => Int): Int = if (buffered.hasNext) {
    val r = if (nextOnError) Try(action(buffered.head)).getOrElse(eof) else action(buffered.head)
    if (r == eof) {
      Try(buffered.head.close())
      buffered.next()
      next(eof, action)
    } else r
  } else eof

  @tailrec private def nextLong(eof: Long, action: InputStream => Long): Long = if (buffered.hasNext) {
    val r = if (nextOnError) Try(action(buffered.head)).getOrElse(eof) else action(buffered.head)
    if (r == eof) {
      Try(buffered.head.close())
      buffered.next()
      nextLong(eof, action)
    } else r
  } else eof

  override def read(): Int = next(-1, _.read())
  override def read(b: Array[Byte]): Int = next(-1, _.read(b))
  override def read(b: Array[Byte], off: Int, len: Int): Int = next(-1, _.read(b, off, len))
  override def skip(n: Long): Long = nextLong(0, _.skip(n))
  override def available(): Int = next(0, _.available)
  override def close(): Unit = for (s <- buffered) s.close()
  override def markSupported(): Boolean = false
}
