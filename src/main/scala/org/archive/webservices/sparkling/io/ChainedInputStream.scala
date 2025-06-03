package org.archive.webservices.sparkling.io

import java.io.InputStream
import scala.annotation.tailrec
import scala.util.Try

class ChainedInputStream(in: Iterator[InputStream], nextOnError: Boolean = false) extends InputStream {
  val buffered: BufferedIterator[LookAheadInputStream] = in.map(new LookAheadInputStream(_)).buffered

  @tailrec private def next[A](eof: A, action: InputStream => A): A = if (buffered.hasNext) {
    val r = if (nextOnError) Try(action(buffered.head)).getOrElse(eof) else action(buffered.head)
    val isEof = r == eof && (if (nextOnError) Try(buffered.head.eof).getOrElse(true) else buffered.head.eof)
    if (isEof) {
      Try(buffered.head.close())
      buffered.next()
      next(eof, action)
    } else r
  } else eof

  override def read(): Int = next(-1, _.read())
  override def read(b: Array[Byte]): Int = next(-1, _.read(b))
  override def read(b: Array[Byte], off: Int, len: Int): Int = next(-1, _.read(b, off, len))
  override def skip(n: Long): Long = next(0, _.skip(n))
  override def available(): Int = next(0, _.available)
  override def close(): Unit = for (s <- buffered) s.close()
  override def markSupported(): Boolean = false
}
