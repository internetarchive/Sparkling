package org.archive.webservices.sparkling.io

import java.io.InputStream

import scala.util.Try

class CatchingInputStream(in: InputStream) extends InputStream {
  private var eof = false

  private def tryOrEof[R](action: => R, orElse: => R): R = {
    if (eof) orElse else {
      try {
        action
      } catch {
        case e: Exception =>
          e.printStackTrace()
          Try(in.close())
          eof = true
          orElse
      }
    }
  }

  override def read(): Int = tryOrEof(in.read(), -1)
  override def read(b: Array[Byte]): Int = tryOrEof(in.read(b), -1)
  override def read(b: Array[Byte], off: Int, len: Int): Int = tryOrEof(in.read(b, off, len), -1)
  override def skip(n: Long): Long = tryOrEof(in.skip(n), 0)
  override def available(): Int = tryOrEof(in.available(), 0)
  override def close(): Unit = tryOrEof(in.close(), Unit)
  override def mark(readlimit: Int): Unit = tryOrEof(in.mark(readlimit), Unit)
  override def reset(): Unit = tryOrEof(in.reset(), Unit)
  override def markSupported(): Boolean = in.markSupported()
}
