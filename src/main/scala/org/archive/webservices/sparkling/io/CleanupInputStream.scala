package org.archive.webservices.sparkling.io

import java.io.InputStream

import org.archive.webservices.sparkling.util.Common

class CleanupInputStream(in: InputStream, cleanup: () => Unit) extends InputStream {
  def closeIfEof(read: Int, eof: Int = -1): Int = {
    if (read == eof) close()
    read
  }
  override def read(): Int = closeIfEof(in.read())
  override def read(b: Array[Byte]): Int = closeIfEof(in.read(b))
  override def read(b: Array[Byte], off: Int, len: Int): Int = closeIfEof(in.read(b, off, len))
  override def skip(n: Long): Long = in.skip(n)
  override def available(): Int = closeIfEof(in.available(), 0)
  override def close(): Unit = Common.cleanup(in.close())(cleanup)
  override def mark(readlimit: Int): Unit = in.mark(readlimit)
  override def reset(): Unit = in.reset()
  override def markSupported(): Boolean = in.markSupported()
}
