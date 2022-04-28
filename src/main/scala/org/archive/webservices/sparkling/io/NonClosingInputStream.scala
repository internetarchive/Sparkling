package org.archive.webservices.sparkling.io

import java.io.InputStream

class NonClosingInputStream(in: InputStream) extends InputStream {
  override def read(): Int = in.read()
  override def read(b: Array[Byte]): Int = in.read(b)
  override def read(b: Array[Byte], off: Int, len: Int): Int = in.read(b, off, len)
  override def skip(n: Long): Long = in.skip(n)
  override def available(): Int = in.available()
  override def close(): Unit = {}
  override def mark(readlimit: Int): Unit = in.mark(readlimit)
  override def reset(): Unit = in.reset()
  override def markSupported(): Boolean = in.markSupported()
}
