package org.archive.webservices.sparkling.io

import java.io.{IOException, InputStream}

class InputStreamClosewall(val in: InputStream) extends InputStream {
  private var closed = false
  private def ifOpen[A](action: => A): A = if (closed) throw new IOException("Stream closed") else action

  override def read(): Int = ifOpen(in.read())
  override def read(b: Array[Byte]): Int = ifOpen(in.read(b))
  override def read(b: Array[Byte], off: Int, len: Int): Int = ifOpen(in.read(b, off, len))
  override def skip(n: Long): Long = ifOpen(in.skip(n))
  override def available(): Int = ifOpen(in.available())
  override def close(): Unit = closed = true
  override def mark(readlimit: Int): Unit = ifOpen(in.mark(readlimit))
  override def reset(): Unit = ifOpen(in.reset())
  override def markSupported(): Boolean = ifOpen(in.markSupported())
}
