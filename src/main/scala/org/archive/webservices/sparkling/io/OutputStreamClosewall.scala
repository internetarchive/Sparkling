package org.archive.webservices.sparkling.io

import java.io.{IOException, OutputStream}

class OutputStreamClosewall(val out: OutputStream) extends OutputStream {
  private var closed = false
  private def ifOpen[A](action: => A): A = if (closed) throw new IOException("Stream closed") else action

  override def write(b: Int): Unit = ifOpen(out.write(b))
  override def write(b: Array[Byte]): Unit = ifOpen(out.write(b))
  override def write(b: Array[Byte], off: Int, len: Int): Unit = ifOpen(out.write(b, off, len))
  override def flush(): Unit = ifOpen(out.flush())
  override def close(): Unit = closed = true
}
