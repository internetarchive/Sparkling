package org.archive.webservices.sparkling.io

import java.io.{BufferedInputStream, FileInputStream, InputStream}

class LazyBufferedFileInputStream(path: String) extends InputStream {
  private var open = false;
  private lazy val in = {
    open = true
    new BufferedInputStream(new FileInputStream(path))
  }

  override def read(): Int = in.read()
  override def read(b: Array[Byte]): Int = in.read(b)
  override def read(b: Array[Byte], off: Int, len: Int): Int = in.read(b, off, len)
  override def skip(n: Long): Long = in.skip(n)
  override def available(): Int = in.available()
  override def close(): Unit = if (open) in.close()
  override def markSupported(): Boolean = false
}