package org.archive.webservices.sparkling.io

import java.io.InputStream

import org.archive.webservices.sparkling.util.Common

class CleanupInputStream(in: InputStream, cleanup: () => Unit) extends LookAheadInputStream(in) {
  private var closed = false

  def closeIfEof[R](read: => R, eof: R): R = {
    if (closed) return eof
    val r = read
    if (r == eof && super.eof) close()
    r
  }

  override def read(): Int = closeIfEof(super.read(), eof = -1)
  override def read(b: Array[Byte]): Int = closeIfEof(super.read(b), eof = -1)
  override def read(b: Array[Byte], off: Int, len: Int): Int = closeIfEof(super.read(b, off, len), eof = -1)
  override def skip(n: Long): Long = closeIfEof(super.skip(n), 0)
  override def available(): Int = closeIfEof(super.available(), 0)
  override def close(): Unit = if (!closed) {
    closed = true
    Common.cleanup(super.close())(cleanup)
  }
  override def markSupported(): Boolean = false
}
