package org.archive.webservices.sparkling.io

import java.io.InputStream

class ForkedInputStream(val forker: InputStreamForker) extends InputStream {
  private var _pos: Long = forker.pos
  def pos: Long = _pos

  private var _eof: Boolean = false
  def eof: Boolean = _eof

  private var _closed: Boolean = false
  def closed: Boolean = _closed

  override def read(): Int = throw new RuntimeException("This stream should always be wrapped by a BufferedInputStream, which doesn't use this method")

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    if (eof) -1
    else {
      val read = forker.read(this, b, off, len)
      if (read == -1) {
        _eof = true
        close()
      } else _pos += read
      read
    }
  }

  override def skip(n: Long): Long = super.skip(n)

  override def available(): Int = super.available()

  override def close(): Unit = if (!_closed) {
    forker.close(this)
    _closed = true
  }

  override def markSupported(): Boolean = false
}
