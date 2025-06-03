package org.archive.webservices.sparkling.io

import java.io.InputStream

class LookAheadInputStream(in: InputStream) extends InputStream {
  private var _head: Int = -2

  def head: Int = {
    if (_head == -2) _head = in.read()
    _head
  }

  def eof: Boolean = head == -1

  private def resetHead(): Int = {
    val h = _head
    if (h != -1) _head = -2
    h
  }

  private def ifHead[R](action: Int => R, orElse: => R, reset: Boolean = true, eof: R, detectEof: Boolean): R = {
    if (_head == -1) return eof
    val r = if (_head == -2) orElse else action(if (reset) resetHead() else _head)
    if (detectEof && r == eof) _head = -1
    r
  }

  override def read(): Int = ifHead(identity, in.read(), eof = -1, detectEof = true)

  override def read(b: Array[Byte]): Int = ifHead({ head =>
    b(0) = head.toByte
    in.read(b, 1, b.length - 1) + 1
  }, in.read(b), eof = -1, detectEof = true)

  override def read(b: Array[Byte], off: Int, len: Int): Int = ifHead({ head =>
    if (len == 0) 0 else {
      b(off) = head.toByte
      in.read(b, off + 1, len - 1) + 1
    }
  }, in.read(b, off, len), eof = -1, detectEof = true)

  override def skip(n: Long): Long = ifHead({ _ =>
    if (n <= 1) n else in.skip(n - 1)
  }, in.skip(n), reset = n > 0, eof = 0, detectEof = false)

  override def available(): Int = ifHead({ _ =>
    in.available() + 1
  }, in.available(), reset = false, eof = 0, detectEof = false)

  override def close(): Unit = {
    _head = -1
    in.close()
  }
  override def markSupported(): Boolean = false
}
