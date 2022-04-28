package org.archive.webservices.sparkling.io

import java.io.{BufferedInputStream, InputStream}

class InputStreamForker private (in: InputStream) {
  private val buffer = new Array[Byte](InputStreamForker.BufferSize)
  private var buffered = 0
  private var _pos: Long = 0

  private var forks: Int = 0

  private var forksRead: Int = 0

  def eof: Boolean = buffered == -1
  def pos: Long = _pos

  def fork: InputStream = synchronized {
    forks += 1
    forksRead += 1
    new BufferedInputStream(new ForkedInputStream(this))
  }

  def fork(n: Int): Array[InputStream] = (0 until n).map(_ => fork).toArray

  private[io] def close(fork: ForkedInputStream): Unit = if (fork.forker == this && !fork.closed) synchronized {
    if (fork.pos == _pos && fork.eof == eof) forksRead -= 1
    forks -= 1
    if (forks == 0) in.close()
  }

  def read(fork: ForkedInputStream, b: Array[Byte], off: Int, len: Int): Int =
    if (fork.forker == this) {
      while (!eof && fork.pos == _pos && forksRead < forks) Thread.`yield`()
      synchronized {
        if (fork.pos < _pos) {
          val offset = (fork.pos - (_pos - buffered)).toInt
          val maxRead = _pos - fork.pos
          val read = maxRead.toInt.min(b.length).min(len)
          if (read == maxRead) forksRead += 1
          if (read > 0) Array.copy(buffer, offset, b, off, read)
          read
        } else {
          if (eof) {
            if (!fork.eof) forksRead += 1
            -1
          } else {
            buffered = in.read(buffer)
            if (buffered == -1) {
              forksRead = 1
              -1
            } else {
              _pos += buffered
              val read = buffered.min(b.length).min(len)
              forksRead = if (read == buffered) 1 else 0
              if (read > 0) Array.copy(buffer, 0, b, off, read)
              read
            }
          }
        }
      }
    } else 0
}

object InputStreamForker {
  val BufferSize: Int = 8192 // BufferedInputStream.DEFAULT_BUFFER_SIZE

  def apply(in: InputStream): InputStreamForker = new InputStreamForker(in)
}
