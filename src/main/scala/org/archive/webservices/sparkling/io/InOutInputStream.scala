package org.archive.webservices.sparkling.io

import java.io.{ByteArrayOutputStream, InputStream, OutputStream, PipedInputStream, PipedOutputStream}

import scala.util.Try

object InOutInputStream {
  val BufferSize: Int = 8192 // BufferedInputStream.DEFAULT_BUFFER_SIZE

  def apply(in: InputStream)(out: OutputStream => OutputStream): InOutInputStream = new InOutInputStream(in, out)
}

class InOutInputStream private (in: InputStream, out: OutputStream => OutputStream) extends InputStream {
  private val inBuffer = new Array[Byte](InOutInputStream.BufferSize)

  private val outBuffer = new ByteArrayOutputStream()
  private val outStream = out(outBuffer)

  private var eof = false
  private def ensure(length: Int): Int = {
    while (!eof && outBuffer.size < length) {
      val read = in.read(inBuffer)
      if (read == -1) eof = true else {
        outStream.write(inBuffer, 0, read)
        outStream.flush()
      }
    }
    val size = outBuffer.size
    if (eof && size == 0) -1 else size
  }

  override def read(): Int = {
    val read = ensure(1)
    if (read == -1) -1 else {
      val arr = outBuffer.toByteArray
      val r = arr.head
      outBuffer.reset()
      outBuffer.write(arr.slice(1, read - 1 + 1))
      r
    }
  }

  override def read(b: Array[Byte]): Int = {
    val read = ensure(b.length)
    if (read == -1) -1 else {
      val take = read.min(b.length)
      val arr = outBuffer.toByteArray
      arr.copyToArray(b, 0, take)
      outBuffer.reset()
      outBuffer.write(arr.slice(take, read))
      take
    }
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    val maxRead = (b.length - off).min(len)
    val read = ensure(maxRead)
    if (read == -1) -1 else {
      val take = read.min(maxRead)
      val arr = outBuffer.toByteArray
      arr.copyToArray(b, off, take)
      outBuffer.reset()
      outBuffer.write(arr.slice(take, read))
      take
    }
  }

  override def skip(n: Long): Long = {
    val skipInt = Int.MaxValue.toLong.min(n).toInt
    val read = ensure(skipInt)
    if (read == -1) 0 else {
      val skip = read.min(skipInt)
      val arr = outBuffer.toByteArray
      outBuffer.reset()
      outBuffer.write(arr.drop(skip))
      skip
    }
  }

  override def available(): Int = outBuffer.size()

  override def close(): Unit = {
    Try(in.close())
    Try(outBuffer.close())
    Try(outStream.close())
  }

  override def markSupported(): Boolean = false
}
