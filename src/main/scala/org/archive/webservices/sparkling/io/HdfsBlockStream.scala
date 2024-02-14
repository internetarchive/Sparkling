package org.archive.webservices.sparkling.io

import java.io.{ByteArrayInputStream, InputStream}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.archive.webservices.sparkling.logging.LogContext
import org.archive.webservices.sparkling.util.Common

import scala.util.Try

class HdfsBlockStream(fs: FileSystem, file: String, offset: Long = 0, length: Long = -1, retries: Int = 60, sleepMillis: Int = 1000, blockReadTimeoutMillis: Int = -1) extends InputStream {
  implicit val logContext: LogContext = LogContext(this)

  val path = new Path(file)
  val (blockSize: Int, fileSize: Long) = {
    val status = fs.getFileStatus(path)
    (status.getBlockSize.min(Int.MaxValue).toInt, status.getLen)
  }

  private val PeekBlockSize = 1024 * 1024 // 1MB

  private var peek: Boolean = true
  private var pos: Long = offset.max(0)
  private val max: Long = if (length > 0) fileSize.min(pos + length) else fileSize

  private var buffer = new Array[Byte](blockSize)
  private val emptyBlock = new ByteArrayInputStream(Array.emptyByteArray)
  private var block = emptyBlock

  def ensureNextBlock(): InputStream = {
    if (block.available == 0 && pos < max) {
      val end = pos + blockSize
      val nextBlockLength = ((end - (end % blockSize)).min(max) - pos).toInt
      val blockLength = if (peek) PeekBlockSize.min(nextBlockLength) else nextBlockLength
      peek = false
      var readLength = 0
      def readBlock(reporter: Common.ProcessReporter, in: InputStream): Boolean = {
        while (readLength < blockLength) {
          reporter.alive()
          val read = in.read(buffer, readLength, blockLength - readLength)
          readLength += read
          pos += read
        }
        true
      }
      Common.retryObj(fs.open(path, blockLength))(
        retries,
        sleepMillis,
        _.close,
        (_, retry, e) => { "File access failed (" + retry + "/" + retries + "): " + path + " (Offset: " + pos + ") - " + e.getClass.getSimpleName + Option(e.getMessage).map(_.trim).filter(_.nonEmpty).map(" - " + _).getOrElse("") }
      ) { (in, _) =>
        try {
          Common.timeoutWithReporter(blockReadTimeoutMillis) { reporter =>
            reporter.alive("Reading " + path + " (Offset: " + pos + ")")
            if (pos > 0) {
              if (Try {
                in.seek(pos)
                readBlock(reporter, in)
              }.isFailure) {
                val retryNewSource = Try {
                  in.seekToNewSource(pos) && readBlock(reporter, in)
                }.getOrElse(false)
                if (!retryNewSource) {
                  in.seek(pos)
                  readBlock(reporter, in)
                }
              }
            } else readBlock(reporter, in)
          }
        } finally {
          in.close()
        }
      }
      block = new ByteArrayInputStream(buffer, 0, readLength)
      System.gc()
    }
    block
  }

  override def read(): Int = ensureNextBlock().read()

  override def read(b: Array[Byte]): Int = ensureNextBlock().read(b)

  override def read(b: Array[Byte], off: Int, len: Int): Int = ensureNextBlock().read(b, off, len)

  override def skip(n: Long): Long = {
    val available = block.available
    if (n <= available) block.skip(n)
    else {
      block = emptyBlock
      val currentPos = pos - available
      val skip = n.min(max - currentPos)
      pos += skip - available
      skip
    }
  }

  override def available(): Int = block.available

  override def close(): Unit = {
    block = emptyBlock
    buffer = Array.emptyByteArray
    System.gc()
  }

  override def markSupported(): Boolean = false
}
