package org.archive.webservices.sparkling.io

import java.io._

import com.google.common.io.FileBackedOutputStream
import org.apache.commons.io.IOUtils
import org.apache.commons.io.input.BoundedInputStream
import org.archive.webservices.sparkling._
import org.archive.webservices.sparkling.compression.Compression
import org.archive.webservices.sparkling.util._

import scala.util.Try

object IOUtil {
  import org.archive.webservices.sparkling.Sparkling._

  var memoryBuffer: Int = prop(100.mb.toInt)(memoryBuffer, memoryBuffer = _)

  val EmptyStream: InputStream = new ByteArrayInputStream(Array.empty)

  def concatPaths(paths: String*): String = {
    paths.map(_.trim).filter(_.nonEmpty).mkString("/").replaceAll("/+", "/")
  }

  def tmpFile: File = tmpFile()

  def tmpFile(prefix: String = tmpFilePrefix, ext: String = TmpExt, path: Option[String] = None, deleteOnExit: Boolean = true): File = {
    val file = path match {
      case Some(p) =>
        val dir = new File(p).getCanonicalFile
        dir.mkdirs()
        File.createTempFile(prefix, ext, dir)
      case None => File.createTempFile(prefix, ext)
    }
    if (deleteOnExit) file.deleteOnExit()
    file
  }

  def delete(path: String): Unit = delete(new File(path))

  def delete(file: File): Unit = {
    while (file.exists()) {
      val success = Common.tryCatch(file.delete()).getOrElse(false)
      if (!success) Thread.`yield`()
    }
  }

  def copy(in: InputStream, out: OutputStream, length: Long = -1): Unit = { if (length < 0) IOUtils.copy(in, out) else IOUtils.copy(new BoundedInputStream(in, length), out) }

  def copyToBuffer(in: InputStream, length: Long = -1, bufferSize: Int = memoryBuffer): ManagedVal[ValueSupplier[InputStream]] = buffer(bufferSize, lazyEval = false) { buffer =>
    copy(in, buffer, length)
  }

  def bytes(in: InputStream): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    copy(in, out)
    out.close()
    out.toByteArray
  }

  def buffer(lazyEval: Boolean)(write: OutputStream => Unit): ManagedVal[ValueSupplier[InputStream]] = { buffer(memoryBuffer, lazyEval)(write) }

  def buffer(bufferSize: Int, lazyEval: Boolean)(write: OutputStream => Unit): ManagedVal[ValueSupplier[InputStream]] = {
    lazy val streams = collection.mutable.Buffer.empty[InputStream]
    lazy val buffer = new FileBackedOutputStream(bufferSize, true)
    ManagedVal(
      {
        try { write(buffer) }
        finally { buffer.close() }
        ValueSupplier { Common.touch(buffer.asByteSource.openBufferedStream)(streams += _) }
      },
      { _ =>
        streams.foreach(_.close())
        buffer.reset()
      },
      lazyEval
    )
  }

  def buffer(write: OutputStream => Unit, file: Boolean = false, lazyEval: Boolean = false, bufferSize: Int = memoryBuffer): ManagedVal[ValueSupplier[InputStream]] = {
    if (file) fileBuffer(write, lazyEval) else buffer(bufferSize, lazyEval)(write)
  }

  def fileBuffer(write: OutputStream => Unit, lazyEval: Boolean = false): ManagedVal[ValueSupplier[InputStream]] = {
    lazy val streams = collection.mutable.Buffer.empty[InputStream]
    lazy val file = tmpFile
    ManagedVal(
      {
        val stream = fileOut(file)
        try { write(stream) }
        finally { stream.close() }
        ValueSupplier { Common.touch(new BufferedInputStream(new FileInputStream(file)))(streams += _) }
      },
      { _ =>
        streams.foreach(_.close())
        file.delete()
      },
      lazyEval
    )
  }

  def decompress(in: InputStream, filename: Option[String] = None, checkFile: Boolean = false): InputStream = Compression.decompress(in, filename, checkFile)

  def lines(file: String): CleanupIterator[String] = {
    val in = new BufferedInputStream(new FileInputStream(file))
    IteratorUtil.cleanup(lines(in, Some(file)), in.close)
  }

  def linesByLine(in: InputStream, filename: Option[String] = None, maxLineLength: Int = 1.mb.toInt): Iterator[String] = {
    val stream = IOUtil.decompress(in, filename, checkFile = true)
    IteratorUtil.whileDefined(Option(StringUtil.readLine(stream, DefaultCharset, maxLineLength)))
  }

  private val NewLineByte = '\n'.toByte
  def lines(in: InputStream, filename: Option[String] = None, maxLineLength: Int = 1.mb.toInt, dropLastEmpty: Boolean = true): Iterator[String] = {
    val stream = IOUtil.decompress(in, filename, checkFile = true)
    val buffer = Array.ofDim[Byte](64.kb.toInt)
    var bufferLength = 0
    var bufferPosn = 0
    var eof = false
    val lines = IteratorUtil.whileDefined { // see org.apache.hadoop.util.LineReader#readDefaultLine
      if (eof) None else Some {
        var lineLength = 0
        var eol = false
        val bytes = IteratorUtil.whileDefined {
          if (!eol && !eof) Some {
            var startPosn = bufferPosn
            if (bufferPosn >= bufferLength) {
              startPosn = 0
              bufferPosn = 0
              bufferLength = stream.read(buffer)
              if (bufferLength < 0) eof = true
            }
            if (!eof) {
              while (bufferPosn < bufferLength && {
                if (buffer(bufferPosn) == NewLineByte) {
                  eol = true
                  bufferPosn += 1
                  false
                } else true
              }) bufferPosn += 1
              val readLength = bufferPosn - startPosn
              val maxLength = readLength - (if (eol) 1 else 0)
              val appendLength = if (maxLineLength < 0) maxLength else maxLength.min(maxLineLength - lineLength)
              if (appendLength > 0) {
                lineLength += appendLength
                buffer.slice(startPosn, startPosn + appendLength)
              } else Array.empty[Byte]
            } else Array.empty[Byte]
          } else None
        }.toArray.flatten
        StringUtil.fromBytes(bytes, charset = DefaultCharset)
      }
    }.map(_.stripSuffix("\r"))
    if (dropLastEmpty) IteratorUtil.dropLastIf(lines)(_.isEmpty) else lines
  }

  def writeLines(file: String, lines: TraversableOnce[String]): Long = {
    val out = IOUtil.fileOut(file)
    val processed = writeLines(out, lines)
    out.close()
    processed
  }

  def writeLines(out: OutputStream, lines: TraversableOnce[String]): Long = {
    val stream = print(out)
    val processed = lines.map { line =>
      stream.println(line)
      1L
    }.sum
    stream.flush()
    processed
  }

  def eof(in: InputStream, markReset: Boolean = true): Boolean = {
    if (markReset) in.mark(1)
    val eof =
      try { in.read() == -1 }
      catch { case _: EOFException => true }
    if (markReset && !eof) in.reset()
    eof
  }

  def read(in: InputStream, bytes: Array[Byte]): Boolean = {
    var read = 0
    val length = bytes.length
    while ({
      val readNow = in.read(bytes, read, length - read)
      if (readNow == -1) return false
      read += readNow
      read < length
    }) { /* do nothing */ }
    true
  }

  def readToEnd(in: InputStream, close: Boolean = true): Unit = Try {
    while (in.read() != -1) in.skip(Long.MaxValue)
    if (close) in.close()
  }

  def skip(in: InputStream, bytes: Long): Long = {
    var skipped = 0L
    if (bytes > 0) {
      var prevSkip = 1L
      while (
        skipped < bytes && {
          val thisSkip =
            try { in.skip(bytes - skipped) }
            catch { case e: EOFException => -1 }
          if (thisSkip == -1 || (prevSkip == 0 && thisSkip == 0)) false
          else {
            skipped += thisSkip
            prevSkip = thisSkip
            true
          }
        }
      ) {}
    }
    skipped
  }

  def splitStream(in: InputStream, positions: TraversableOnce[(Long, Long)]): Iterator[InputStream] = {
    var currentStream: Option[BoundedInputStream] = None
    var position = 0L
    positions.map { case (offset, length) =>
      if (currentStream.isDefined) IOUtil.readToEnd(currentStream.get)

      IOUtil.skip(in, offset - position)
      position = offset + length

      currentStream = Some({
        val bounded = new BoundedInputStream(in, length)
        bounded.setPropagateClose(false)
        bounded
      })
      currentStream.get
    }.toIterator
  }

  def print(out: OutputStream, autoFlush: Boolean = false, closing: Boolean = true): PrintStream = new PrintStream(if (closing) out else new NonClosingOutputStream(out), autoFlush, DefaultCharset)

  def supportMark(stream: InputStream): InputStream = if (stream.markSupported()) stream else new BufferedInputStream(stream)

  def fileOut(path: String, append: Boolean = false): OutputStream = new BufferedOutputStream(new FileOutputStream(path, append))
  def fileOut(file: File): OutputStream = new BufferedOutputStream(new FileOutputStream(file, false))
  def fileOut(file: File, append: Boolean): OutputStream = new BufferedOutputStream(new FileOutputStream(file, append))
}
