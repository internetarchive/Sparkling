package org.archive.webservices.sparkling.compression

import java.io.{ByteArrayInputStream, InputStream}

import com.github.luben.zstd.shaded.ZstdInputStream
import org.archive.webservices.sparkling._
import org.archive.webservices.sparkling.io.IOUtil
import org.archive.webservices.sparkling.util.{BytesUtil, CacheMap}

class Zstd extends Decompressor {
  import Zstd._

  var dict: Option[Array[Byte]] = None
  def setDict(d: Array[Byte]): Unit = dict = Some(d)
  def clearDict(): Unit = dict = None

  def ifInit(filename: String, close: Boolean = true)(in: => InputStream): Option[InputStream] = {
    if (isCompressed(filename)) {
      var initialized: Option[InputStream] = None
      dict = Some(dictionaryCache.getOrElse(filename, {
        initialized = Some(init(in))
        dict.getOrElse(Array.empty)
      })).filter(_.nonEmpty)
      if (close) {
        for (s <- initialized) s.close()
        None
      } else initialized
    } else None
  }

  def init(in: InputStream): InputStream = {
    val buffered = IOUtil.supportMark(in)
    if (!IOUtil.eof(buffered)) {
      val buffer = Array.ofDim[Byte](4)
      buffered.mark(4)
      if (IOUtil.read(buffered, buffer)) {
        if (buffer.sameElements(FileMagic)) {
          buffered.mark(4)
          if (IOUtil.read(buffered, buffer)) {
            if (buffer.sameElements(RecordMagic)) {
              buffered.reset()
            } else {
              val dictLength = BytesUtil.littleEndianInt(buffer)
              if (dictLength <= maxDictSize) {
                val dictBuffer = Array.ofDim[Byte](dictLength)
                if (IOUtil.read(buffered, dictBuffer)) {
                  val dictIn = new ZstdInputStream(new ByteArrayInputStream(dictBuffer))
                  val dict = try {
                    IOUtil.bytes(dictIn)
                  } finally {
                    dictIn.close()
                  }
                  if (dict.take(4).sameElements(DictMagic)) setDict(dict)
                }
              }
            }
          }
        } else if (buffer.sameElements(RecordMagic)) {
          buffered.reset()
        }
      }
    }
    buffered
  }

  def decompress(in: InputStream, filename: Option[String] = None, checkFile: Boolean = false): InputStream = {
    if ((filename.isEmpty && !checkFile) || (filename.isDefined && Zstd.isCompressed(filename.get))) {
      val initialized = filename.flatMap(ifInit(_, close = false)(in)).getOrElse(init(in))
      val zstd = new ZstdInputStream(initialized)
      for (d <- dict) zstd.setDict(d)
      zstd
    } else in
  }
}

object Zstd extends Zstd {
  import Sparkling._

  var maxDictSize: Long = prop(100.mb)(maxDictSize, maxDictSize = _)
  var minDictionaryCacheSize: Long = prop(50.mb)(minDictionaryCacheSize, minDictionaryCacheSize = _)
  var maxDictionaryCacheSize: Long = prop(100.mb)(maxDictionaryCacheSize, maxDictionaryCacheSize = _)
  lazy val dictionaryCache: CacheMap[String, Array[Byte]] = CacheMap[String, Array[Byte]](minDictionaryCacheSize, maxDictionaryCacheSize)((_, bytes) => bytes.length)

  val FileMagic: Array[Byte] = Array(93, 42, 77, 24)
  val RecordMagic: Array[Byte] = Array(40, -75, 47, -3)
  val DictMagic: Array[Byte] = Array(55, -92, 48, -20)

  def isCompressed(filename: String): Boolean = filename.toLowerCase.endsWith(ZstdExt)

  def isCompressed(in: InputStream): Boolean = {
    in.mark(4)
    try {
      val buffer = Array.ofDim[Byte](4)
      if (IOUtil.read(in, buffer)) {
        buffer.sameElements(FileMagic) || buffer.sameElements(RecordMagic)
      } else false
    } finally {
      in.reset()
    }
  }

  def initContext(close: Boolean = true)(in: => InputStream)(implicit context: DecompressionContext): Option[InputStream] = {
    context.zstd { zstd =>
      val initialized = for (f <- context.filename) yield zstd.ifInit(f, close = close)(in)
      initialized.getOrElse {
        val initialized = zstd.init(in)
        if (close) initialized.close()
        Some(initialized)
      }
    }.flatten
  }
}
