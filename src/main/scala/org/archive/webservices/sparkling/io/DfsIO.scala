package org.archive.webservices.sparkling.io

import org.archive.webservices.sparkling.AccessContext
import org.archive.webservices.sparkling.logging.LogContext
import org.archive.webservices.sparkling.util.{CleanupIterator, Common, IteratorUtil}

import java.io.{FileSystem => _, _}
import scala.util.{Random, Try}

object DfsIO {
  type LoadingStrategy = LoadingStrategy.Value
  val DefaultLoadingStrategy: LoadingStrategy.Value = LoadingStrategy.Remote

  object LoadingStrategy extends Enumeration {
    val Remote, BlockWise, CopyLocal, Dynamic = Value
  }

  val DefaultLineBuffer = 1000

  implicit def toInstance(self: this.type)(implicit context: AccessContext = AccessContext.default): DfsIO = {
    context.dfsIO
  }
}

trait DfsIO extends Serializable {
  import org.archive.webservices.sparkling.Sparkling._

  @transient implicit lazy val logContext: LogContext = LogContext(this)

  def supportsHadoopFileAPI: Boolean = false

  def getBlockSize(path: String): Long

  def open(
      path: String,
      offset: Long = 0,
      length: Long = 0,
      decompress: Boolean = true,
      retries: Int = 10,
      sleepMillis: Int = 1000,
      strategy: DfsIO.LoadingStrategy = DfsIO.DefaultLoadingStrategy
  ): InputStream

  def access[R](
      path: String,
      offset: Long = 0,
      length: Long = 0,
      decompress: Boolean = true,
      retries: Int = 60,
      sleepMillis: Int = 1000 * 60,
      strategy: DfsIO.LoadingStrategy = DfsIO.DefaultLoadingStrategy
  )(action: InputStream => R): R = {
    val in = open(path, offset, length, decompress, retries, sleepMillis, strategy)
    try {
      action(in)
    } finally {
      Try(in.close())
    }
  }

  def copyFromLocal(src: String, dst: String, move: Boolean = false, overwrite: Boolean = false, replication: Short = 0): Unit

  def copyToLocal(src: String, dst: String): Unit = {
    val out = IOUtil.fileOut(dst)
    try {
      access(src)(IOUtil.copy(_, out))
    } finally {
      out.close()
    }
  }

  def rename(src: String, dst: String): Unit

  def length(path: String): Long

  def lines(path: String, n: Int = -1, offset: Long = 0): Seq[String] = access(path, offset) { in =>
    val lines = IOUtil.lines(in)
    if (n < 0) lines.toList else lines.take(n).toList
  }

  def files(path: String, recursive: Boolean = true): Iterator[String]

  def dir(path: String): String

  def mkdirs(path: String): Boolean

  def deleteOnExit(path: String): Unit

  def createTmpPath(prefix: String = tmpFilePrefix, path: String = tmpHdfsPath, deleteOnExit: Boolean = true): String = {
    var rnd = System.currentTimeMillis + "-" + Random.nextInt.abs
    var tmpPath: String = null
    while ({
      tmpPath = IOUtil.concatPaths(path, prefix + rnd)
      exists(tmpPath) || mkdirs(tmpPath)
    }) rnd = System.currentTimeMillis + "-" + Random.nextInt.abs
    if (deleteOnExit) this.deleteOnExit(tmpPath)
    tmpPath
  }

  def tmpPath[R](action: String => R): R = {
    val path = createTmpPath()
    try {
      action(path)
    } finally {
      delete(path)
    }
  }

  def delete(path: String): Unit

  def exists(path: String): Boolean

  def ensureOutDir(path: String, ensureNew: Boolean = true): Unit = {
    if (ensureNew && exists(path)) Common.printThrow("Path exists: " + path)
    mkdirs(path)
  }

  def ensureNewFile(path: String): Unit = { if (exists(path)) Common.printThrow("File exists: " + path) }

  def out(path: String, overwrite: Boolean = false, compress: Boolean = true, useWriter: Boolean = true, append: Boolean = false, temporary: Boolean = false): OutputStream

  def writeLines(path: String, lines: => TraversableOnce[String], overwrite: Boolean = false, compress: Boolean = true, useWriter: Boolean = true, skipIfExists: Boolean = false): Long = {
    if (skipIfExists && exists(path)) return 0L
    val stream = out(path, overwrite, compress, useWriter)
    val processed = IOUtil.writeLines(stream, lines)
    Try(stream.close())
    processed
  }

  def concat(files: Seq[String], outPath: String, append: Boolean = false): Unit = {
    val stream = out(outPath, compress = false, append = append)
    for (file <- files) access(file, decompress = false) { in => IOUtil.copy(in, stream) }
    Try(stream.close())
  }

  def iterLines(path: String, readFully: Boolean = false): CleanupIterator[String] = CleanupIterator.flatten {
    files(path).map { file =>
      val in = if (readFully) open(file, length = -1) else open(file)
      IteratorUtil.cleanup(IOUtil.lines(in), in.close)
    }
  }

  def countLines(path: String, copyLocal: Boolean = false): Long = IteratorUtil.count(iterLines(path, readFully = copyLocal))

  def collectLines(path: String): Seq[String] = files(path).toSeq.par.flatMap { file =>
    val in = open(file)
    IteratorUtil.cleanup(IOUtil.lines(in), in.close)
  }.seq

  def collectDistinctLines(path: String, parallel: Boolean = true, map: String => Option[String] = Some(_), lineBuffer: Int = DfsIO.DefaultLineBuffer): Set[String] =
    if (parallel) {
      val parallel = files(path).toSet.par
      parallel.flatMap { file =>
        val in = open(file)
        IteratorUtil.cleanup(IOUtil.lines(in), in.close).flatMap(l => map(l)).grouped(lineBuffer).map(_.toSet).foldLeft(Set.empty[String])(_ ++ _)
      }.seq
    } else iterLines(path).flatMap(l => map(l)).grouped(lineBuffer).map(_.toSet).foldLeft(Set.empty[String])(_ ++ _)

  def touch(path: String): Unit = Common.touch(out(path, useWriter = false, append = exists(path), compress = false))(_.write(Array.empty[Byte])).close()
}
