package org.archive.webservices.sparkling.io

import org.apache.commons.io.input.BoundedInputStream
import org.apache.hadoop.fs._
import org.apache.spark.deploy.SparkHadoopUtil
import org.archive.webservices.sparkling.AccessContext
import org.archive.webservices.sparkling.logging.{Log, LogContext}
import org.archive.webservices.sparkling.util.{CleanupIterator, CollectionUtil, Common, IteratorUtil}

import java.io.{FileSystem => _, _}
import java.net.URI
import java.util.zip.GZIPOutputStream
import scala.util.{Random, Try}

object HdfsIO {
  import org.archive.webservices.sparkling.Sparkling._

  type LoadingStrategy = LoadingStrategy.Value

  object LoadingStrategy extends Enumeration {
    val Remote, BlockWise, CopyLocal, Dynamic = Value
  }

  var defaultLoadingStrategy: LoadingStrategy = prop(LoadingStrategy.Dynamic)(defaultLoadingStrategy, defaultLoadingStrategy = _)
  var dynamicCopyLocalThreshold: Double = prop(0.5)(dynamicCopyLocalThreshold, dynamicCopyLocalThreshold = _)
  var blockReadTimeoutMillis: Int = prop(1000 * 60 * 5)(blockReadTimeoutMillis, blockReadTimeoutMillis = _) // 5 minutes

  val DefaultLineBuffer = 1000
  val ReplicationProperty = "dfs.replication"
  val BufferSizeProperty = "io.file.buffer.size"

  private var fsCache = Map.empty[String, HdfsIO]

  def apply(fs: FileSystem): HdfsIO = apply(fs.getUri.toString)
  def apply(host: String, port: Int): HdfsIO = apply("hdfs://" + host + ":" + port)
  def apply(uri: String): HdfsIO = fsCache.getOrElse(uri, synchronized {
    fsCache.getOrElse(uri, {
      val hdfsIO = new HdfsIO(uri)
      fsCache = fsCache.updated(uri, hdfsIO)
      hdfsIO
    })
  })

  lazy val default: HdfsIO = apply(FileSystem.get(SparkHadoopUtil.get.conf))

  implicit def toInstance(self: this.type)(implicit context: AccessContext = AccessContext.default): HdfsIO = {
    context.hdfsIO
  }
}

class HdfsIO private (val uri: String) extends Serializable {
  import org.archive.webservices.sparkling.Sparkling._

  @transient lazy val fs: FileSystem = FileSystem.get(new URI(uri), SparkHadoopUtil.get.conf)

  @transient implicit lazy val logContext: LogContext = LogContext(this)

  @transient private lazy val localFiles = collection.mutable.Map.empty[String, String]

  @transient implicit lazy val accessContext: AccessContext = AccessContext(this)

  def clearFileLocalCopy(path: String): Unit = localFiles.synchronized {
    for (localCopy <- localFiles.remove(path)) {
      val f = new File(localCopy)
      var retry = false
      while (f.exists) {
        if (retry) Thread.`yield`()
        Try(f.delete())
        retry = true
      }
    }
  }

  def open(
      path: String,
      offset: Long = 0,
      length: Long = 0,
      decompress: Boolean = true,
      retries: Int = 10,
      sleepMillis: Int = 1000,
      strategy: HdfsIO.LoadingStrategy = HdfsIO.defaultLoadingStrategy
  ): InputStream = {
    val loadingStrategy =
      if (strategy == HdfsIO.LoadingStrategy.Dynamic) {
        val fileSize = this.length(path)
        val copyLocalThreshold = fileSize.toDouble * HdfsIO.dynamicCopyLocalThreshold
        if (localFiles.contains(path)) HdfsIO.LoadingStrategy.CopyLocal
        else if (length > copyLocalThreshold) HdfsIO.LoadingStrategy.CopyLocal
        else if (length < 0 && fileSize > fs.getFileStatus(new Path(path)).getBlockSize) HdfsIO.LoadingStrategy.CopyLocal
        else HdfsIO.LoadingStrategy.BlockWise
      } else strategy
    Log.info(
      "Opening file " + path + " (Offset: " + offset + ", length: " + length + ", decompress: " + decompress + ", strategy: " + loadingStrategy +
        (if (strategy == HdfsIO.LoadingStrategy.Dynamic) " [dynamic]" else "") + ")"
    )
    val in = loadingStrategy match {
      case HdfsIO.LoadingStrategy.Remote => Common.retryObj(fs.open(new Path(path)))(
          retries,
          sleepMillis,
          _.close,
          (_, retry, e) => {
            "File access failed (" + retry + "/" + retries + "): " + path + " (Offset: " + offset + ") - " + e.getClass.getSimpleName + Option(e.getMessage).map(_.trim).filter(_.nonEmpty).map(" - " + _).getOrElse("")
          }
        ) { (in, _) =>
          if (offset > 0) {
            if (Try(in.seek(offset)).isFailure) {
              if (!Try(in.seekToNewSource(offset)).getOrElse(false)) {
                in.seek(offset)
              }
            }
          }
          if (length > 0) new BoundedInputStream(in, length) else in
        }
      case HdfsIO.LoadingStrategy.BlockWise => new HdfsBlockStream(fs, path, offset, length, retries, sleepMillis, HdfsIO.blockReadTimeoutMillis)
      case HdfsIO.LoadingStrategy.CopyLocal => Common.retryObj {
          val localCopy = localFiles.synchronized {
            localFiles.getOrElseUpdate(path, {
              val tmpPath = IOUtil.tmpFile.getCanonicalPath
              fs.copyToLocalFile(new Path(path), new Path(tmpPath))
              tmpPath
            })
          }
          new FileInputStream(localCopy)
        }(
          retries,
          sleepMillis,
          _.close,
          (_, retry, e) => { "File access failed (" + retry + "/" + retries + "): " + path + " - " + e.getClass.getSimpleName + Option(e.getMessage).map(_.trim).filter(_.nonEmpty).map(" - " + _).getOrElse("") }
        ) { (in, _) =>
          if (offset > 0) in.getChannel.position(offset)
          if (length > 0) new BoundedInputStream(in, length) else in
        }
    }
    val buffered = IOUtil.supportMark(in)
    if (IOUtil.eof(buffered)) {
      buffered.close()
      IOUtil.EmptyStream
    } else if (decompress) {
      val decompressed = IOUtil.decompress(buffered, Some(path))
      IOUtil.supportMark(decompressed)
    } else buffered
  }

  def access[R](
      path: String,
      offset: Long = 0,
      length: Long = 0,
      decompress: Boolean = true,
      retries: Int = 60,
      sleepMillis: Int = 1000 * 60,
      strategy: HdfsIO.LoadingStrategy = HdfsIO.defaultLoadingStrategy
  )(action: InputStream => R): R = {
    val in = open(path, offset, length, decompress, retries, sleepMillis, strategy)
    val r = action(in)
    Try(in.close())
    r
  }

  def copyFromLocal(src: String, dst: String, move: Boolean = false, overwrite: Boolean = false, replication: Short = 0): Unit = {
    val dstPath = new Path(dst)
    val exists = fs.exists(dstPath)
    if (exists && !overwrite) throw new FileAlreadyExistsException(dst)
    val dstTmpPath = new Path(dst + "._copying")
    val dstReplication = if (replication == 0) if (defaultReplication == 0) fs.getDefaultReplication(dstPath) else defaultReplication else replication
    val conf = new org.apache.hadoop.conf.Configuration(SparkHadoopUtil.get.conf)
    conf.setInt(HdfsIO.ReplicationProperty, 1)
    FileUtil.copy(FileSystem.getLocal(conf), new Path(src), fs, dstTmpPath, move, true, conf)
    if (exists && overwrite) delete(dst)
    fs.rename(dstTmpPath, dstPath)
    if (dstReplication > 1) fs.setReplication(dstPath, dstReplication)
  }

  def rename(src: String, dst: String): Unit = fs.rename(new Path(src), new Path(dst))

  def length(path: String): Long = fs.getFileStatus(new Path(path)).getLen

  def lines(path: String, n: Int = -1, offset: Long = 0): Seq[String] = access(path, offset) { in =>
    val lines = IOUtil.lines(in)
    if (n < 0) lines.toList else lines.take(n).toList
  }

  def files(path: String, recursive: Boolean = true): Iterator[String] = {
    val glob = fs.globStatus(new Path(path))
    if (glob == null) Iterator.empty
    else glob.toIterator.flatMap { status => if (status.isDirectory && recursive) files(new Path(status.getPath, "*").toString) else Iterator(status.getPath.toString) }
  }

  def dir(path: String): String = {
    val p = new Path(path)
    val status = fs.globStatus(p)
    if (status == null || status.isEmpty || (status.length == 1 && status.head.isDirectory)) path else p.getParent.toString
  }

  def createTmpPath(prefix: String = tmpFilePrefix, path: String = tmpHdfsPath, deleteOnExit: Boolean = true): String = {
    var rnd = System.currentTimeMillis + "-" + Random.nextInt.abs
    var tmpPath: Path = null
    while ({
      tmpPath = new Path(path, prefix + rnd)
      fs.exists(tmpPath) || !fs.mkdirs(tmpPath)
    }) rnd = System.currentTimeMillis + "-" + Random.nextInt.abs
    if (deleteOnExit) fs.deleteOnExit(tmpPath)
    tmpPath.toString
  }

  def tmpPath[R](action: String => R): R = {
    val path = createTmpPath()
    try {
      action(path)
    } finally {
      delete(path)
    }
  }

  def delete(path: String): Unit = if (exists(path)) {
    val p = new Path(path)
    val success = fs.delete(p, true)
    if (!success) fs.deleteOnExit(p)
  }

  def exists(path: String): Boolean = fs.exists(new Path(path))

  def ensureOutDir(path: String, ensureNew: Boolean = true): Unit = {
    if (ensureNew && exists(path)) Common.printThrow("Path exists: " + path)
    fs.mkdirs(new Path(path))
  }

  def ensureNewFile(path: String): Unit = { if (exists(path)) Common.printThrow("File exists: " + path) }

  def writer(path: String, overwrite: Boolean = false, append: Boolean = false, replication: Short = 0): HdfsFileWriter = HdfsFileWriter(path, overwrite, append, replication)

  def bufferSize: Int = fs.getConf.getInt(HdfsIO.BufferSizeProperty, 4096)

  def out(path: String, overwrite: Boolean = false, compress: Boolean = true, useWriter: Boolean = true, append: Boolean = false, temporary: Boolean = false): OutputStream = {
    val out =
      if (useWriter) writer(path, overwrite, append, if (temporary) tmpFileReplication else 0)
      else if (append) fs.append(new Path(path))
      else {
        val fsPath = new Path(path)
        if (temporary) fs.create(fsPath, overwrite, bufferSize, tmpFileReplication, fs.getDefaultBlockSize(fsPath)) else fs.create(fsPath, overwrite)
      }
    if (compress && path.toLowerCase.endsWith(GzipExt)) new GZIPOutputStream(out) else out
  }

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

  def collectDistinctLines(path: String, parallel: Boolean = true, map: String => Option[String] = Some(_), lineBuffer: Int = HdfsIO.DefaultLineBuffer): Set[String] =
    if (parallel) {
      val parallel = files(path).toSet.par
      parallel.flatMap { file =>
        val in = open(file)
        IteratorUtil.cleanup(IOUtil.lines(in), in.close).flatMap(l => map(l)).grouped(lineBuffer).map(_.toSet).foldLeft(Set.empty[String])(_ ++ _)
      }.seq
    } else iterLines(path).flatMap(l => map(l)).grouped(lineBuffer).map(_.toSet).foldLeft(Set.empty[String])(_ ++ _)

  def touch(path: String): Unit = Common.touch(out(path, useWriter = false, append = exists(path), compress = false))(_.write(Array.empty[Byte])).close()
}
