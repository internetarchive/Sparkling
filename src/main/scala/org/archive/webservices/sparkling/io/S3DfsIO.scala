package org.archive.webservices.sparkling.io

import org.archive.webservices.sparkling.AccessContext
import org.archive.webservices.sparkling.Sparkling.GzipExt
import org.archive.webservices.sparkling.util.Common

import java.io.{FileSystem => _, _}
import java.util.zip.GZIPOutputStream

object S3DfsIO {
  def forBucket(bucket: String)(s3: => S3Client): S3DfsIO = new S3DfsIO(s3, bucket)

  def apply(endpoint: String, accessKey: String, secretKey: String, bucket: String): S3DfsIO = {
    forBucket(bucket)(S3Client(endpoint, accessKey, secretKey))
  }

  implicit def toInstance(self: this.type)(implicit context: AccessContext = AccessContext.default): S3DfsIO = {
    context.dfsIO.asInstanceOf[S3DfsIO]
  }

  private var clientCache: Option[S3Client] = None
}

class S3DfsIO private (s3: => S3Client, bucket: String) extends DfsIO {
  import org.archive.webservices.sparkling._

  @transient lazy val client: S3Client = S3DfsIO.synchronized {
    S3DfsIO.clientCache match {
      case Some(client) => client
      case None =>
        S3DfsIO.clientCache = Some(s3)
        client
    }
  }

  def open(
      path: String,
      offset: Long = 0,
      length: Long = 0,
      decompress: Boolean = true,
      retries: Int = 10,
      sleepMillis: Int = 1000,
      strategy: DfsIO.LoadingStrategy = HdfsIO.defaultLoadingStrategy
  ): InputStream = client.open(bucket, path, offset, length, close = false)(identity)

  def copyFromLocal(src: String, dst: String, move: Boolean = false, overwrite: Boolean = false, replication: Short = 0): Unit = try {
    if (exists(dst)) {
      if (!overwrite) throw new IOException("file exists: " + dst)
    }
    client.upload(bucket, src, dst)
  } finally {
    if (move) IOUtil.delete(src)
  }

  def rename(src: String, dst: String): Unit = if (exists(src)) {
    client.copy(bucket, src, dst)
    client.delete(bucket, src)
  }

  def length(path: String): Long = client.length(bucket, path)

  def files(path: String, recursive: Boolean = true): Iterator[String] = {
    val list = if (path.endsWith("/")) client.list(bucket, path) else {
      val split = path.split('/').filter(_.nonEmpty)
      if (split.isEmpty) client.list(bucket, path) else {
        val f = split.last
        val p = if (split.length > 1) split.dropRight(1).mkString("/") else ""
        if (f.contains("*")) {
          val fSplit = f.split("\\*", -1)
          client.list(bucket, p).filter { m =>
            val name = m.split('/').filter(_.nonEmpty).last
            name.startsWith(fSplit.head) && name.endsWith(fSplit.last)
          }
        } else client.list(bucket, p).filter {
          _.split('/').filter(_.nonEmpty).last == f
        }
      }
    }
    if (recursive) {
      def iter(l: Iterator[String]): Iterator[String] = l.flatMap { p =>
        if (p.endsWith("/")) iter(client.list(bucket, p)) else Iterator(p)
      }
      iter(list)
    } else list
  }

  def dir(path: String): String = path.split("/", -1).dropRight(1).mkString("/")

  def delete(path: String): Unit = if (exists(path)) client.delete(bucket, path)

  def exists(path: String): Boolean = Common.retry(client.exists(bucket, path), times = 30, sleepMillis = 1000)

  def out(path: String, overwrite: Boolean = false, compress: Boolean = true, useWriter: Boolean = true, append: Boolean = false, temporary: Boolean = false): OutputStream = {
    val tmpFile = IOUtil.tmpFile
    if (exists(path)) {
      if (append) client.download(bucket, path, tmpFile)
      else throw new IOException("file exists: " + path)
    }
    val out = IOUtil.fileOut(tmpFile, append = append)
    val compressed = if (compress && path.toLowerCase.endsWith(GzipExt)) new GZIPOutputStream(out) else out
    new CleanupOutputStream(compressed, () => {
      compressed.flush()
      compressed.close()
      client.upload(bucket, tmpFile, path)
    })
  }

  override def mkdirs(path: String): Boolean = true

  override def deleteOnExit(path: String): Unit = {}

  override def getBlockSize(path: String): Long = 64.mb
}
