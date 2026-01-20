package org.archive.webservices.sparkling.io

import com.amazonaws.services.s3.model.GetObjectRequest
import org.archive.webservices.sparkling.AccessContext
import org.archive.webservices.sparkling.Sparkling.{GzipExt, loggingId}
import org.archive.webservices.sparkling.util.IteratorUtil

import java.io.{FileSystem => _, _}
import java.util.zip.GZIPOutputStream

object S3DfsIO {
  def forBucket(bucket: String)(s3: => S3Client): S3DfsIO = {
    new S3DfsIO(s3, bucket)
  }

  implicit def toInstance(self: this.type)(implicit context: AccessContext = AccessContext.default): S3DfsIO = {
    context.dfsIO.asInstanceOf[S3DfsIO]
  }
}

class S3DfsIO private (s3: => S3Client, bucket: String) extends DfsIO {
  import org.archive.webservices.sparkling._

  def open(
      path: String,
      offset: Long = 0,
      length: Long = 0,
      decompress: Boolean = true,
      retries: Int = 10,
      sleepMillis: Int = 1000,
      strategy: DfsIO.LoadingStrategy = HdfsIO.defaultLoadingStrategy
  ): InputStream = {
    val client = s3
    val request = if (offset > 0 || length > 0) {
      val end = (if (length > 0) offset + length else client.length(bucket, path)) - 1
      new GetObjectRequest(bucket, path).withRange(offset, end)
    } else new GetObjectRequest(bucket, path)
    new CleanupInputStream(
      client.s3.getObject(request).getObjectContent,
      client.close)
  }

  def copyFromLocal(src: String, dst: String, move: Boolean = false, overwrite: Boolean = false, replication: Short = 0): Unit = try {
    s3.access { client =>
      if (client.exists(bucket, dst)) {
        if (overwrite) client.delete(bucket, dst)
        else throw new IOException("file exists: " + dst)
      }
      client.upload(bucket, src, dst).waitForCompletion()
    }
  } finally {
    if (move) IOUtil.delete(src)
  }

  def rename(src: String, dst: String): Unit = s3.access { client =>
    if (client.exists(bucket, src)) {
      client.s3.copyObject(bucket, src, bucket, dst)
      client.delete(bucket, src)
    }
  }

  def length(path: String): Long = s3.access(_.length(bucket, path))

  def files(path: String, recursive: Boolean = true): Iterator[String] = {
    val client = s3
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
      def iter(l: Seq[String]): Iterator[String] = l.toIterator.flatMap { p =>
        if (p.endsWith("/")) iter(client.list(bucket, p)) else Iterator(p)
      }
      IteratorUtil.cleanup(iter(list), client.close)
    } else {
      try {
        list.toIterator
      } finally {
        client.close()
      }
    }
  }

  def dir(path: String): String = path.split("/", -1).dropRight(1).mkString("/")

  def delete(path: String): Unit = s3.access { client =>
    if (client.exists(bucket, path)) client.delete(bucket, path)
  }

  def exists(path: String): Boolean = s3.access(_.exists(bucket, path))

  def out(path: String, overwrite: Boolean = false, compress: Boolean = true, useWriter: Boolean = true, append: Boolean = false, temporary: Boolean = false): OutputStream = {
    val tmpFile = IOUtil.tmpFile
    val client = s3
    if (exists(path)) {
      if (overwrite) client.delete(bucket, path)
      else {
        if (append) client.download(bucket, path, tmpFile).waitForCompletion()
        else throw new IOException("file exists: " + path)
      }
    }
    val out = IOUtil.fileOut(tmpFile, append = append)
    val compressed = if (compress && path.toLowerCase.endsWith(GzipExt)) new GZIPOutputStream(out) else out
    new CleanupOutputStream(compressed, () => {
      compressed.flush()
      compressed.close()
      client.upload(bucket, tmpFile, path).waitForCompletion()
      client.close()
    })
  }

  override def mkdirs(path: String): Boolean = true

  override def deleteOnExit(path: String): Unit = {}

  override def getBlockSize(path: String): Long = 64.mb
}
