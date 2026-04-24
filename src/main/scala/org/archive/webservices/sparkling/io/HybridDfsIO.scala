package org.archive.webservices.sparkling.io

import org.archive.webservices.sparkling.Sparkling.GzipExt
import org.archive.webservices.sparkling.util.Common

import java.io.{IOException, InputStream, OutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

/** DfsIO implementation that routes between S3 and HDFS based on path scheme.
 *  Paths beginning with s3:// are handled via S3Client using full bucket/key
 *  URIs; all other paths fall through to HdfsIO. A single S3Client (tied to
 *  the configured endpoint) is reused across all buckets.
 */
class HybridDfsIO(endpoint: String, accessKey: String, secretKey: String) extends DfsIO {
  import org.archive.webservices.sparkling._

  @transient lazy val client: S3Client = S3Client(endpoint, accessKey, secretKey)

  private def isS3(path: String): Boolean = path.startsWith("s3://")

  private def parse(uri: String): (String, String) = {
    val stripped = uri.stripPrefix("s3://")
    val slash = stripped.indexOf('/')
    if (slash < 0) (stripped, "")
    else (stripped.substring(0, slash), stripped.substring(slash + 1))
  }

  def open(
      path: String,
      offset: Long = 0,
      length: Long = 0,
      decompress: Boolean = true,
      retries: Int = 10,
      sleepMillis: Int = 1000,
      strategy: DfsIO.LoadingStrategy = DfsIO.DefaultLoadingStrategy
  ): InputStream = {
    if (!isS3(path)) return HdfsIO.default.open(path, offset, length, decompress, retries, sleepMillis, strategy)
    val (bucket, key) = parse(path)
    val raw = client.open(bucket, key, offset, length, close = false)(identity)
    if (decompress && key.toLowerCase.endsWith(GzipExt)) new GZIPInputStream(raw) else raw
  }

  def copyFromLocal(src: String, dst: String, move: Boolean = false, overwrite: Boolean = false, replication: Short = 0): Unit = {
    if (!isS3(dst)) { HdfsIO.default.copyFromLocal(src, dst, move, overwrite, replication); return }
    try {
      val (bucket, key) = parse(dst)
      if (exists(dst)) {
        if (!overwrite) throw new IOException("file exists: " + dst)
      }
      client.upload(bucket, src, key)
    } finally {
      if (move) IOUtil.delete(src)
    }
  }

  def rename(src: String, dst: String): Unit = {
    if (!isS3(src)) { HdfsIO.default.rename(src, dst); return }
    if (exists(src)) {
      val (srcBucket, srcKey) = parse(src)
      val (dstBucket, dstKey) = parse(dst)
      client.copy(srcBucket, srcKey, dstKey)
      client.delete(srcBucket, srcKey)
    }
  }

  def length(path: String): Long = {
    if (!isS3(path)) return HdfsIO.default.length(path)
    val (bucket, key) = parse(path)
    client.length(bucket, key)
  }

  def files(path: String, recursive: Boolean = true): Iterator[String] = {
    if (!isS3(path)) return HdfsIO.default.files(path, recursive)
    val (bucket, rawKey) = parse(path)
    def prefixUri(key: String) = s"s3://$bucket/$key"

    val list = if (rawKey.endsWith("/") || rawKey.isEmpty) {
      client.list(bucket, rawKey).map(prefixUri)
    } else {
      val parts = rawKey.split('/').filter(_.nonEmpty)
      if (parts.isEmpty) {
        client.list(bucket, rawKey).map(prefixUri)
      } else {
        val name = parts.last
        val prefix = if (parts.length > 1) parts.dropRight(1).mkString("/") else ""
        if (name.contains("*")) {
          val nameParts = name.split("\\*", -1)
          client.list(bucket, prefix).filter { key =>
            val n = key.split('/').filter(_.nonEmpty).last
            n.startsWith(nameParts.head) && n.endsWith(nameParts.last)
          }.map(prefixUri)
        } else {
          client.list(bucket, prefix).filter {
            _.split('/').filter(_.nonEmpty).last == name
          }.map(prefixUri)
        }
      }
    }

    if (recursive) {
      def iter(l: Iterator[String]): Iterator[String] = l.flatMap { p =>
        if (p.endsWith("/")) iter(client.list(bucket, parse(p)._2).map(prefixUri))
        else Iterator(p)
      }
      iter(list)
    } else list
  }

  def dir(path: String): String = {
    if (!isS3(path)) return HdfsIO.default.dir(path)
    val (bucket, key) = parse(path)
    val parentKey = key.split("/", -1).dropRight(1).mkString("/")
    s"s3://$bucket/$parentKey"
  }

  // Note: unlike HdfsIO.delete which is recursive, S3 requires you to delete all files in a
  // directory before deleting the directory itself (subject to S3 implementation). So we just
  // delete the specified path, and if it's a directory the delete should silently fail.
  // For the moment, the caller is responsible for deleting all files within it first.
  def delete(path: String): Unit = {
    if (!isS3(path)) { HdfsIO.default.delete(path); return }
    if (exists(path)) {
      val (bucket, key) = parse(path)
      client.delete(bucket, key)
    }
  }

  def exists(path: String): Boolean = {
    if (!isS3(path)) return HdfsIO.default.exists(path)
    val (bucket, key) = parse(path)
    Common.retry(client.exists(bucket, key), times = 30, sleepMillis = 1000)
  }

  def out(path: String, overwrite: Boolean = false, compress: Boolean = true, useWriter: Boolean = true, append: Boolean = false, temporary: Boolean = false): OutputStream = {
    if (!isS3(path)) return HdfsIO.default.out(path, overwrite, compress, useWriter, append, temporary)
    val (bucket, key) = parse(path)
    val tmpFile = IOUtil.tmpFile
    if (exists(path)) {
      if (append) client.download(bucket, key, tmpFile)
      else if (!overwrite) throw new IOException("file exists: " + path)
    }
    val fileOut = IOUtil.fileOut(tmpFile, append = append)
    val compressed = if (compress && path.toLowerCase.endsWith(GzipExt)) new GZIPOutputStream(fileOut) else fileOut
    new CleanupOutputStream(compressed, () => {
      compressed.flush()
      compressed.close()
      client.upload(bucket, tmpFile, key)
    })
  }

  override def mkdirs(path: String): Boolean = {
    if (!isS3(path)) HdfsIO.default.mkdirs(path) else true
  }

  override def deleteOnExit(path: String): Unit = {
    if (!isS3(path)) HdfsIO.default.deleteOnExit(path)
  }

  override def getBlockSize(path: String): Long = {
    if (!isS3(path)) HdfsIO.default.getBlockSize(path) else 64.mb
  }
}
