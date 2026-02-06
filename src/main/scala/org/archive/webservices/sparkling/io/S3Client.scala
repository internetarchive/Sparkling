package org.archive.webservices.sparkling.io

import software.amazon.awssdk.auth.credentials.{AnonymousCredentialsProvider, AwsBasicCredentials, AwsCredentialsProvider, StaticCredentialsProvider}
import software.amazon.awssdk.core.internal.http.AmazonSyncHttpClient
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3
import software.amazon.awssdk.services.s3.S3Configuration
import software.amazon.awssdk.services.s3.model._

import java.io._
import java.net.URI
import scala.collection.convert.ImplicitConversions._

object S3Client {
  var region: Region = Region.US_EAST_1

  def aws(credentials: AwsCredentialsProvider): S3Client = {
    new S3Client(s3.S3Client.builder.credentialsProvider(credentials).region(region).build)
  }

  def aws(accessKey: String, secretKey: String): S3Client = {
    aws(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
  }

  def anonymousAws: S3Client = aws(AnonymousCredentialsProvider.create())

  def apply(endpoint: String, accessKey: String, secretKey: String): S3Client = {
    apply(endpoint, StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
  }

  def apply(endpoint: String, credentials: AwsCredentialsProvider): S3Client = {
    val config = S3Configuration.builder
      .pathStyleAccessEnabled(true)
      .chunkedEncodingEnabled(false).build
    val httpClientBuilder = UrlConnectionHttpClient.builder()
    val client = s3.S3Client.builder.httpClientBuilder(httpClientBuilder).endpointOverride(URI.create(endpoint))
      .credentialsProvider(credentials)
      .region(region)
      .serviceConfiguration(config).build
    new S3Client(client)
  }
}

class S3Client(val client: s3.S3Client) {
  def upload(bucket: String, requestBody: RequestBody, dstPath: String): PutObjectResponse = client.putObject({
    PutObjectRequest.builder.bucket(bucket).key(dstPath).build
  }, requestBody)

  def upload(bucket: String, file: File, dstPath: String): PutObjectResponse = {
    upload(bucket, RequestBody.fromFile(file), dstPath)
  }

  def upload(bucket: String, srcPath: String, dstPath: String): PutObjectResponse = {
    upload(bucket, new File(srcPath), dstPath)
  }

  def upload(bucket: String, in: InputStream, length: Long, dstPath: String): PutObjectResponse = {
    upload(bucket, RequestBody.fromInputStream(in, length), dstPath)
  }

  def upload(bucket: String, dstPath: String)(action: OutputStream => Unit): PutObjectResponse = {
    val tmpFile = IOUtil.tmpFile
    val out = IOUtil.fileOut(tmpFile)
    try {
      action(out)
    } finally {
      out.close()
    }
    val in = new CleanupInputStream(new BufferedInputStream(new FileInputStream(tmpFile)), () => tmpFile.delete())
    upload(bucket, in, tmpFile.length, dstPath)
  }

  def uploadPrint(bucket: String, dstPath: String)(action: PrintStream => Unit): PutObjectResponse = {
    upload(bucket, dstPath) { out =>
      val print = IOUtil.print(new NonClosingOutputStream(out))
      action(print)
      print.close()
    }
  }

  def uploadLines(bucket: String, lines: Iterator[String], dstPath: String): PutObjectResponse = {
    uploadPrint(bucket, dstPath) { out =>
      for (line <- lines) out.println(line)
    }
  }

  def list(bucket: String, path: String): Iterator[String] = {
    val prefix = (path.trim.stripSuffix("/") + "/").stripPrefix("/")
    val listObjectsRequest = ListObjectsV2Request.builder.bucket(bucket).prefix(prefix).delimiter("/").build
    val listing = client.listObjectsV2Paginator(listObjectsRequest)
    listing.contents.toIterator.map(_.key) ++ listing.commonPrefixes.map(_.prefix)
  }

  def exists(bucket: String, dstPath: String): Boolean = meta(bucket, dstPath).isDefined

  def meta(bucket: String, dstPath: String): Option[HeadObjectResponse] = try {
    Some(client.headObject(HeadObjectRequest.builder.bucket(bucket).key(dstPath).build))
  } catch {
    case _: NoSuchKeyException => None
  }

  def length(bucket: String, dstPath: String): Long = meta(bucket, dstPath).map(_.contentLength.longValue).getOrElse(-1)

  def download(bucket: String, src: String, dst: String): Unit = download(bucket, src, new File(dst))

  def download(bucket: String, src: String, dst: File, offset: Long = 0, length: Long = 0): Unit = {
    val out = new FileOutputStream(dst)
    try {
      open(bucket, src, offset = offset, length = length)(IOUtil.copy(_, out))
    } finally {
      out.close()
    }
  }

  def copy(bucket: String, src: String, dst: String): Unit = {
    client.copyObject(CopyObjectRequest.builder.sourceBucket(bucket).sourceKey(src).destinationKey(bucket).destinationKey(dst).build)
  }

  def open[R](bucket: String, path: String, offset: Long = 0, length: Long = 0, close: Boolean = true)(action: InputStream => R): R = {
    var request = GetObjectRequest.builder.bucket(bucket).key(path)
    if (offset > 0 || length > 0) {
      val end = if (length > 0) length + offset - 1 else 0
      request = request.range(s"bytes=$offset-" + (if (end > 0) end else ""))
    }
    val obj = client.getObject(request.build)
    val in = new CleanupInputStream(obj, () => {
      obj.abort()
      obj.close()
    }, closeSuper = false)
    try {
      action(in)
    } finally {
      if (close) in.close()
    }
  }

  def openLines[R](bucket: String, dstPath: String)(action: Iterator[String] => R): R = open(bucket, dstPath) { in =>
    action(IOUtil.lines(in, maxLineLength = -1))
  }

  def delete(bucket: String, dstPath: String): Boolean = try {
    client.deleteObject(DeleteObjectRequest.builder.bucket(bucket).key(dstPath).build)
    true
  } catch {
    case _: NoSuchKeyException => false
  }

  def access[R](action: S3Client => R): R = try {
    action(this)
  } finally {
    close()
  }

  def close(): Unit = client.close()
}
