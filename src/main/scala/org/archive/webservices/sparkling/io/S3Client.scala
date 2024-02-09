package org.archive.webservices.sparkling.io

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, AnonymousAWSCredentials, BasicAWSCredentials}
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.transfer.{TransferManager, Upload}
import com.amazonaws.services.s3.{AmazonS3Client, S3ClientOptions}
import com.amazonaws.{AmazonServiceException, ClientConfiguration}

import java.io._

object S3Client {
  object AnonymousAWSCredentialsProvider extends AWSCredentialsProvider {
    private val credentials = new AnonymousAWSCredentials()
    def getCredentials: AWSCredentials = credentials
    def refresh(): Unit = {}
  }

  class CredentialsProvider(accessKey: String, secretKey: String) extends AWSCredentialsProvider {
    def getCredentials: AWSCredentials = new BasicAWSCredentials(accessKey, secretKey)
    def refresh(): Unit = {}
  }

  def aws(credentials: AWSCredentialsProvider): S3Client = new S3Client(new AmazonS3Client(credentials))

  def aws(accessKey: String, secretKey: String): S3Client = aws(new CredentialsProvider(accessKey, secretKey))

  def anonymousAws: S3Client = aws(AnonymousAWSCredentialsProvider)

  def apply(endpoint: String, accessKey: String, secretKey: String): S3Client = {
    apply(endpoint, new CredentialsProvider(accessKey, secretKey))
  }

  def apply(endpoint: String, credentials: AWSCredentialsProvider): S3Client = {
    val s3 = new AmazonS3Client(credentials, {
      val conf = new ClientConfiguration()
      // https://github.com/seaweedfs/seaweedfs/blob/master/other/java/s3copier/src/main/java/com/seaweedfs/s3/HighLevelMultipartUpload.java#L32
      conf.setSignerOverride("AWSS3V4SignerType")
      conf
    })
    s3.setEndpoint(endpoint)
    s3.setS3ClientOptions({
      val options = new S3ClientOptions()
      options.setPathStyleAccess(true)
      options
    })
    new S3Client(s3)
  }
}

class S3Client(val s3: AmazonS3Client) {
  val transfers = new TransferManager(s3)

  def upload(bucket: String, file: File, dstPath: String): Upload = transfers.upload(bucket, dstPath, file)

  def upload(bucket: String, srcPath: String, dstPath: String): Upload = upload(bucket, new File(srcPath), dstPath)

  def upload(bucket: String, in: InputStream, length: Long, dstPath: String): Upload = transfers.upload(bucket, dstPath, in, {
    val meta = new ObjectMetadata()
    meta.setContentLength(length)
    meta
  })

  def upload(bucket: String, dstPath: String)(action: OutputStream => Unit): Upload = {
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

  def uploadPrint(bucket: String, dstPath: String)(action: PrintStream => Unit): Upload = {
    upload(bucket, dstPath) { out =>
      val print = IOUtil.print(new NonClosingOutputStream(out))
      action(print)
      print.close()
    }
  }

  def uploadLines(bucket: String, lines: Iterator[String], dstPath: String): Upload = {
    uploadPrint(bucket, dstPath) { out =>
      for (line <- lines) out.println(line)
    }
  }

  def exists(bucket: String, dstPath: String): Boolean = meta(bucket, dstPath).isDefined

  def meta(bucket: String, dstPath: String): Option[ObjectMetadata] = try {
    Some(s3.getObjectMetadata(bucket, dstPath))
  } catch {
    case e: AmazonServiceException =>
      if (e.getStatusCode == 404) None else throw e
  }

  def length(bucket: String, dstPath: String): Long = meta(bucket, dstPath).map(_.getContentLength).getOrElse(-1)

  def open[R](bucket: String, dstPath: String)(action: InputStream => R): R = {
    val tmpFile = IOUtil.tmpFile
    try {
      transfers.download(bucket, dstPath, tmpFile).waitForCompletion()
      val in = new BufferedInputStream(new FileInputStream(tmpFile))
      try {
        action(in)
      } finally {
        in.close()
      }
    } finally {
      tmpFile.delete()
    }
  }

  def openLines[R](bucket: String, dstPath: String)(action: Iterator[String] => R): R = open(bucket, dstPath) { in =>
    action(IOUtil.lines(in, maxLineLength = -1))
  }

  def delete(bucket: String, dstPath: String): Boolean = try {
    s3.deleteObject(bucket, dstPath)
    true
  } catch {
    case e: AmazonServiceException =>
      if (e.getStatusCode == 404) false else throw e
  }

  def access[R](action: S3Client => R): R = try {
    action(this)
  } finally {
    close()
  }

  def close(): Unit = transfers.shutdownNow(true) // shutDownS3Client = true
}
