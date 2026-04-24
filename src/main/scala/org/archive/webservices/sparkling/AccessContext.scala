package org.archive.webservices.sparkling

import org.apache.spark.broadcast.Broadcast
import org.archive.webservices.sparkling.io.{DfsIO, HdfsIO, HybridDfsIO}

case class AccessContext(dfsIO: DfsIO) {
  def broadcast: Broadcast[AccessContext] = Sparkling.sc.broadcast(this)
}

object AccessContext {
  val S3EndpointEnvVar  = "S3_ENDPOINT"
  val S3AccessKeyEnvVar = "AWS_ACCESS_KEY_ID"
  val S3SecretKeyEnvVar = "AWS_SECRET_ACCESS_KEY"

  lazy val default: AccessContext = AccessContext(HdfsIO.default)

  def fromEnv: AccessContext = sys.env.get(S3EndpointEnvVar) match {
    case Some(endpoint) =>
      val accessKey = sys.env.getOrElse(S3AccessKeyEnvVar, throw new RuntimeException(s"$S3AccessKeyEnvVar must be set when $S3EndpointEnvVar is set"))
      val secretKey = sys.env.getOrElse(S3SecretKeyEnvVar, throw new RuntimeException(s"$S3SecretKeyEnvVar must be set when $S3EndpointEnvVar is set"))
      AccessContext(new HybridDfsIO(endpoint, accessKey, secretKey))
    case None =>
      default
  }

  implicit def fromBroadcast(implicit broadcast: Broadcast[AccessContext]): AccessContext = broadcast.value
}