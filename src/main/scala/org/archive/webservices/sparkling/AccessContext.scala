package org.archive.webservices.sparkling

import org.apache.spark.broadcast.Broadcast
import org.archive.webservices.sparkling.io.{DfsIO, HdfsIO}

case class AccessContext(dfsIO: DfsIO) {
  def broadcast: Broadcast[AccessContext] = Sparkling.sc.broadcast(this)
}

object AccessContext {
  lazy val default: AccessContext = AccessContext(HdfsIO.default)

  implicit def fromBroadcast(implicit broadcast: Broadcast[AccessContext]): AccessContext = broadcast.value
}