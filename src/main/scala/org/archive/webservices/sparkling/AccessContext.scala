package org.archive.webservices.sparkling

import org.archive.webservices.sparkling.io.{DfsIO, HdfsIO}

case class AccessContext (dfsIO: DfsIO)

object AccessContext {
  lazy val default: AccessContext = apply(HdfsIO.default)
  def apply(dfsIO: DfsIO): AccessContext = new AccessContext(dfsIO)
}