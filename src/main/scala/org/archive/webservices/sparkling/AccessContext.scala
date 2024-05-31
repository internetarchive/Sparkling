package org.archive.webservices.sparkling

import org.archive.webservices.sparkling.io.HdfsIO

case class AccessContext (hdfsIO: HdfsIO)

object AccessContext {
  lazy val default: AccessContext = apply(HdfsIO.default)
  def apply(hdfsIO: HdfsIO): AccessContext = new AccessContext(hdfsIO)
}