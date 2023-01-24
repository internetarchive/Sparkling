package org.archive.webservices.sparkling.compression

import java.io.InputStream

trait Decompressor {
  def decompress(in: InputStream, filename: Option[String] = None, checkFile: Boolean = false): InputStream
}
