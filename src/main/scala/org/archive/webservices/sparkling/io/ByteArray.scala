package org.archive.webservices.sparkling.io

import java.io.{BufferedInputStream, ByteArrayInputStream, InputStream}

class ByteArray {
  private val arrays = collection.mutable.Buffer.empty[Array[Byte]]

  def append(array: Array[Byte]): Unit = if (array.nonEmpty) arrays += array
  def append(array: ByteArray): Unit = if (array.nonEmpty) arrays ++= array.arrays

  def toInputStream: InputStream = new BufferedInputStream(new ChainedInputStream(arrays.toIterator.map(new ByteArrayInputStream(_))))

  def length: Long = arrays.map(_.length.toLong).sum

  def isEmpty: Boolean = length == 0
  def nonEmpty: Boolean = length > 0

  def copy(): ByteArray = {
    val copy = new ByteArray()
    copy.append(this)
    copy
  }
}
