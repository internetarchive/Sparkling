package org.archive.webservices.sparkling.util

object BytesUtil {
  def littleEndianInt(bytes: Array[Byte]): Int = {
    if (bytes.isEmpty) return 0
    var value: Int = bytes.last
    for (b <- bytes.reverse.drop(1)) {
      value <<= 8
      value |= b & 0xFF
    }
    value
  }

  def bigEndianInt(bytes: Array[Byte]): Int = {
    if (bytes.isEmpty) return 0
    var value: Int = bytes.head
    for (b <- bytes.drop(1)) {
      value <<= 8
      value |= b & 0xFF
    }
    value
  }
}
