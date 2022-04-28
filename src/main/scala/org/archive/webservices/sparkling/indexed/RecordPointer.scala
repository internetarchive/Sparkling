package org.archive.webservices.sparkling.indexed

case class PositionPointer(offset: Long, length: Long)

case class RecordPointer(path: String, position: PositionPointer, prefixIdx: Int)
