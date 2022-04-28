package org.archive.webservices.sparkling.io

abstract class StringSerializer[A] extends Serializable {
  def serialize(a: A): String
  def deserialize(a: String): A
}

object StringSerializer {
  implicit def toTypedInOut[A](serializer: StringSerializer[A]): TypedInOut[A] = TypedInOut.toStringInOut(serializer.serialize, serializer.deserialize)

  implicit lazy val identity: StringSerializer[String] = new StringSerializer[String] {
    override def serialize(a: String): String = a
    override def deserialize(a: String): String = a
  }
}
