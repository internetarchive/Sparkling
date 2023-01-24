package org.archive.webservices.sparkling.io

import java.io.OutputStream

class PipingOutputStream extends OutputStream {
  var out: Option[OutputStream] = None
  def set(o: OutputStream): Unit = out = Some(o)

  override def write(b: Int): Unit = for (o <- out) o.write(b)
  override def write(b: Array[Byte]): Unit = for (o <- out) o.write(b)
  override def write(b: Array[Byte], off: Int, len: Int): Unit = for (o <- out) o.write(b, off, len)
  override def flush(): Unit = for (o <- out) o.flush()
  override def close(): Unit = for (o <- out) o.close()
}
