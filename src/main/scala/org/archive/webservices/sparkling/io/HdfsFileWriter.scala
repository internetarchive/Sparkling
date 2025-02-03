package org.archive.webservices.sparkling.io

import org.apache.hadoop.fs.Path
import org.archive.webservices.sparkling.AccessContext
import org.archive.webservices.sparkling.logging.{Log, LogContext}

import java.io.{FileInputStream, OutputStream}
import scala.util.Try

class HdfsFileWriter private (accessContext: AccessContext, filename: String, append: Boolean, replication: Short) extends OutputStream {
  implicit val logContext: LogContext = LogContext(this)

  private val file = IOUtil.tmpFile

  Log.info("Writing to temporary local file " + file.getCanonicalPath + " (" + filename + ")...")

  val out = IOUtil.fileOut(file)

  override def close(): Unit = {
    Try { out.close() }
    Log.info("Copying from temporary file " + file.getCanonicalPath + " to " + filename + "...")
    if (append) {
      val in = new FileInputStream(file)
      val appendOut = accessContext.hdfsIO.fs.append(new Path(filename))
      IOUtil.copy(in, appendOut)
      appendOut.close()
      in.close()
    } else accessContext.hdfsIO.copyFromLocal(file.getCanonicalPath, filename, move = true, overwrite = true, replication)
    if (!file.delete()) file.deleteOnExit()
    Log.info("Done. (" + filename + ")")
  }

  override def write(b: Int): Unit = out.write(b)
  override def write(b: Array[Byte]): Unit = out.write(b)
  override def write(b: Array[Byte], off: Int, len: Int): Unit = out.write(b, off, len)
  override def flush(): Unit = out.flush()
}

object HdfsFileWriter {
  def apply(filename: String, overwrite: Boolean = false, append: Boolean = false, replication: Short = 0)(implicit accessContext: AccessContext = AccessContext.default): HdfsFileWriter = {
    if (!overwrite && !append) accessContext.hdfsIO.ensureNewFile(filename)
    new HdfsFileWriter(accessContext, filename, append, replication)
  }
}
