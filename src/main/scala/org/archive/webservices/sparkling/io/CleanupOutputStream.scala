package org.archive.webservices.sparkling.io

import java.io.{FilterOutputStream, OutputStream}

class CleanupOutputStream(out: OutputStream, cleanup: () => Unit) extends FilterOutputStream(out) {
  override def close(): Unit = try {
    super.close()
  } finally {
    cleanup()
  }
}
