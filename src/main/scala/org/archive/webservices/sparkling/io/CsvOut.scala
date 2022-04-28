package org.archive.webservices.sparkling.io

import java.io.{OutputStream, PrintStream}

class CsvOut(out: OutputStream) {
  val print: PrintStream = IOUtil.print(out)

  private var firstFieldInLine: Boolean = true

  def line[R](action: => R): R = {
    firstFieldInLine = true
    val r = action
    print.println()
    r
  }

  def unescape(str: String): Unit = {
    if (firstFieldInLine) { print.print(str) }
    else print.print("," + str)
    firstFieldInLine = false
  }

  def escape(str: String): Unit = field(str, escape = true)

  def field(str: String, escape: Boolean = true): Unit = { unescape(if (escape) "\"" + str.replace("\"", "\"\"") + "\"" else str) }

  def field(i: Int): Unit = unescape(i.toString)

  def field(l: Long): Unit = unescape(l.toString)

  def field(b: Boolean): Unit = unescape(b.toString)

  def close(): Unit = {
    print.flush()
    print.close()
  }
}
