package org.archive.webservices.sparkling.util

import org.archive.webservices.sparkling.logging.{Log, LogContext}

class MemoryControl (maxGrowthFactor: Double = 2) {
  implicit val logContext: LogContext = LogContext(this)

  private var enforcing = false

  def usedMem: Long = {
    val runtime = Runtime.getRuntime
    val totalMemory = runtime.totalMemory
    val freeMemory = runtime.freeMemory
    totalMemory - freeMemory
  }

  private var refMem = usedMem

  def check(): Boolean = {
    val used = usedMem
    val ref = refMem
    used < ref || ((used - ref).toDouble / ref < maxGrowthFactor)
  }

  def enforce(): Unit = {
    if (enforcing || !check()) synchronized {
      enforcing = !check()
      if (enforcing) {
        Log.info("Enforcing max memory growth of factor " + maxGrowthFactor + " from " + StringUtil.formatBytes(refMem) + "...")
        System.gc()
        Thread.`yield`()
        refMem = usedMem
        enforcing = false
      }
    }
  }

  private var syncing = 0
  def sync(start: Boolean): Unit = {
    if (!start) synchronized(syncing -= 1)
    while (!check() && syncing > 0) Thread.`yield`()
    enforce()
    if (start) synchronized(syncing += 1)
  }

  def enforceSync[R](task: => R): R = {
    sync(true)
    try {
      task
    } finally {
      sync(false)
    }
  }
}
