package org.archive.webservices.sparkling.io

import org.apache.spark.rdd.RDD
import org.archive.webservices.sparkling.Sparkling
import org.archive.webservices.sparkling.logging.{Log, LogContext}
import org.archive.webservices.sparkling.util.Common

import java.io.{BufferedInputStream, File, FileInputStream, InputStream}
import java.time.Instant
import scala.reflect.ClassTag
import scala.util.Try

object StageSyncManager {
  val ThreadSyncSleep = 1000 // 1s

  def sleep(): Unit = Thread.sleep(ThreadSyncSleep)

  private var stages = Map.empty[String, StageSyncManager]

  def stageId(stageId: Int): String = {
    Sparkling.appId + "-" + stageId
  }

  def stageId: String = stageId(Sparkling.taskContext.map(_.stageId).getOrElse(0))

  def syncFile(workingDir: String, stageId: String): File = {
    // this is exclusively per JVM to sync across JVMs / executors (not threads / tasks)
    // stays until sync at the end of all parallel tasks of an executor
    new File(workingDir, stageId + "._sync")
  }

  def claimedFile(workingDir: String, stageId: String): File = {
    // this contains the last time claimed, to check if the current JVM had it last, to avoid transferring the process
    new File(workingDir, stageId + "._claimed")
  }

  def pidFile(workingDir: String, stageId: String): File = {
    // this is created per process / cluster node, shared across JVMs / executors
    new File(workingDir, stageId + ".pid")
  }

  def launchDetachableShell(proc: SystemProcess): Int = proc.synchronized {
    val tmpSesId = "bash" + Instant.now.toEpochMilli
    proc.exec("tmux -f /dev/null new-session -d -s " + tmpSesId + " '/bin/bash' \\; display-message -p -t " + tmpSesId + ":0 '#{pane_pid}'")
    val pid = proc.readAllInput().mkString.trim.toInt
    proc.exec(s"kill -STOP $pid; reptyr -T $pid 2>/dev/null; kill -CONT $pid;", supportsEcho = true, blocking = true)
    pid
  }

  def stage: StageSyncManager = {
    val id = stageId
    stages.getOrElse(id, synchronized {
      stages.getOrElse(id, {
        val stage = new StageSyncManager(id)
        stages += stageId -> stage
        stage
      })
    })
  }

  def syncProcess(cmd: String, workingDir: String, shell: => SystemProcess, exec: (SystemProcess, Int, String) => Unit, cleanup: (SystemProcess, Int) => Unit = (_, _) => {}): (SystemProcess, Int) = {
    stage.syncProcess(cmd, workingDir, shell, exec)
  }

  def claimProcess(workingDir: String): (SystemProcess, Int) = stage.claimProcess(workingDir)

  def claimFileIn(path: String): InputStream = stage.claimFileIn(path: String)

  def sync[A: ClassTag](rdd: RDD[A]): RDD[A] = {
    rdd.mapPartitions(syncPartition)
  }

  def syncPartition[A](partition: Iterator[A]): Iterator[A] = {
    stage.syncPartition(partition)
  }

  def cleanup(stageId: String): Unit = synchronized {
    for (stage <- stages.get(stageId)) {
      stage.cleanup()
      stages -= stageId
    }
  }

  def lockMutex(blocking: Boolean = true): Unit = stage.lockMutex(blocking)

  def unlockMutex(): Unit = stage.unlockMutex()
}

class StageSyncManager private (stageId: String) {
  implicit val logContext: LogContext = LogContext(this)

  private var activeTasks = Set.empty[Long]
  private var syncTasks = Set.empty[Long]
  private var syncing: Boolean = false

  private var processes = Map.empty[String, (SystemProcess, Int)]
  private var cleanupHooks = Map.empty[String, (SystemProcess, Int) => Unit]
  private var files = Map.empty[String, InputStream]
  private var claimed = Set.empty[String]
  private var lastClaimed = Map.empty[String, Long]

  private var mutex: Set[Long] = Set.empty
  private var blockingMutex: Option[Long] = None

  def lockMutex(blocking: Boolean = true): Unit = {
    val task = Sparkling.taskId
    if (mutex.contains(task)) return
    def hold: Boolean = (blocking && mutex.nonEmpty) || blockingMutex.isDefined
    while (!mutex.contains(task)) {
      while (hold) StageSyncManager.sleep()
      synchronized(if (!hold) {
        if (blocking) {
          Log.info(s"Locking mutex for task $task.")
          blockingMutex = Some(task)
        }
        mutex += task
      })
    }
  }

  def unlockMutex(): Unit = {
    val task = Sparkling.taskId
    if (!mutex.contains(task)) return
    synchronized(if (mutex.contains(task)) {
      if (blockingMutex.contains(task)) {
        Log.info(s"Unlocking mutex for task $task.")
        blockingMutex = None
      }
      mutex -= task
    })
  }

  def syncPartition[A](partition: Iterator[A]): Iterator[A] = {
    val task = Sparkling.taskId
    Iterator(true).flatMap { _ =>
      while (syncing) StageSyncManager.sleep()
      synchronized(activeTasks += task)
      Iterator.empty
    } ++ partition ++ Iterator(true).flatMap { _ =>
      synchronized {
        activeTasks -= task
        syncTasks += task
        if (activeTasks.isEmpty) {
          syncStage()
          syncing = true
        }
      }
      // wait here until another task / core of this JVM / executor encounters activeTasks.isEmpty and syncs up
      while (!syncing) StageSyncManager.sleep()
      // complete syncing
      synchronized {
        syncTasks -= task
        if (syncTasks.isEmpty) syncing = false
      }
      Iterator.empty
    }
  }

  private def syncStage(): Unit = synchronized {
    for (f <- claimed.map(StageSyncManager.syncFile(_, stageId))) f.delete()
    for (s <- files.values) s.close()
    claimed = Set.empty
    files = Map.empty
  }

  def cleanup(): Unit = synchronized {
    syncStage()
    for ((d, (p, pid)) <- processes) {
      Common.sync(StageSyncManager.syncFile(d, stageId)) {
        for (f <- Some(StageSyncManager.pidFile(d, stageId)).filter(_.exists())) f.delete()
        for (f <- Some(StageSyncManager.claimedFile(d, stageId)).filter(_.exists())) f.delete()
        for (cleanup <- cleanupHooks.get(d)) cleanup(p, pid)
        p.destroy()
      }
    }
    cleanupHooks = Map.empty
    processes = Map.empty
    lastClaimed = Map.empty
    mutex = Set.empty
    blockingMutex = None
  }

  def syncProcess(cmd: String, workingDir: String, shell: => SystemProcess, exec: (SystemProcess, Int, String) => Unit, cleanup: (SystemProcess, Int) => Unit = (_, _) => {}): (SystemProcess, Int) = {
    def checkProcess(p: (SystemProcess, Int) => Boolean): Boolean = {
      for ((process, pid) <- processes.get(workingDir)) p(process, pid)
      true
    }

    lazy val syncf = StageSyncManager.syncFile(workingDir, stageId)
    lazy val pidf = StageSyncManager.pidFile(workingDir, stageId)
    while (checkProcess((p, pid) => return (p, pid)) && !pidf.exists() && !syncf.createNewFile()) StageSyncManager.sleep()

    synchronized {
      checkProcess((p, pid) => return (p, pid))
      val p = shell
      val pid = if (pidf.exists) {
        val pid = IOUtil.lines(pidf.getAbsolutePath).mkString.trim.toInt
        processes += workingDir -> (p, pid)
        pid
      } else {
        syncf.deleteOnExit()
        val pid = p.synchronized {
          val pid = StageSyncManager.launchDetachableShell(p)
          exec(p, pid, s"exec $cmd")
          IOUtil.writeLines(pidf.getAbsolutePath, Seq(pid.toString))
          pidf.deleteOnExit()
          processes += workingDir -> (p, pid)
          cleanupHooks += workingDir -> cleanup
          pid
        }
        registerClaim(workingDir)
        pid
      }
      (p, pid)
    }
  }

  private def registerClaim(workingDir: String): Unit = synchronized {
    claimed += workingDir
    val millis = Instant.now.toEpochMilli
    val claimf = StageSyncManager.claimedFile(workingDir, stageId)
    IOUtil.writeLines(claimf.getAbsolutePath, Seq(millis.toString))
    lastClaimed += workingDir -> millis
  }

  private def isLastClaim(workingDir: String): Boolean = {
    lastClaimed.get(workingDir).contains {
      val claimf = StageSyncManager.claimedFile(workingDir, stageId)
      Try(IOUtil.lines(claimf.getAbsolutePath).mkString.trim.toLong).getOrElse(0)
    }
  }

  def claimFileIn(path: String): InputStream = {
    files.getOrElse(path, synchronized {
      files.getOrElse(path, {
        val in = new LazyBufferedFileInputStream(path)
        files += path -> in
        in
      })
    })
  }

  def claimProcess(workingDir: String): (SystemProcess, Int) = {
    for ((proc, pid) <- processes.get(workingDir) if claimed.contains(workingDir)) return (proc, pid)

    synchronized {
      for ((proc, pid) <- processes.get(workingDir) if claimed.contains(workingDir)) return (proc, pid)

      val syncf = StageSyncManager.syncFile(workingDir, stageId)
      while (!syncf.createNewFile()) StageSyncManager.sleep()
      syncf.deleteOnExit()

      processes.get(workingDir) match {
        case Some((proc, pid)) =>
          if (!isLastClaim(workingDir)) proc.synchronized {
            proc.consumeAllInput(true)
            proc.exec(s"kill -STOP $pid; reptyr $pid 2>/dev/null; kill -CONT $pid", supportsEcho = false)
          }
          registerClaim(workingDir)
          return (proc, pid)
        case None =>
          throw new RuntimeException(s"No process available for $workingDir ($stageId).")
      }
    }
  }
}
