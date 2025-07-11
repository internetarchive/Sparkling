package org.archive.webservices.sparkling.io

import org.apache.spark.rdd.RDD
import org.archive.webservices.sparkling.Sparkling
import org.archive.webservices.sparkling.io.StageSyncManager.initFile
import org.archive.webservices.sparkling.logging.{Log, LogContext}
import org.archive.webservices.sparkling.util.{Common, IteratorUtil}

import java.io.{File, InputStream}
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

  def initFile(workingDir: String): File = {
    // this is exclusively per JVM to sync across JVMs / executors (not threads / tasks)
    // stays until sync at the end of all parallel tasks of an executor
    new File(workingDir, "_initializing")
  }

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
    proc.exec("tmux -f /dev/null new-session -d -s " + tmpSesId + " '/bin/bash -c \"exec bash\"' \\; display-message -p -t " + tmpSesId + ":0 '#{pane_pid}'")
    val pid = proc.readAllInput().mkString.trim.toInt
    proc.exec(s"kill -STOP $pid; reptyr -T $pid 2>/dev/null; kill -CONT $pid")
    proc.demandLine("echo $$", pid.toString)
    proc.consumeAllInput()
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

  def syncProcess(cmd: String, workingDir: String, shell: => SystemProcess, exec: (SystemProcess, Int, String) => Unit, cleanup: (SystemProcess, Int) => Unit = (_, _) => {}, restart: Boolean = false): (SystemProcess, Int) = {
    stage.syncProcess(cmd, workingDir, shell, exec, cleanup, restart)
  }

  def claimProcess(workingDir: String): (SystemProcess, Int, Boolean) = stage.claimProcess(workingDir)

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

  private var files = Map.empty[String, InputStream]

  private val processLock = new AnyRef
  private var processes = Map.empty[String, (SystemProcess, Int)]
  private var cleanupHooks = Map.empty[String, (SystemProcess, Int) => Unit]
  private var claimed = Set.empty[String]
  private var lastClaimed = Map.empty[String, Long]

  private val mutexLock = new AnyRef
  private var mutex: Set[Long] = Set.empty
  private var blockingMutex: Option[Long] = None

  def lockMutex(blocking: Boolean = true): Unit = {
    val task = Sparkling.taskId
    Log.info(s"Locking mutex for task $task... (blocking: $blocking)")
    if (mutex.contains(task)) {
      Log.info(s"Already locked $task (blocking: $blocking).")
      return
    }
    def hold: Boolean = {
      (blocking && mutex.nonEmpty && !mutex.contains(task)) || (blockingMutex.isDefined && !blockingMutex.contains(task))
    }
    while (!mutex.contains(task)) {
      if (hold) {
        Log.info(s"Waiting for mutex for task $task... (blocking: $blocking, blocking task: $blockingMutex)")
        while (hold) StageSyncManager.sleep()
        Log.info(s"Mutex available for $task, attempting... (blocking: $blocking)")
      }
      Log.info(s"Synchronized #lockMutex.. (task: $task, blocking: $blocking)")
      mutexLock.synchronized {
        Log.info(s"Synchronized #lockMutex....  (task: $task, blocking: $blocking)")
        if (!hold) {
          if (blocking) {
            Log.info(s"Locked mutex for task $task (blocking: $blocking).")
            blockingMutex = Some(task)
          }
          mutex += task
        }
        Log.info(s"Synchronized #lockMutex.  (task: $task, blocking: $blocking)")
      }
    }
  }

  def unlockMutex(): Unit = {
    val task = Sparkling.taskId
    Log.info(s"Unlocking mutex for task $task...")
    if (!mutex.contains(task)) {
      Log.info(s"Already unlocked $task.")
      return
    }
    Log.info(s"Synchronized #unlockMutex.. (task: $task)")
    mutexLock.synchronized {
      Log.info(s"Synchronized #unlockMutex.... (task: $task)")
      if (mutex.contains(task)) {
        if (blockingMutex.contains(task)) {
          Log.info(s"Unlocked mutex for task $task.")
          blockingMutex = None
        }
        mutex -= task
      }
    }
    Log.info(s"Synchronized #unlockMutex. (task: $task)")
  }

  def syncPartition[A](partition: Iterator[A]): Iterator[A] = {
    val task = Sparkling.taskId
    Iterator(true).flatMap { _ =>
      Log.info(s"Syncing task $task, waiting...")
      while (syncing) StageSyncManager.sleep()
      Log.info(s"Syncing task $task...")
      synchronized(activeTasks += task)
      Iterator.empty
    } ++ IteratorUtil.tryFinally(partition, {
      synchronized {
        activeTasks -= task
        syncTasks += task
        if (activeTasks.isEmpty) {
          syncStage()
          syncing = true
        }
      }
      // wait here until another task / core of this JVM / executor encounters activeTasks.isEmpty and syncs up
      if (!syncing) {
        Log.info(s"Waiting for tasks to sync up...")
        while (!syncing) StageSyncManager.sleep()
        Log.info(s"Tasks synced up...")
      }
      // complete syncing
      synchronized {
        syncTasks -= task
        if (syncTasks.isEmpty) syncing = false
      }
    })
  }

  private def syncStage(): Unit = {
    Log.info(s"Synchronized #syncStage..")
    synchronized {
      Log.info(s"Synchronized #syncStage....")

      Log.info(s"Synchronized #syncStage/processLock..")
      processLock.synchronized {
        Log.info(s"Synchronized #syncStage/processLock....")
        for (d <- claimed) IOUtil.delete(StageSyncManager.syncFile(d, stageId))
        claimed = Set.empty
      }
      Log.info(s"Synchronized #syncStage/processLock.")

      // avoid blocking if a pipe was not properly closed
      for (s <- files.values) Common.tryCatch(Common.timeout(10000)(s.close()))
      files = Map.empty
    }
    Log.info(s"Synchronized #syncStage.")
  }

  def cleanup(): Unit = {
    Log.info(s"Synchronized #cleanup..")
    synchronized {
      Log.info(s"Synchronized #cleanup....")
      processLock.synchronized {
        Log.info(s"Synchronized #cleanup/processLock...")
        syncStage()
        for ((d, (p, pid)) <- processes) {
          Log.info(s"Synchronized #cleanup/syncFile..")
          // sync is okay, because claimed was cleared before within the same processLock
          Common.sync(StageSyncManager.syncFile(d, stageId)) {
            Log.info(s"Synchronized #cleanup/syncFile....")
            for (f <- Some(StageSyncManager.pidFile(d, stageId)).filter(_.exists())) f.delete()
            for (f <- Some(StageSyncManager.claimedFile(d, stageId)).filter(_.exists())) f.delete()
            for (cleanup <- cleanupHooks.get(d)) Common.tryCatch(Common.timeout(10000)(cleanup(p, pid)))
            p.destroy()
          }
          Log.info(s"Synchronized #cleanup/syncFile.")
        }
        processes = Map.empty
        lastClaimed = Map.empty
        cleanupHooks = Map.empty
      }
      Log.info(s"Synchronized #cleanup/processLock.")
      Log.info(s"Synchronized #cleanup/mutexLock..")
      mutexLock.synchronized {
        Log.info(s"Synchronized #cleanup/mutexLock....")
        mutex = Set.empty
        blockingMutex = None
      }
      Log.info(s"Synchronized #cleanup/mutexLock.")
    }
    Log.info(s"Synchronized #cleanup.")
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

  private def registerLastClaim(workingDir: String): Unit = {
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

  def syncProcess(cmd: String, workingDir: String, shell: => SystemProcess, exec: (SystemProcess, Int, String) => Unit, cleanup: (SystemProcess, Int) => Unit = (_, _) => {}, restart: Boolean = false): (SystemProcess, Int) = {
    if (!restart) for ((process, pid) <- processes.get(workingDir) if !process.destroyed) return (process, pid)

    Log.info(s"Synchronized #syncProcess/processLock.. ($workingDir, $stageId)")
    lazy val syncf = StageSyncManager.syncFile(workingDir, stageId)
    Common.synchronizedIf(processLock, {
      if (!restart) for ((process, pid) <- processes.get(workingDir) if !process.destroyed) return (process, pid)
      syncf.createNewFile() || claimed.contains(workingDir)
    }, StageSyncManager.sleep()) {
      Log.info(s"Synchronized #syncProcess/processLock.... ($workingDir, $stageId)")

      var p = shell

      val pidf = StageSyncManager.pidFile(workingDir, stageId)
      if (pidf.exists) {
        var taken = false
        val pid = IOUtil.lines(pidf.getAbsolutePath).mkString.trim.toInt
        try {
          if (restart) {
            val oldPid = processes.get(workingDir).map(_._2)
            if (!oldPid.contains(pid)) {
              Log.info(s"Process already restarted $workingDir, $stageId.")
              taken = true
              return (p, pid)
            }
          } else {
            Log.info(s"Process exists $workingDir, $stageId.")
            taken = true
            return (p, pid)
          }
        } finally {
          if (taken) {
            processes += workingDir -> (p, pid)
            IOUtil.delete(syncf)
            claimed -= workingDir
            lastClaimed -= workingDir
            Log.info(s"Synchronized #syncProcess/processLock. (Process existed $workingDir, $stageId.)")
          }
        }
      }

      // no other executor (re)started, therefore clean up...
      for {
        (p, pid) <- processes.get(workingDir)
        cleanup <- cleanupHooks.get(workingDir)
      } {
        Log.info(s"Stopping old process ($pid) $workingDir...")
        p.destroy()
        Log.info(s"Cleaning up old process ($pid) $workingDir...")
        Common.tryCatch(Common.timeout(10000)(cleanup(p, pid)))
        Log.info(s"Cleaned up old process ($pid) $workingDir.")
      }

      Log.info(s"Synchronized #syncProcess/initFile.. ($workingDir)")
      Common.sync(initFile(workingDir)) {
        Log.info(s"Synchronized #syncProcess/initFile.... ($workingDir)")
        var launched = false
        Common.infinite {
          try {
            Log.info(s"Launching $workingDir...")
            return Common.timeout(600000) { // 10 minutes
              val pid = StageSyncManager.launchDetachableShell(p)
              exec(p, pid, s"exec $cmd")
              IOUtil.writeLines(pidf.getAbsolutePath, Seq(pid.toString))
              processes += workingDir -> (p, pid)
              cleanupHooks += workingDir -> cleanup
              registerLastClaim(workingDir)
              launched = true
              (p, pid)
            }
          } catch {
            case e: Exception =>
              Log.error(e)
              Log.info(s"Retrying after error (${e.getMessage}): $workingDir...")
              e.printStackTrace()
              p.destroy()
              p = shell
          } finally {
            if (launched) Log.info(s"Synchronized #syncProcess/processLock. (Launched $workingDir, $stageId.)")
          }
        }
      }
    }
  }

  def claimProcess(workingDir: String): (SystemProcess, Int, Boolean) = {
    for ((proc, pid) <- processes.get(workingDir) if claimed.contains(workingDir)) return (proc, pid, false)

    Log.info(s"Synchronized #claimProcess/processLock.. ($workingDir, $stageId)")
    lazy val syncf = StageSyncManager.syncFile(workingDir, stageId)
    Common.synchronizedIf(processLock, {
      for ((proc, pid) <- processes.get(workingDir) if claimed.contains(workingDir)) return (proc, pid, false)
      syncf.createNewFile()
    }, StageSyncManager.sleep()) {
      Log.info(s"Synchronized #claimProcess/processLock.... ($workingDir, $stageId)")

      processes.get(workingDir) match {
        case Some((proc, pid)) =>
          Log.info(s"Claiming process $workingDir...")

          val reclaim = !isLastClaim(workingDir)
          if (reclaim) {
            proc.exec(s"kill -STOP $pid; reptyr $pid 2>/dev/null; kill -CONT $pid", supportsEcho = false)
          }

          registerLastClaim(workingDir)

          Log.info(s"Synchronized #claimProcess/processLock. (Claimed process $workingDir, $stageId.)")
          return (proc, pid, reclaim)
        case None =>
          throw new RuntimeException(s"No process available for $workingDir ($stageId).")
      }
    }
  }
}
