package org.archive.webservices.sparkling.util

import ammonite.interp.api.InterpAPI
import ammonite.repl.api.ReplAPI
import org.apache.hadoop.fs.Path
import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession
import org.archive.webservices.BasicReadOnlyHttpFileSystem
import org.archive.webservices.sparkling.Sparkling
import org.archive.webservices.sparkling.io.{DfsIO, IOUtil}

import java.net.InetAddress

object SparkUtil {
  import org.archive.webservices.sparkling.Sparkling._

  trait SparkReplClassServer {
    def stop(): Unit
    def uri: String
  }

  val DefaultConfig: Map[String, String] = Map(
    "spark.submit.deployMode" -> "client",
    "spark.network.timeout" -> "9999999s",
    "spark.executor.heartbeatInterval" -> "999999s",
    "spark.driver.port" -> "54728",
    "spark.ui.port" -> "54040",
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.constraintPropagation.enabled" -> "false",
    "spark.default.parallelism" -> "100"
  )

  def ammoniteBuilder(implicit interpApi: InterpAPI, replApi: ReplAPI): SparkSession.Builder = {
    new AmmoniteSessionBuilder(interpApi, replApi)
  }

  def k8sConfig(
      containerImage: String,
      nameSpace: String,
      appName: String = "",
      executors: Int,
      executorCores: Int,
      executorMemory: String = "1g",
      master: String = "https://kubernetes.default.svc:443",
      driverPort: Int = 7077,
      uiPort: Int = 4040,
      additionalConfigs: Map[String, String] = Map.empty,
      verbose: Boolean = false,
      jars: Seq[String] = Seq.empty)(implicit interpApi: InterpAPI, replApi: ReplAPI): SparkContext = {
    val builder = ammoniteBuilder
    for (sc <- Sparkling.scOpt) sc.stop()
    if (!verbose) {
      Logger.getRootLogger.setLevel(Level.OFF)
      Logger.getLogger("org").setLevel(Level.OFF)
    }
    var building = builder.appName(Some(appName).filter(_.nonEmpty).map(_ + " (" + Sparkling.Name + ")").getOrElse(Sparkling.Name))
    building = building.master("k8s://" + master)
    val host = InetAddress.getLocalHost
    val config = DefaultConfig ++ Map(
      "spark.kubernetes.container.image" -> containerImage,
      "spark.kubernetes.namespace" -> nameSpace,
      "spark.kubernetes.driver.pod.name" -> host.getHostName,
      "spark.kubernetes.container.image.pullPolicy" -> "Always",
      "spark.driver.host" -> host.getHostAddress,
      "spark.driver.port" -> driverPort,
      "spark.ui.port" -> uiPort,
      "spark.executor.instances" -> executors,
      "spark.executor.cores" -> executorCores,
      "spark.executor.memory" -> executorMemory,
      "spark.hadoop.fs.http.impl" -> classOf[BasicReadOnlyHttpFileSystem].getName) ++ additionalConfigs
    for ((k, v) <- config) building = building.config(k, v.toString)
    val sc = building.getOrCreate.sparkContext
    Sparkling.resetSparkContext(Some(sc))
    sc
  }

  // interp.load.cp(ammonite.ops.Path(sys.env.get("HADOOP_CONF_DIR").get))
  // Thread.currentThread().getContextClassLoader.getResource("core-site.xml")
  // https://github.com/alexarchambault/ammonite-spark/blob/b33e9145fda4a13d9020cef13bb461e65634ae39/modules/core/src/main/scala/org/apache/spark/sql/ammonitesparkinternals/AmmoniteSparkSessionBuilder.scala

  // to use Almond / Ammonite builder: org.apache.spark.sql.NotebookSparkSession.builder().progress(false)
  def config[B <: SparkSession.Builder](
      builder: B = SparkSession.builder,
      appName: String = "",
      queue: String = "default",
      executors: Int = 100,
      executorCores: Int = 1,
      executorMemory: String = "2g",
      additionalConfigs: Map[String, String] = Map.empty,
      verbose: Boolean = false,
      jars: Seq[String] = Seq.empty,
      replClassServer: SparkReplClassServer = null
  ): SparkContext = {
    for (sc <- Sparkling.scOpt) sc.stop()
    if (!verbose) {
      Logger.getRootLogger.setLevel(Level.OFF)
      Logger.getLogger("org").setLevel(Level.OFF)
    }
    var building = builder.appName(Some(appName).filter(_.nonEmpty).map(_ + " (" + Sparkling.Name + ")").getOrElse(Sparkling.Name))
    val config = DefaultConfig ++ Map(
      "spark.master" -> "yarn",
      "spark.yarn.queue" -> queue,
      "spark.executor.instances" -> executors,
      "spark.executor.cores" -> executorCores,
      "spark.executor.memory" -> executorMemory,
      "spark.executorEnv.JAVA_HOME" -> Option(System.getenv("JAVA_HOME")).getOrElse("/usr/lib/jvm/jdk1.8.0_144")) ++
      Option(replClassServer).map("spark.repl.class.uri" -> _.uri).toMap ++ additionalConfigs
    for ((k, v) <- config) building = building.config(k, v.toString)
    val sc = building.getOrCreate.sparkContext
    Sparkling.resetSparkContext(Some(sc))
    for (p <- Seq(jarPath) ++ jars) sc.addJar(p)
    for (server <- Option(replClassServer)) sc.addSparkListener(new SparkListener {
      override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = server.stop()
    })
    sc
  }

  def dynamicAllocationConfig[B <: SparkSession.Builder](
      builder: B = SparkSession.builder,
      appName: String = "",
      queue: String = "default",
      minExecutors: Int = 2,
      maxExecutors: Int = 20,
      executorCores: Int = 1,
      executorMemory: String = "2g",
      executorIdleTimeout: String = "60s",
      additionalConfigs: Map[String, String] = Map.empty,
      verbose: Boolean = false,
      jars: Seq[String] = Seq.empty,
      replClassServer: SparkReplClassServer = null
  ): SparkContext = {
    for (sc <- Sparkling.scOpt) sc.stop()
    if (!verbose) {
      Logger.getRootLogger.setLevel(Level.OFF)
      Logger.getLogger("org").setLevel(Level.OFF)
    }
    var building = builder.appName(Some(appName).filter(_.nonEmpty).map(_ + " (" + Sparkling.Name + ")").getOrElse(Sparkling.Name) + " [dynamic]")
    val config = DefaultConfig ++ Map(
      "spark.master" -> "yarn",
      "spark.yarn.queue" -> queue,
      "spark.executor.cores" -> executorCores,
      "spark.executor.memory" -> executorMemory,
      "spark.dynamicAllocation.enabled" -> "true",
      "spark.shuffle.service.enabled" -> "true",
      "spark.dynamicAllocation.minExecutors" -> minExecutors,
      "spark.dynamicAllocation.maxExecutors" -> maxExecutors,
      "spark.dynamicAllocation.executorIdleTimeout" -> executorIdleTimeout,
      "spark.executorEnv.JAVA_HOME" -> Option(System.getenv("JAVA_HOME")).getOrElse("/usr/lib/jvm/jdk1.8.0_144")
    ) ++ Option(replClassServer).map("spark.repl.class.uri" -> _.uri).toMap ++ additionalConfigs
    for ((k, v) <- config) building = building.config(k, v.toString)
    val sc = building.getOrCreate.sparkContext
    Sparkling.resetSparkContext(Some(sc))
    for (p <- Seq(jarPath) ++ jars) sc.addJar(p)
    for (server <- Option(replClassServer)) sc.addSparkListener(new SparkListener {
      override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = server.stop()
    })
    sc
  }

  def addLocalFile(path: String): Unit = {
    val dst = IOUtil.concatPaths(DfsIO.createTmpPath(), new Path(path).getName)
    DfsIO.copyFromLocal(path, dst)
    sc.addFile("hdfs://" + dst, recursive = true)
  }

//  private var cleanupObjects = CollectionUtil.concurrentMap[Any, Long]
//  private var cleanups = CollectionUtil.concurrentMap[Long, mutable.Map[Any, () => Unit]]
//
  def cleanupTask(owner: Any, cleanup: () => Unit): Unit = {
//    val task = TaskContext.get
//    if (task != null) {
//      cleanupObjects.getOrElseUpdate(
//        owner, {
//          val id = task.taskAttemptId
//          val taskCleanups = cleanups.getOrElseUpdate(
//            id, {
//              val taskCleanups = CollectionUtil.concurrentMap[Any, () => Unit]
//              task.addTaskCompletionListener[Unit] { ctx: TaskContext =>
//                cleanups.remove(id)
//                for ((o, c) <- taskCleanups) {
//                  cleanupObjects.remove(o)
//                  c()
//                }
//                taskCleanups.clear()
//              }
//              taskCleanups
//            }
//          )
//          taskCleanups.update(owner, cleanup)
//          id
//        }
//      )
//    }
  }

  def removeTaskCleanup(owner: Any): Unit = {} //{ for (attemptId <- cleanupObjects.remove(owner); attempt <- cleanups.get(attemptId)) attempt.remove(owner) }
}
