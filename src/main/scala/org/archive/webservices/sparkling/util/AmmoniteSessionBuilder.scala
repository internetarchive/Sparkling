package org.archive.webservices.sparkling.util

import ammonite.interp.api.InterpAPI
import ammonite.repl.api.ReplAPI
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.ammonitesparkinternals.{AmmoniteClassServer, AmmoniteSparkSessionBuilder, SparkDependencies}

import java.net.InetAddress

object AmmoniteSessionBuilder {
  var systemJars = Set.empty[String]

  def loadedJars(implicit replApi: ReplAPI): Seq[String] = {
    AmmoniteSparkSessionBuilder.classpath(
      replApi.sess.frames.last.classloader.getParent
    ).map(_.toURI) ++ {
      replApi.sess.frames.flatMap(_.classpath).filter(AmmoniteSparkSessionBuilder.shouldPassToSpark).map(_.toURI)
    }
  }.map(_.toASCIIString)

  def registerSystemJars()(implicit replApi: ReplAPI): Unit = {
    systemJars ++= loadedJars
  }
}

class AmmoniteSessionBuilder private[util](interpApi: InterpAPI, replApi: ReplAPI) extends SparkSession.Builder {
  private val options: scala.collection.Map[String, String] = {
    // copied from AmmoniteSparkSessionBuilder#options0
    def fieldVia(name: String): Option[scala.collection.mutable.HashMap[String, String]] =
      try {
        val f = classOf[SparkSession.Builder].getDeclaredField(name)
        f.setAccessible(true)
        Some(f.get(this).asInstanceOf[scala.collection.mutable.HashMap[String, String]])
      } catch {
        case _: NoSuchFieldException =>
          None
      }

    fieldVia("org$apache$spark$sql$SparkSession$Builder$$options")
      .orElse(fieldVia("options"))
      .getOrElse {
        println("Warning: can't read SparkSession Builder options (options field not found)")
        Map.empty[String, String]
      }
  }

  override def getOrCreate: SparkSession = {
    val loadedJars = AmmoniteSessionBuilder.loadedJars(replApi)
    val jars = loadedJars.filter { uri =>
      uri.contains("/lihaoyi/") || uri.contains("/sparkling")
    } ++ (if (AmmoniteSessionBuilder.systemJars.isEmpty) Set.empty else {
      loadedJars.toSet -- AmmoniteSessionBuilder.systemJars
    })

    config("spark.jars", (options.get("spark.jars").map {
      _.split(',').map(_.trim).filter(_.nonEmpty)
    }.toSeq.flatten ++ jars).distinct.mkString(","))

    val classServer = new AmmoniteClassServer(
      InetAddress.getLocalHost.getHostAddress,
      "0.0.0.0",
      AmmoniteClassServer.randomPort(),
      replApi.sess.frames
    )

    config("spark.repl.class.uri", classServer.uri.toString.stripSuffix("/") + "/")

    val session = super.getOrCreate()

    interpApi.beforeExitHooks += { v =>
      if (!session.sparkContext.isStopped)
        session.sparkContext.stop()
      v
    }

    session.sparkContext.addSparkListener(
      new SparkListener {
        override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
          classServer.stop()
        }
      }
    )

    session
  }
}