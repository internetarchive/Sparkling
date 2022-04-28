package org.archive.webservices.sparkling.util

import java.net.InetAddress

import ammonite.interp.api.InterpAPI
import ammonite.repl.api.ReplAPI
import coursierapi.Dependency
import org.apache.spark.sql.ammonitesparkinternals.AmmoniteClassServer
import org.archive.webservices.sparkling.Sparkling

// cp. https://github.com/alexarchambault/ammonite-spark/blob/b33e9145fda4a13d9020cef13bb461e65634ae39/modules/core/src/main/scala/org/apache/spark/sql/ammonitesparkinternals/AmmoniteSparkSessionBuilder.scala
object AmmoniteUtil {
  def classServer(repl: ReplAPI): AmmoniteClassServer = {
    val host = InetAddress.getLocalHost.getHostAddress
    new AmmoniteClassServer(host, host, AmmoniteClassServer.randomPort(), repl.sess.frames)
  }

  def initSparkYarn(implicit interp: InterpAPI, repl: ReplAPI): SparkUtil.SparkReplClassServer = {
    interp.load.ivy(Dependency.of("org.apache.spark", "spark-yarn_" + Sparkling.ScalaVersion, Sparkling.SparkVersion)) // import $ivy.`org.apache.spark::spark-yarn:2.4.7`
//    interp.load.ivy(Dependency.of("org.apache.spark", "spark-sql_" + Sparkling.ScalaVersion, Sparkling.SparkVersion)) // import $ivy.`org.apache.spark::spark-sql:2.4.7`
    interp.load.cp(ammonite.ops.Path(sys.env("HADOOP_CONF_DIR")))
    val server = classServer(repl)
    new SparkUtil.SparkReplClassServer {
      override def stop(): Unit = server.stop()
      override def uri: String = server.uri.toString
    }
  }
}
