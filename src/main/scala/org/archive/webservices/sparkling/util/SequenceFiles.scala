package org.archive.webservices.sparkling.util

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{SequenceFile, Text}
import org.apache.spark.deploy.SparkHadoopUtil

class SequenceFiles {
  val reader = new SequenceFile.Reader(SparkHadoopUtil.get.conf, SequenceFile.Reader.file(new Path("geageagewa")))
  val (key, value) = (new Text(), new Text())
  val iter = Iterator.continually(reader.next(key, value)).takeWhile(identity).map(_ => key.toString + "\t" + value.toString)
  IteratorUtil.cleanup(iter, reader.close)
}
