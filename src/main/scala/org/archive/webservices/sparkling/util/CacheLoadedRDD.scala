package org.archive.webservices.sparkling.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, Partitioner, TaskContext}

import java.io.File
import scala.reflect.ClassTag

class CacheLoadedRDD[A : ClassTag](private var loaded: RDD[A], private var cached: RDD[_]) extends RDD[A](loaded) {
  override val partitioner: Option[Partitioner] = firstParent[String].partitioner

  override def compute(split: Partition, context: TaskContext): Iterator[A] = firstParent[A].compute(split, context)

  override protected def getPartitions: Array[Partition] = firstParent[String].partitions

  override def unpersist(blocking: Boolean): CacheLoadedRDD.this.type = {
    cached.unpersist(blocking)
    super.unpersist(blocking)
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    loaded = null
    cached = null
  }
}
