package org.archive.webservices.sparkling.budget

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD
import org.archive.webservices.sparkling.io.HdfsIO
import org.archive.webservices.sparkling.util.{PrimaryKeyPartitioner, RddUtil}

import scala.reflect.ClassTag
import scala.util.Try

object BudgetRddManager {
  import org.archive.webservices.sparkling.Sparkling._

  def repartitionByBudget[D: ClassTag](rdd: RDD[D], budget: Long, bufferSize: Int = 10000000)(getRecordCost: D => Long): RDD[D] = {
    var dstPartitions = Map.empty[Int, (Int, Long)]
    var currentPartition = 0
    var currentCost = 0L
    var prevSrcPartition = -1
    RddUtil.iterateAggregates(rdd.map(r => Try { getRecordCost(r) }.getOrElse(0L)), bufferSize) { records => records.sum }.foreach { aggregate =>
      val srcPartition = aggregate.records.partitionIdx
      val costSum = aggregate.value
      if (prevSrcPartition != srcPartition) {
        dstPartitions = dstPartitions.updated(srcPartition, (currentPartition, currentCost))
        prevSrcPartition = srcPartition
      }
      if (currentCost + costSum > budget) {
        for (cost <- aggregate.records.get) {
          if (currentCost != 0 && currentCost + cost > budget) {
            currentPartition += 1
            currentCost = 0
          }
          currentCost += cost
        }
      } else { currentCost += costSum }
    }
    val dstPartitionsBc = sc.broadcast(dstPartitions)
    rdd.mapPartitionsWithIndex { case (idx, records) =>
      val dstPartitions = dstPartitionsBc.value
      dstPartitions.get(idx) match {
        case Some((dst, dstCost)) =>
          var currentPartition = dst
          var currentCost = dstCost
          records.map { record =>
            val cost = Try { getRecordCost(record) }.getOrElse(0L)
            if (currentCost != 0 && currentCost + cost > budget) {
              currentPartition += 1
              currentCost = 0
            }
            currentCost += cost
            (currentPartition, record)
          }
        case None => Iterator.empty
      }
    }.partitionBy(new PrimaryKeyPartitioner(currentPartition + 1)).values
  }

  def reloadByBudget(rdd: RDD[String], budget: Long)(getRecordCost: String => Long)(action: RDD[String] => Unit): Unit = reloadByBudget(rdd, budget, (s: String) => s)(getRecordCost)(action)

  def reloadByBudget[D: ClassTag, R](rdd: RDD[String], budget: Long, getRecord: String => D)(getRecordCost: D => Long)(action: RDD[D] => R): R = {
    HdfsIO.tmpPath { tmpPath =>
      val path = tmpPath + "/dataset.gz"
      rdd.saveAsTextFile(path, classOf[GzipCodec])
      action(BudgetAwareRddLoader.loadTextLines(path, budget, getRecord)(getRecordCost))
    }
  }
}
