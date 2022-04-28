package org.archive.webservices.sparkling.budget

import java.io.InputStream

import org.apache.spark.rdd.RDD
import org.archive.webservices.sparkling.Sparkling
import org.archive.webservices.sparkling.io.{HdfsIO, IOUtil}
import org.archive.webservices.sparkling.util.{IteratorUtil, RddUtil}

import scala.reflect.ClassTag
import scala.util.Try

object BudgetAwareRddLoader {
  import Sparkling._

  def load[D: ClassTag](path: String, budget: Long, getRecords: InputStream => TraversableOnce[D])(getRecordCost: D => Long): RDD[D] = {
    val costs = BudgetUtil.computeFileCosts(path, getRecords)(getRecordCost)
    val totalCosts = costs.values.sum
    val filePartitions = costs.map { case (f, c) => (f, (c.toDouble / budget).ceil.toInt) }
    val filePartitionsBc = sc.broadcast(filePartitions)
    val maxPartitions = filePartitions.values.sum
    Sparkling.initPartitions(RddUtil.parallelize(maxPartitions)).flatMap { idx =>
      val filePartitions = filePartitionsBc.value
      val files = filePartitions.keys.toSeq.sorted

      var currentPartition = -1
      var currentCost = budget
      var currentMaxPartition = -1
      var streams = Seq.empty[InputStream]
      var break = false
      IteratorUtil.cleanup(
        files.toIterator.flatMap { file =>
          val in = HdfsIO.open(file)
          streams :+= in
          IteratorUtil.cleanup(
            getRecords(in).toIterator,
            () => {
              currentMaxPartition += filePartitions(file)
              in.close()
            }
          )
        }.dropWhile { record =>
          if (currentMaxPartition > currentPartition && maxPartitions - currentMaxPartition + currentPartition >= idx) {
            break = true
            false
          } else {
            val cost = Try { getRecordCost(record) }.getOrElse(0L)
            if (currentCost != 0 && currentCost + cost > budget) {
              currentPartition += 1
              currentCost = 0
            }
            if (currentPartition == idx) false
            else {
              currentCost += cost
              true
            }
          }
        }.takeWhile { record =>
          if (break) false
          else {
            val cost = Try { getRecordCost(record) }.getOrElse(0L)
            val take = currentCost == 0 || currentCost + cost <= budget
            currentCost += cost
            take
          }
        },
        () => streams.foreach(s => Try { s.close() })
      )
    }
  }

  def loadTextLines(path: String, budget: Long)(getRecordCost: String => Long): RDD[String] = loadTextLines(path, budget, s => s)(getRecordCost)

  def loadTextLines[D: ClassTag](path: String, budget: Long, getRecord: String => D)(getRecordCost: D => Long): RDD[D] =
    load(path, budget, IOUtil.lines(_).flatMap { l => Try { getRecord(l) }.toOption })(getRecordCost)
}
