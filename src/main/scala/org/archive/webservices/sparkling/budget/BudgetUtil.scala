package org.archive.webservices.sparkling.budget

import java.io.InputStream

import org.apache.spark.rdd.RDD
import org.archive.webservices.sparkling.io.IOUtil
import org.archive.webservices.sparkling.util.RddUtil

import scala.reflect.ClassTag
import scala.util.Try

object BudgetUtil {
  def computeFileCosts[D: ClassTag](path: String, getRecords: InputStream => TraversableOnce[D])(getRecordCost: D => Long): collection.Map[String, Long] = {
    RddUtil.loadBinary(path) { (filename, in) =>
      val cost = getRecords(in).map(r => Try { getRecordCost(r) }.getOrElse(0L)).sum
      Some(filename, cost)
    }.collectAsMap
  }

  def computeTextFileCosts(path: String, getRecordCost: String => Long): collection.Map[String, Long] = computeFileCosts(path, IOUtil.lines(_))(getRecordCost)

  def computePartitionCosts[D: ClassTag](rdd: RDD[D])(getRecordCost: D => Long): collection.Map[Int, Long] = {
    rdd.mapPartitionsWithIndex { (idx, records) => Iterator((idx, records.map(r => Try { getRecordCost(r) }.getOrElse(0L)).sum)) }.collectAsMap
  }
}
