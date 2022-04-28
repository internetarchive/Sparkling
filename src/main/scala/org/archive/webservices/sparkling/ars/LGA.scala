package org.archive.webservices.sparkling.ars

import io.circe.syntax._
import io.circe.{Json, parser}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.archive.webservices.sparkling.Sparkling
import org.archive.webservices.sparkling.html.{HtmlProcessor, LinkExtractor}
import org.archive.webservices.sparkling.http.HttpMessage
import org.archive.webservices.sparkling.io.IOUtil
import org.archive.webservices.sparkling.util.{IteratorUtil, RddUtil, SurtUtil}
import org.archive.webservices.sparkling.warc.WarcRecord

import scala.collection.immutable.ListMap
import scala.util.Try

object LGA {
  Sparkling.registerClasses(classOf[LgaLink], classOf[LgaNode], classOf[LgaLabel])

  case class LgaLabel(surt: String, url: String)

  case class LgaLink(src: LgaLabel, timestamp: String, dst: LgaLabel)

  case class LgaNode(label: LgaLabel, id: Long) {
    def toJson: Json = ListMap("surt_url" -> label.surt.asJson, "url" -> label.url.asJson, "id" -> id.asJson).asJson
    def toJsonString(pretty: Boolean): String = if (pretty) toJson.spaces4 else toJson.noSpaces
    def toJsonString: String = toJsonString(pretty = false)
  }

  case class LgaAdjacencyList(srcId: Long, timestamp: String, dstIds: Set[Long]) {
    def toJson: Json = ListMap("id" -> srcId.asJson, "timestamp" -> timestamp.asJson, "outlink_ids" -> dstIds.toSeq.sorted.asJson).asJson
    def toJsonString(pretty: Boolean): String = if (pretty) toJson.spaces4 else toJson.noSpaces
    def toJsonString: String = toJsonString(pretty = false)
  }

  def warcToMap(rdd: RDD[WarcRecord]): RDD[LgaNode] = parsedToMap(parse(rdd))

  def parsedToMap(parsed: RDD[((LgaLabel, String), Iterator[LgaLabel])]): RDD[LgaNode] = {
    parsedStringsToMap(parsed.map { case ((src, ts), dsts) => ((src.surt, src.url, ts), dsts.map(dst => (dst.surt, dst.url))) })
  }

  def parsedStringsToMap(parsed: RDD[((String, String, String), Iterator[(String, String)])]): RDD[LgaNode] = {
    val numPartitions = parsed.getNumPartitions

    val surtFrequencies = parsed.flatMap { case ((srcSurt, srcUrl, _), dsts) =>
      val url = srcUrl
      Iterator((srcSurt, (1L, url))) ++ dsts.map { case (dstSurt, dstUrl) => (dstSurt, (1L, dstUrl)) }
    }.reduceByKey((a, b) => (a, b) match { case ((c1, u1), (c2, u2)) => (c1 + c2, if (u1.length < u2.length) u1 else u2) }, numPartitions)

    val hosts = surtFrequencies.flatMap { case (surt, (count, url)) => SurtUtil.validateHost(surt).map((_, (surt, count, url))) }

    val surtHostFrequencies = RddUtil.repartitionAndSortWithinPartitionsBy(hosts, numPartitions)(_._1).mapPartitions { surtCounts =>
      IteratorUtil.groupSortedBy(surtCounts)(_._1).flatMap { case (host, hostUrls) =>
        var hostCount = 0L
        val buffer = IOUtil.buffer { out =>
          val print = IOUtil.print(out)
          try {
            for ((surt, count, url) <- hostUrls.map(_._2)) {
              hostCount += count
              print.println(Seq(surt, count.toString, url).mkString("\t"))
            }
          } finally { print.close() }
        }
        IteratorUtil.cleanup(
          IOUtil.lines(buffer.get.get).flatMap { str =>
            val split = str.split("\t")
            if (split.length == 3) Some((split(0), split(1).toLong, split(2), host, hostCount)) else None
          },
          () => buffer.clear(false)
        )
      }
    }

    val sorted = surtHostFrequencies.sortBy({ case (surt, count, url, host, hostCount) => (-hostCount, host, -count, surt) }, numPartitions = numPartitions)
      .map { case (surt, count, url, host, hostCount) => (surt, url) }

    sorted.zipWithIndex.map { case ((surt, url), id) => LgaNode(LgaLabel(surt, url), id) }
  }

  def warcToGraph[R](rdd: RDD[WarcRecord], bodyString: HttpMessage => String = _.bodyString)(saveAndLoadMap: RDD[LgaNode] => RDD[LgaNode])(graph: RDD[LgaAdjacencyList] => R): R = {
    val parsed = parse(rdd, bodyString).persist(StorageLevel.DISK_ONLY)
    val r = parsedToGraph(parsed)(saveAndLoadMap)(graph)
    parsed.unpersist(true)
    r
  }

  def parsedToGraph[R](parsed: RDD[((LgaLabel, String), Iterator[LgaLabel])])(saveAndLoadMap: RDD[LgaNode] => RDD[LgaNode])(graph: RDD[LgaAdjacencyList] => R): R = {
    val numPartitions = parsed.getNumPartitions

    val flat = parsed.flatMap { case ((src, ts), dsts) => dsts.map(dst => (src.surt, src.url, ts, dst.surt, dst.url)) }
    val map = parsedStringsToMap(flat.mapPartitions { tuples =>
      IteratorUtil.groupSortedBy(tuples) { case (surt, url, ts, _, _) => (surt, url, ts) }.map { case (src, dsts) => (src, dsts.map { case (_, _, _, surt, url) => (surt, url) }) }
    })
    val surtIds = saveAndLoadMap(map).map(n => (n.label.surt, n.id))
    val srcMapped = flat.map { case (srcSurt, _, ts, dstSurt, _) => (srcSurt, (ts, dstSurt)) }.join(surtIds, numPartitions).map { case (_, ((ts, dst), srcId)) => ((srcId, ts), dst) }
    val dstMapped = srcMapped.map { case ((src, ts), dst) => (dst, (ts, src)) }.join(surtIds, numPartitions).map { case (_, ((ts, srcId), dstId)) => ((srcId, ts), dstId) }
    val r = graph(dstMapped.aggregateByKey(Set.empty[Long])((dsts, dstId) => dsts + dstId, (dsts1, dsts2) => dsts1 ++ dsts2).map { case ((srcId, timestamp), dstIds) =>
      LgaAdjacencyList(srcId, timestamp, dstIds)
    })
    r
  }

  def parseNode(json: String): Option[LgaNode] = {
    parser.parse(json).right.toOption.map(_.hcursor).flatMap { j =>
      for {
        surt <- j.get[String]("surt_url").toOption
        url <- j.get[String]("url").toOption
        id <- j.get[Long]("id").toOption
      } yield LgaNode(LgaLabel(surt, url), id)
    }
  }

  def parseGraph(json: String): Option[LgaAdjacencyList] = {
    parser.parse(json).right.toOption.map(_.hcursor).flatMap { j =>
      for {
        srcId <- j.get[Long]("id").toOption
        timestamp <- j.get[String]("timestamp").toOption
        dstIds <- j.get[Set[Long]]("outlink_ids").toOption
      } yield LgaAdjacencyList(srcId, timestamp, dstIds)
    }
  }

  def parse(rdd: RDD[WarcRecord], bodyString: HttpMessage => String = _.bodyString): RDD[((LgaLabel, String), Iterator[LgaLabel])] = {
    rdd.flatMap { warc =>
      for {
        url <- warc.url
        surt <- SurtUtil.validate(url)
        timestamp <- warc.timestamp
        http <- warc.http if http.status == 200 && http.mime.contains("text/html")
        links <- Try(links(warc, bodyString)).toOption
      } yield { ((LgaLabel(surt, url), timestamp), links.map(_.dst)) }
    }
  }

  def links(warc: WarcRecord, bodyString: HttpMessage => String = _.bodyString): Iterator[LgaLink] = {
    for {
      url <- warc.url
      surt <- SurtUtil.validate(url)
      timestamp <- warc.timestamp
      http <- warc.http if http.status == 200 && http.mime.contains("text/html")
      html <- HtmlProcessor.strictHtml(bodyString(http))
    } yield {
      val src = LgaLabel(surt, url)
      val dstUrls = LinkExtractor.outLinks(html, Some(url)).flatMap { u => SurtUtil.validate(u).map((_, u)) }.toSeq.sortBy { case (dstSurt, dstUrl) => (dstSurt, dstUrl.length, dstUrl) }.toIterator
      IteratorUtil.firstOfSorted(dstUrls)(_._1).map { case (dstSurt, dstFullUrl) => LgaLink(src, timestamp, LgaLabel(dstSurt, dstFullUrl)) }
    }
  }.toIterator.flatten
}
