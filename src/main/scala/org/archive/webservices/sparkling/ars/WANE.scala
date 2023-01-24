package org.archive.webservices.sparkling.ars

import java.util.Properties

import edu.stanford.nlp.pipeline._
import io.circe.syntax._
import io.circe.{Json, parser}
import org.archive.webservices.sparkling.html.HtmlProcessor
import org.archive.webservices.sparkling.util.RegexUtil
import org.archive.webservices.sparkling.warc.WarcRecord

import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap

object WANE {
  lazy val pipeline: StanfordCoreNLP = {
    val props = new Properties
    props.setProperty("annotators", "tokenize,ssplit,ner")
    props.setProperty("ner.useSUTime", "false")
    props.setProperty("ner.applyNumericClassifiers", "false")
    props.setProperty("ner.applyFineGrained", "false")
    new StanfordCoreNLP(props)
  }

  lazy val fineGrainedPipeline: StanfordCoreNLP = {
    val props = new Properties
    props.setProperty("annotators", "tokenize,ssplit,pos,lemma,ner")
    props.setProperty("ner.useSUTime", "false")
    props.setProperty("ner.applyNumericClassifiers", "false")
    new StanfordCoreNLP(props)
  }

  case class WaneRecord(url: String, timestamp: String, digest: String, locations: Set[String], organizations: Set[String], persons: Set[String]) {
    def toJson: Json = ListMap(
      "url" -> url.asJson,
      "timestamp" -> timestamp.asJson,
      "digest" -> digest.asJson,
      "named_entities" -> ListMap("locations" -> locations.toSeq.sorted, "organizations" -> organizations.toSeq.sorted, "persons" -> persons.toSeq.sorted).asJson
    ).asJson
    def toJsonString(pretty: Boolean): String = if (pretty) toJson.spaces4 else toJson.noSpaces
    def toJsonString: String = toJsonString(pretty = false)
  }

  def get(warc: WarcRecord, maxContentLength: Long = -1, maxInputTextLength: Int = -1, pipeline: StanfordCoreNLP = pipeline): Option[WaneRecord] = {
    if (maxContentLength < 0 || warc.contentLength.exists(_ <= maxContentLength)) {
      for {
        url <- warc.url
        timestamp <- warc.timestamp
        digest <- warc.payloadDigest
        http <- warc.http if http.status == 200 && http.mime.contains("text/html")
        html <- HtmlProcessor.strictHtml(http.bodyString)
      } yield get(url, timestamp, digest, {
        val body = RegexUtil.oneLineSpaceTrim(HtmlProcessor.fastBodyText(html))
        if (maxInputTextLength < 0) body else body.take(maxInputTextLength)
      }, pipeline)
    } else None
  }

  def get(url: String, timestamp: String, digest: String, text: String): WaneRecord = get(url, timestamp, digest, text, pipeline)

  def get(url: String, timestamp: String, digest: String, text: String, pipeline: StanfordCoreNLP): WaneRecord = {
    val recognized = entities(text, pipeline)
    WaneRecord(
      url,
      timestamp,
      digest,
      recognized.getOrElse("LOCATION", Set.empty),
      recognized.getOrElse("ORGANIZATION", Set.empty),
      recognized.getOrElse("PERSON", Set.empty)
    )
  }

  def parse(json: String): Option[WaneRecord] = {
    parser.parse(json).right.toOption.map(_.hcursor).flatMap { j =>
      val entites = j.downField("named_entities")
      for {
        url <- j.get[String]("url").toOption
        timestamp <- j.get[String]("timestamp").toOption
        digest <- j.get[String]("digest").toOption
        locations <- entites.get[Set[String]]("locations").toOption
        organizations <- entites.get[Set[String]]("organizations").toOption
        persons <- entites.get[Set[String]]("persons").toOption
      } yield WaneRecord(url, timestamp, digest, locations, organizations, persons)
    }
  }

  def cleanup(mention: String): Option[String] = Some(mention).filter(_ != null).map(_.trim).filter { m =>
    m.nonEmpty && !m.exists(_ < ' ')
  }

  def entities(text: String, pipeline: StanfordCoreNLP = pipeline): Map[String, Set[String]] =
    try {
      val doc = new CoreDocument(text)
      pipeline.annotate(doc)
      if (doc.entityMentions != null) {
        doc.entityMentions.asScala.flatMap(m => cleanup(m.text).map((m.entityType, _))).groupBy(_._1).map { case (entityType, group) =>
          entityType -> group.map(_._2).toSet
        }
      } else Map.empty
    } catch {
      case _: Error     => Map.empty
      case _: Exception => Map.empty
    }
}
