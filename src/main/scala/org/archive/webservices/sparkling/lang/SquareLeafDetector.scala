package org.archive.webservices.sparkling.lang

import org.archive.webservices.sparkling.html.HtmlProcessor
import org.archive.webservices.sparkling.util.{IteratorUtil, RegexUtil}

object SquareLeafDetector {
  val MinTerms = 10

  def detect(html: String)(detect: String => Seq[(String, Double, Boolean, Double)]): Seq[(String, Double)] = {
    val texts = HtmlProcessor.iterateTags(html).map(t => HtmlProcessor.text(Some(t)).trim).filter(_.nonEmpty)
    val textTerms = texts.map(t => (t, RegexUtil.tokenize(t).length)).filter(_._2 > 0).buffered
    val combined = IteratorUtil.whileDefined {
      if (textTerms.hasNext) Some {
        if (textTerms.head._2 >= MinTerms) textTerms.next
        else IteratorUtil.takeWhile(textTerms)(_._2 < MinTerms).reduce((a, b) => (a, b) match { case ((t1, c1), (t2, c2)) => (t1 + " " + t2, c1 + c2) })
      }
      else None
    }.filter(_._2 >= MinTerms)
    var totalWeight = 0.0
    combined.flatMap { case (text, terms) =>
      val weight = Math.pow(terms, 2)
      totalWeight += weight
      detect(text).filter { case (_, _, r, _) => r }.map { case (lang, prob, reliable, proportion) => (lang, weight * proportion) }
    }.toSeq.groupBy(_._1).toSeq.map { case (lang, records) => (lang, records.map(_._2).sum / totalWeight) }.sortBy(-_._2)
  }
}
