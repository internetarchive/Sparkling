package org.archive.webservices.sparkling.html

import java.net.URL

import org.archive.webservices.sparkling.html.HtmlProcessor.TagMatch
import org.archive.webservices.sparkling.util.{RegexUtil, StringUtil}

import scala.util.Try
import scala.util.matching.Regex

// cf. https://github.com/internetarchive/webarchive-commons/blob/master/src/main/java/org/archive/resource/html/ExtractingParseObserver.java
object LinkExtractor {
  val BaseTag = "base"
  val LinkTags: Set[String] = Set("a", "form", "area")
  val ContentEmbedTags: Set[String] = Set("applet", "embed", "frame", "iframe", "img", "input", "object", "source")
  val EmbedTags: Set[String] = ContentEmbedTags ++ Set("link", "script")
  val ValidJsUrlBeginChars: Set[Int] = "abcdefghijklmnopqrstuvwxyz_0123456789".map(_.toInt).toSet
  val CssUrlPattern: Regex = """url\s*\(\s*([\\"']*.+?[\\"']*)\s*\)""".r
  val CssImportUrlPattern: Regex = """@import\s+(('[^']+')|("[^"]+")|(\('[^']+'\))|(\("[^"]+"\))|(\([^)]+\))|([a-z0-9_.:/\\-]+))\s*;""".r

  def baseUrl(html: String, url: Option[String] = None): Option[String] = {
    HtmlProcessor.tag(html, BaseTag).buffered.headOption.flatMap(HtmlProcessor.attributeValue(_, "href")).flatMap(resolveLink(_, url)).orElse(url)
  }

  def isValidLinkProtocol(url: String): Boolean = {
    val lowerCase = url.trim.toLowerCase
    lowerCase.startsWith("http:") || lowerCase.startsWith("https:") || lowerCase.startsWith("ftp:")
  }

  def resolveLink(url: String, base: Option[String] = None): Option[String] = {
    if (RegexUtil.matchesAbsoluteUrlStart(url)) {
      if (isValidLinkProtocol(url)) Try(new URL(url.trim).toString.trim).toOption
      else None
    } else base match {
      case Some(b) =>
        if (isValidLinkProtocol(b)) Try(new URL(new URL(b.trim), url.trim).toString.trim).toOption
        else None
      case None => None
    }
  }.map(_.replace(" ", "%20").replace("\n", "%0A").replace("\r", "%0D").replace("\t", "%09"))

  def tagTargets(tag: TagMatch, url: Option[String] = None): Set[String] = {
    tag.name match {
      case "a" =>
        HtmlProcessor.attributeValue(tag.attributes, "href").flatMap(resolveLink(_, url)).toIterator
      case "form" =>
        HtmlProcessor.attributeValue(tag.attributes, "action").flatMap(resolveLink(_, url)).toIterator
      case "applet" =>
        val files = HtmlProcessor.attributeValue(tag.attributes, "archive").map(_.split("[ ,]").toSeq).getOrElse {
          HtmlProcessor.attributeValue(tag.attributes, "code").orElse {
            HtmlProcessor.attributeValue(tag.attributes, "object")
          }.toSeq
        }
        val codebase = HtmlProcessor.attributeValue(tag.attributes, "codebase").map(resolveLink(_, url)).getOrElse(url)
        files.flatMap(resolveLink(_, codebase))
      case "object" =>
        val files = HtmlProcessor.attributeValue(tag.attributes, "archive").map(_.split("[ ,]").toSeq).getOrElse {
          HtmlProcessor.attributeValue(tag.attributes, "data").orElse {
            HtmlProcessor.attributeValue(tag.attributes, "classid")
          }.toSeq
        }
        val codebase = HtmlProcessor.attributeValue(tag.attributes, "codebase").map(resolveLink(_, url)).getOrElse(url)
        files.flatMap(resolveLink(_, codebase))
      case "area" =>
        HtmlProcessor.attributeValue(tag.attributes, "href").flatMap(resolveLink(_, url)).toIterator
      case "embed" =>
        HtmlProcessor.attributeValue(tag.attributes, "src").flatMap(resolveLink(_, url)).toIterator
      case "frame" =>
        HtmlProcessor.attributeValue(tag.attributes, "src").flatMap(resolveLink(_, url)).toIterator
      case "iframe" =>
        HtmlProcessor.attributeValue(tag.attributes, "src").flatMap(resolveLink(_, url)).toIterator
      case "img" =>
        HtmlProcessor.attributeValue(tag.attributes, "src").flatMap(resolveLink(_, url)).toIterator
      case "input" =>
        HtmlProcessor.attributeValue(tag.attributes, "src").flatMap(resolveLink(_, url)).toIterator
      case "source" =>
        HtmlProcessor.attributeValue(tag.attributes, "src").flatMap(resolveLink(_, url)).toIterator
      case "link" =>
        if (HtmlProcessor.attributeValue(tag.attributes, "rel").map(_.toLowerCase).contains("stylesheet")) {
          HtmlProcessor.attributeValue(tag.attributes, "href").flatMap(resolveLink(_, url)).toIterator
        } else Iterator.empty
      case "script" =>
        HtmlProcessor.attributeValue(tag.attributes, "src").flatMap(resolveLink(_, url)).toIterator
      case _ => Iterator.empty
    }
  }.toSet

  def outLinksWithChildren(html: String, url: Option[String] = None): Iterator[(String, TagMatch, Seq[TagMatch])] = {
    val base = baseUrl(html, url)
    HtmlProcessor.tagsWithChildren(html, LinkTags).flatMap { case (tag, children) =>
      tagTargets(tag, base).map((_, tag, children))
    }
  }

  def outLinks(html: String, url: Option[String] = None): Iterator[String] = {
    val base = baseUrl(html, url)
    HtmlProcessor.tags(html, LinkTags).flatMap(tagTargets(_, base))
  }

  def outLinksWithText(html: String, url: Option[String] = None): Iterator[(String, String, TagMatch)] = outLinksWithChildren(html, url).map { case (target, tag, children) =>
    (target, if (tag.name == "a") HtmlProcessor.text(children) else "", tag)
  }

  def outLinksHandler(html: String, url: Option[String] = None): TagHandler[Seq[(String, String, TagMatch)]] = {
    val base = baseUrl(html, url)
    TagHandler(LinkTags, Seq.empty[(String, String, TagMatch)]) { (tag, children, result) =>
      result ++ tagTargets(tag, base).map((_, if (tag.name == "a") HtmlProcessor.text(children) else "", tag))
    }
  }

  def targetLabelHandler(html: String, url: Option[String] = None, tag: String, targetAttribute: String, labelAttributes: Set[String], includeText: Boolean = true): TagHandler[Set[(String, String)]] = {
    val base = baseUrl(html, url)
    TagHandler(Set(tag), Set.empty[(String, String)], hierarchical = false) { (tag, children, result) =>
      val targetUrls = HtmlProcessor.attributeValue(tag.attributes, targetAttribute).flatMap(LinkExtractor.resolveLink(_, base)).toSet
      result ++ targetUrls.flatMap { dst =>
        labelAttributes.flatMap { attribute =>
          HtmlProcessor.attributeValue(tag.attributes, attribute).map(alt => (dst, alt))
        } ++ (if (includeText) Set((dst, HtmlProcessor.text(children))) else Set.empty)
      }
    }
  }

  def cssEmbeds(css: String, url: Option[String] = None): Set[String] = {
    for (m <- CssUrlPattern.findAllMatchIn(css) ++ CssImportUrlPattern.findAllMatchIn(css)) yield {
      resolveLink(StringUtil.stripBrackets(StringUtil.stripBracket(m.group(1), "(", ")"), Seq("\\", "\"", "'")), url)
    }
  }.flatten.toSet

  def jsStringUrls(script: String, url: Option[String] = None): Set[String] = {
    var prevDoubleQuote = -1
    val doubleQuoted = Iterator.continually {
      prevDoubleQuote = script.indexOf('"', prevDoubleQuote + 1)
      prevDoubleQuote
    }.takeWhile(_ != -1).sliding(2).map{case Seq(a,b) => script.substring(a + 1, b)}.filter(_.length > 2)

    var prevSingleQuote = -1
    val singleQuoted = Iterator.continually {
      prevSingleQuote = script.indexOf('\'', prevSingleQuote + 1)
      prevSingleQuote
    }.takeWhile(_ != -1).sliding(2).map{case Seq(a,b) => script.substring(a + 1, b)}.filter(_.length > 2)

    for (candidate <- doubleQuoted ++ singleQuoted) yield {
      if (candidate.contains(" ")) None
      else {
        val slash = candidate.indexOf('/')
        val query = candidate.indexOf('?')
        val fragment = candidate.indexOf("#")
        val dot = candidate.indexOf(".")
        if (slash != -1 || query != -1 || fragment != -1 || dot != -1) {
          if (ValidJsUrlBeginChars.contains(candidate.head.toLower.toInt) || ((slash == 0 || query == 0 || fragment == 0) && ValidJsUrlBeginChars.contains(candidate(1).toLower.toInt))) {
            resolveLink(candidate, url)
          } else None
        } else None
      }
    }
  }.flatten.toSet

  def embeds(html: String, url: Option[String] = None, includeJsStringUrls: Boolean = false): Set[String] = {
    val handler = embedsHandler(html, url, includeJsStringUrls = includeJsStringUrls)
    HtmlProcessor.process(html, Set(handler))
    handler.result
  }

  def embedsHandler(html: String, url: Option[String] = None, tags: Set[String] = EmbedTags, filterTag: TagMatch => Boolean = _ => true, filterUrl: String => Boolean = _ => true, includeJsStringUrls: Boolean = false): TagHandler[Set[String]] = {
    val base = baseUrl(html, url)
    TagHandler.all(Set.empty[String], handleClosing = true) { (tag, _, result) =>
      result ++ {
        if (filterTag(tag)) {
          if (tag.opening) {
            {
              HtmlProcessor.attributeValue(tag.attributes, "background").flatMap(resolveLink(_, base))
            } ++ {
              HtmlProcessor.attributeValue(tag.attributes, "style").toIterator.flatMap(cssEmbeds(_, base))
            } ++ {
              if (EmbedTags.contains(tag.name)) tagTargets(tag, base)
              else Iterator.empty
            }
          } else if (tag.closingTag) {
            if (tag.name == "style") cssEmbeds(tag.text, base)
            else if (includeJsStringUrls && tag.name == "script") jsStringUrls(tag.text, base)
            else Iterator.empty
          } else Iterator.empty
        } else Iterator.empty
      }.filter(filterUrl)
    }
  }

  def contentEmbedsHandler(html: String, url: Option[String] = None, minContentTextLength: Int = 30): TagHandler[Set[String]] = {
    var inContent = false
    embedsHandler(html, url, ContentEmbedTags, { tag =>
      if (inContent) true
      else {
        if (tag.closingTag && !HtmlProcessor.EscapeTags.contains(tag.name)) inContent = tag.text.trim.length >= minContentTextLength
        inContent
      }
    }, { url =>
      val fileUrl = RegexUtil.split(url, "[\\?\\#]", 2).head.toLowerCase
      !fileUrl.endsWith(".js") && !fileUrl.endsWith(".css")
    })
  }
}
