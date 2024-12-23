package org.archive.webservices.sparkling.html

import java.io.InputStream
import java.nio.charset.CodingErrorAction

import org.archive.webservices.sparkling.Sparkling
import org.archive.webservices.sparkling.Sparkling.prop
import org.archive.webservices.sparkling.util.{IteratorUtil, RegexUtil, StringUtil}

import scala.util.Try
import scala.util.matching.Regex

object HtmlProcessor {
  var maxHtmlStackDepth: Int = prop(500)(maxHtmlStackDepth, maxHtmlStackDepth = _) // empirically determined, amazon.com ~ 180
  var strictMode: Boolean = prop(true)(strictMode, strictMode = _)
  var stopAtMaxHtmlStackDepth: Boolean = prop(true)(stopAtMaxHtmlStackDepth, stopAtMaxHtmlStackDepth = _)

  val CodeTags: Set[String] = Set("script", "style")
  val EscapeTags: Set[String] = CodeTags ++ Set("textarea", "pre")
  val TagOpenClosePattern: Regex = """<([ /]*)([A-Za-z0-9_\-:!]+)([^>]*(>|$|\n))""".r
  val InTagPattern: Regex = """['">]""".r
  val CharsetHtmlPattern: Regex = "(encoding|charset)=[\\\" ]*([^\\\" ]+)".r
  val MaxHtmlStackDepthReachedMsg = "Max HTML stack depth reached."

  case class TagMatch(tag: String, name: String, opening: Boolean, closing: Boolean, attributes: String, text: String, stack: List[String]) {
    def closingTag: Boolean = closing && !opening
    def openingTag: Boolean = opening && !closing
    def selfClosingTag: Boolean = opening && closing
  }

  def strictHtml(html: String): Option[String] = Some(html.trim).filter(h => h.startsWith("<") && h.endsWith(">"))

  def removeTags(str: String, start: String, end: String): String = {
    val startLc = StringUtil.toLowerCase(start)
    val endLc = StringUtil.toLowerCase(end)
    var filtered = str
    var lowerCase = StringUtil.toLowerCase(filtered)
    var startIdx = lowerCase.indexOf(startLc)
    while (startIdx >= 0) {
      val endIdx = lowerCase.indexOf(endLc, startIdx)
      filtered = filtered.take(startIdx) + (if (endIdx < 0) "" else filtered.drop(endIdx + end.length))
      lowerCase = StringUtil.toLowerCase(filtered)
      startIdx = lowerCase.indexOf(startLc)
    }
    val prefixTag = lowerCase.indexOf(endLc)
    if (prefixTag < 0) filtered else filtered.drop(prefixTag + end.length)
  }

  def removeComments(text: String): String = removeTags(text, "<!--", "-->")

  def removeScripts(html: String): String = removeTags(html, "<script", "</script>")

  def findTagEnd(remainingHtml: String): Option[String] = {
    var remaining = remainingHtml
    var m = InTagPattern.findFirstMatchIn(remaining)
    if (m.isDefined) {
      var tag = ""
      while (m.isDefined) {
        val matched = m.get.matched
        val start = m.get.start
        if (matched == "\"" || matched == "'") {
          val to = remaining.indexOf(matched, start + 1)
          val idx = if (to < 0) start else to
          tag += remaining.take(idx + 1)
          remaining = remaining.drop(idx + 1)
          m = InTagPattern.findFirstMatchIn(remaining)
        } else if (matched == ">") {
          tag += remaining.take(start + 1)
          m = None
        }
      }
      Some(tag)
    } else None
  }

  def fastBodyText(html: String): String = {
    val lowerCase = StringUtil.toLowerCase(html)
    val headEnd = lowerCase.lastIndexOf("</head>")
    val bodyStart = if (headEnd < 0) lowerCase.indexOf("<body") else lowerCase.indexOf("<body", headEnd)
    if (bodyStart < 0) "" else {
      var remaining = removeScripts(html.drop(bodyStart))
      var inCode: Option[String] = None
      removeComments {
        IteratorUtil.whileDefined(TagOpenClosePattern.findFirstMatchIn(remaining)).map { tag =>
          val slash = tag.group(1).trim
          val name = tag.group(2).trim.toLowerCase
          val opening = !slash.startsWith("/")
          val (closing, tagEnd) = if (opening) {
            val attributesStart = tag.start(3)
            val remainingTag = findTagEnd(remaining.drop(attributesStart)).getOrElse(tag.group(3))
            val tagEnd = attributesStart + remainingTag.length
            val attributes = remainingTag.stripSuffix(">").trim
            if (attributes.endsWith("/")) (true, tagEnd) else (false, tagEnd)
          } else (!opening, tag.end)
          val text = remaining.take(tag.start)
          remaining = remaining.drop(tagEnd)
          if (inCode.isDefined) {
            if (!opening && closing && inCode.contains(name)) inCode = None
            ""
          } else {
            if (opening && !closing && EscapeTags.contains(name)) inCode = Some(name)
            text
          }
        }.mkString + remaining
      }
    }
  }

  def readStream(in: InputStream, charsets: Seq[String] = Seq(Sparkling.DefaultCharset), maxLineLength: Int = 4096 * 1024): String = {
    var c = charsets
    var cSet = c.toSet
    IteratorUtil.whileDefined {
      val line = StringUtil.readLineBytes(in, maxLength = maxLineLength)
      if (line == null) None else Some {
        val str = {
          c.toIterator.flatMap { charset => Try(StringUtil.fromBytes(line, StringUtil.codec(charset, CodingErrorAction.REPORT))).toOption } ++ IteratorUtil.getLazy { _ =>
            StringUtil.fromBytes(line, StringUtil.codec(c.headOption.getOrElse(Sparkling.DefaultCharset), CodingErrorAction.IGNORE))
          }
        }.next
        val newC = IteratorUtil.distinct(CharsetHtmlPattern.findAllMatchIn(str).map(_.group(2)).filter(!cSet.contains(_))).toSeq
        if (newC.nonEmpty) {
          c = newC ++ c
          cSet = c.toSet
        }
        str
      }
    }.mkString("\n")
  }

  def iterateTags(html: String): Iterator[TagMatch] = {
    (if (!strictMode) Some(html) else strictHtml(html)).map { strict =>
      var stack = List.empty[String]
      var inCode: Option[String] = None
      val tags = collection.mutable.Map.empty[String, Int]
      var maxHtmlStackDepthReached = false
      var remaining = strict
      val matches = IteratorUtil.whileDefined(TagOpenClosePattern.findFirstMatchIn(remaining))
      for (tag <- matches if !maxHtmlStackDepthReached) yield {
        if (maxHtmlStackDepthReached) None
        else {
          val slash = tag.group(1).trim
          val name = tag.group(2).trim.toLowerCase
          val opening = !slash.startsWith("/")
          val (attributes, closing, tagEnd) = if (opening && inCode.isEmpty) {
            val attributesStart = tag.start(3)
            val remainingTag = findTagEnd(remaining.drop(attributesStart)).getOrElse(tag.group(3))
            val tagEnd = attributesStart + remainingTag.length
            val attributes = remainingTag.stripSuffix(">").trim
            if (attributes.endsWith("/")) (attributes.dropRight(1).trim, true, tagEnd) else (attributes, false, tagEnd)
          } else ("", !opening, tag.end)
          if (inCode.isDefined && name == inCode.get && closing && !opening) inCode = None
          if (inCode.isEmpty) {
            if (!opening || !closing) {
              if (opening) {
                stack +:= name
                if (stack.size > maxHtmlStackDepth) {
                  maxHtmlStackDepthReached = true
                  println(MaxHtmlStackDepthReachedMsg)
                  if (stopAtMaxHtmlStackDepth) throw new RuntimeException(MaxHtmlStackDepthReachedMsg)
                }
                tags.update(name, tags.getOrElse(name, 0) + 1)
                if (EscapeTags.contains(name)) inCode = Some(name)
              } else if (tags.contains(name)) {
                val drop = stack.takeWhile(_ != name)
                stack = stack.drop(drop.size)
                for (dropTag <- drop ++ Iterator(stack.head)) {
                  val count = tags(dropTag)
                  if (count == 1) tags.remove(dropTag) else tags.update(dropTag, count - 1)
                }
                stack = stack.drop(1)
              }
            }
            val text = remaining.take(tag.start)
            remaining = remaining.drop(tagEnd)
            Some(TagMatch(tag.matched, name, opening, closing, attributes, removeComments(text), stack))
          } else {
            remaining = remaining.drop(tagEnd)
            None
          }
        }
      }
    }.getOrElse(Iterator.empty)
  }.flatten

  private def internalProcessTags(
      tags: BufferedIterator[TagMatch],
      handlers: Set[TagHandler[_]],
      outer: Boolean = true,
      inner: Boolean = true,
      ignoreHierarchical: Set[String] = Set.empty
  ): Iterator[(TagMatch, Seq[TagMatch])] = {
    if (!tags.hasNext) return Iterator.empty
    var iter = tags
    IteratorUtil.whileDefined {
      if (iter.hasNext) Some {
        val head = iter.head
        val name = head.name
        iter.next()
        if (head.opening) {
          val activeHandlers = handlers.filter(_.handles(name))
          if (activeHandlers.isEmpty) Iterator.empty
          else {
            val (hierarchicalHandlers, flatHandlers) = activeHandlers.partition(_.hierarchical)
            for (handler <- flatHandlers) handler.handle(head, Seq.empty)
            if (hierarchicalHandlers.nonEmpty && !ignoreHierarchical.contains(name)) {
              if (head.closing) {
                for (handler <- hierarchicalHandlers) handler.handle(head, Seq.empty)
                Iterator((head, Seq.empty))
              } else {
                val lvl = head.stack.size
                val tail = IteratorUtil.takeUntil(iter, including = true) { t => t.stack.size < lvl }.toSeq
                val innerTags =
                  if (tail.isEmpty) Seq.empty
                  else {
                    val last = tail.last
                    val children = if (last.closingTag && last.name == name) tail.dropRight(1) else tail
                    internalProcessTags(children.toIterator.buffered, handlers, outer, inner, if (inner) ignoreHierarchical else ignoreHierarchical + name).toSeq
                  }
                if (outer || !innerTags.exists { case (t, _) => t.name == name }) {
                  for (handler <- hierarchicalHandlers) handler.handle(head, tail)
                  Iterator((head, tail)) ++ innerTags
                } else innerTags
              }
            } else if (flatHandlers.nonEmpty) Iterator((head, Seq.empty))
            else Iterator.empty
          }
        } else {
          val activeHandlers = handlers.filter(h => h.handleClosing && h.handles(name))
          if (activeHandlers.isEmpty) Iterator.empty
          else {
            for (handler <- activeHandlers) handler.handle(head, Seq.empty)
            Iterator((head, Seq.empty))
          }
        }
      }
      else None
    }
  }.flatten

  def lazyProcessTags(tags: TraversableOnce[TagMatch], handlers: Set[TagHandler[_]], outer: Boolean = true, inner: Boolean = true): Iterator[(TagMatch, Seq[TagMatch])] = {
    internalProcessTags(tags.toIterator.buffered, handlers, outer, inner)
  }

  def lazyProcess(html: String, handlers: Set[TagHandler[_]], outer: Boolean = true, inner: Boolean = true): Iterator[(TagMatch, Seq[TagMatch])] = {
    lazyProcessTags(iterateTags(html), handlers, outer, inner)
  }

  def processTags(tags: TraversableOnce[TagMatch], handlers: Set[TagHandler[_]], outer: Boolean = true, inner: Boolean = true): Int = { lazyProcessTags(tags, handlers, outer, inner).size }

  def process(html: String, handlers: Set[TagHandler[_]], outer: Boolean = true, inner: Boolean = true): Int = { lazyProcess(html, handlers, outer, inner).size }

  def encloseTags(tags: TraversableOnce[TagMatch], names: Set[String], outer: Boolean = true, inner: Boolean = true): Iterator[(TagMatch, Seq[TagMatch])] = {
    lazyProcessTags(tags, Set(TagHandler.noop(names)), outer, inner)
  }

  def text(children: TraversableOnce[TagMatch]): String = children.filter(t => !(EscapeTags.contains(t.name) && t.closing)).map(_.text).mkString

  def textHandler(tags: Set[String]): TagHandler[Seq[(TagMatch, String)]] = { TagHandler(tags, Seq.empty[(TagMatch, String)])((tag, children, r) => r ++ Seq((tag, text(children)))) }

  def childrenHandler(tags: Set[String]): TagHandler[Seq[(TagMatch, Seq[TagMatch])]] = { TagHandler(tags, Seq.empty[(TagMatch, Seq[TagMatch])])((tag, children, r) => r ++ Seq((tag, children))) }

  def tagsWithText(html: String, names: Set[String], outer: Boolean = true, inner: Boolean = true): Iterator[(TagMatch, String)] = tagsWithChildren(html, names, outer, inner)
    .map { case (tag, children) => (tag, text(children)) }

  def tagsWithChildren(html: String, names: Set[String], outer: Boolean = true, inner: Boolean = true): Iterator[(TagMatch, Seq[TagMatch])] = { encloseTags(iterateTags(html), names, outer, inner) }

  def printTags(html: String, names: Set[String], outer: Boolean = true, inner: Boolean = true): Iterator[String] = {
    tagsWithChildren(html, names, outer, inner).map { case (tag, children) => print(tag, children) }
  }

  def tags(html: String, names: Set[String]): Iterator[TagMatch] = {
    val lower = names.map(_.toLowerCase)
    iterateTags(html).filter { t => t.opening && lower.contains(t.name) }
  }

  def tag(html: String, name: String): Iterator[String] = RegexUtil.r(s"""(?i)< *$name(>| [^>]*>)""").findAllIn(html)

  def attributeValue(tag: String, attribute: String): Option[String] = { RegexUtil.r(s"""(?i)(^|\\W)$attribute *= *('[^']*'|"[^"]*")""").findFirstMatchIn(tag).map(_.group(2).drop(1).dropRight(1)) }

  def attributeValues(html: String, tagName: String, attribute: String): Iterator[String] = tag(html, tagName).flatMap { tag => attributeValue(tag, attribute) }

  def print(tag: TagMatch, children: TraversableOnce[TagMatch]): String = tag.tag + children.map(t => t.text + t.tag).mkString

  def print(tags: TraversableOnce[TagMatch]): String = {
    val iter = tags.toIterator
    if (iter.hasNext) print(iter.next, iter) else ""
  }
}
