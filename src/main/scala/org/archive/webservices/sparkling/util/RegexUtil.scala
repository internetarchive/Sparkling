package org.archive.webservices.sparkling.util

import scala.util.matching.Regex

object RegexUtil {
  private val patternCache = CollectionUtil.concurrentMap[String, Regex]

  def r(pattern: String): Regex = patternCache.getOrElseUpdate(pattern, pattern.r)

  lazy val urlStartPattern: Regex = r("[a-z]+\\:.*")
  lazy val noWordCharacterPattern: Regex = r("[^\\p{L}\\p{M}]+")
  lazy val oneLineSpacePattern: Regex = r("\\s+")
  lazy val newLineSpacePattern: Regex = "\\s*?\\n(\\s*)|\\s+".r
  lazy val tokenDelimiterPattern: Regex = r("[^\\p{L}\\p{M}0-9]+")
  lazy val nonLatinPattern: Regex = r("[^\\p{IsLatin}0-9\\s.,!?'\"()-]+")

  def matchesAbsoluteUrlStart(url: String): Boolean = urlStartPattern.pattern.matcher(url.toLowerCase).matches
  def oneValidWordCharacter(str: String): Boolean = str.nonEmpty && !noWordCharacterPattern.pattern.matcher(str).matches
  def oneLineSpaceTrim(str: String): String = oneLineSpacePattern.replaceAllIn(str, " ").trim
  def newLineSpaceTrim(str: String): String = {
    newLineSpacePattern.replaceAllIn(str, { m =>
      if (m.group(0).contains("\n")) {
        if (Option(m.group(1)).exists(_.contains("\n"))) "\n\n" else "\n"
      } else " "
    }).trim
  }
  def tokenize(str: String): Array[String] = tokenDelimiterPattern.replaceAllIn(str.toLowerCase, " ").trim.split(' ')
  def removeLongWords(str: String, maxWordLength: Int = 100): String = {
    if (maxWordLength < 0) str else r(s"\\S{$maxWordLength,}").replaceAllIn(str, "")
  }
  def cleanLatin(str: String, stripSpaces: Boolean = true, maxWordLength: Int = 100): String = {
    val clean = nonLatinPattern.replaceAllIn(removeLongWords(str, maxWordLength), "").trim
    if (stripSpaces) newLineSpaceTrim(clean) else clean
  }

  def split(str: String, pattern: String, limit: Int = -1): Seq[String] = {
    if (str.isEmpty) return Seq(str)
    if (limit < 1) r(pattern).split(str).toSeq
    else {
      val p = r(pattern)
      var remaining = str
      var count = 1
      var eof = false
      IteratorUtil.whileDefined {
        if (eof) None
        else {
          if (count == limit) {
            eof = true
            Some(remaining)
          } else if (remaining.nonEmpty) Some {
            p.findFirstMatchIn(remaining) match {
              case Some(m) =>
                count += 1
                remaining = m.after.toString
                m.before.toString
              case None =>
                eof = true
                remaining
            }
          }
          else None
        }
      }.toList
    }
  }
}
