package org.archive.webservices.sparkling.cdx

import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.archive.webservices.sparkling.io.{HdfsBackedMap, HdfsIO, IOUtil}
import org.archive.webservices.sparkling.util._

object CdxServerIndexDistributable {
  def apply(indexDirPath: String): CdxServerIndexDistributable = {
    new CdxServerIndexDistributable(indexDirPath)
  }
}

class CdxServerIndexDistributable private (indexDirPath: String) extends Serializable {
  import org.archive.webservices.sparkling.Sparkling._

  lazy val blockSize: Long = HdfsIO.fs.getDefaultBlockSize(new Path(indexDirPath))

  private var _filesBroadcasted: Boolean = false
  private lazy val filesBc: Broadcast[Seq[String]] = sc.broadcast {
    _filesBroadcasted = true
    HdfsIO.files(new Path(indexDirPath, "part-*.gz").toString).map(_.split('/').last).toSeq.sorted
  }

  private var _cdxIndexBroadcasted: Boolean = false
  private lazy val cdxIndexBc: Broadcast[HdfsBackedMap[String]] = sc.broadcast {
    _cdxIndexBroadcasted = true
    HdfsBackedMap(new Path(indexDirPath, "part-*-idx").toString, key = StringUtil.prefixBySeparator(_, " "), cache = false, preloadLength = false)
  }

  private var _broadcasted: Boolean = false
  lazy val broadcast: Broadcast[CdxServerIndexDistributable] = synchronized {
    // ensure both files and index have been initialized
    _broadcasted = (filesBc.value.hashCode + cdxIndexBc.value.hashCode) != 0
    sc.broadcast(this)
  }

  def unpersist(blocking: Boolean = false): Unit = {
    if (_broadcasted) broadcast.unpersist(blocking)
    if (_filesBroadcasted) filesBc.unpersist(blocking)
    if (_cdxIndexBroadcasted) cdxIndexBc.unpersist(blocking)
  }

  def lookup(surt: String, timestamp: String = "", filter: CdxRecord => Boolean = _ => true): CleanupIterator[CdxRecord] = {
    val files = filesBc.value
    val cdxIndex = cdxIndexBc.value
    val lookup = cdxIndex.before(surt).iter {
      _.headOption.map(_._2.get).flatMap { idxLines =>
        idxLines.map(RegexUtil.split(_, "\t")).filter(_.length > 2).map { split =>
          (split(1) + ".gz", split(2).toLong)
        }.buffered.headOption
      }
    }
    val fileOffsets = lookup.toIterator ++ lookup.toIterator.flatMap { case (filename, _) =>
      files.dropWhile(_ <= filename).map((_, 0L))
    }
    val cdx = CleanupIterator.flatten(fileOffsets.map { case (f, o) =>
      val in = HdfsIO.open(new Path(indexDirPath, f).toString, offset = o, strategy = HdfsIO.LoadingStrategy.Remote)
      CleanupIterator(IOUtil.lines(in), in.close)
    }).chain {
      IteratorUtil.groupSortedBy(_)(StringUtil.prefixBySeparator(_, " "))
    }.chain {
      _.dropWhile(_._1 < surt).take(1).filter(_._1 == surt).flatMap(_._2).flatMap(CdxRecord.fromString).filter(filter)
    }
    if (timestamp.nonEmpty) {
      val ts = Time14Util.parse(timestamp).toInstant.getMillis
      var distance = -1L
      CleanupIterator {
        cdx.iter { iter =>
          IteratorUtil.last(iter.takeWhile { r =>
            val d = (r.time.toInstant.getMillis - ts).abs
            if (d <= distance || distance < 0) {
              distance = d
              true
            } else false
          })
        }
      }
    } else cdx
  }
}
