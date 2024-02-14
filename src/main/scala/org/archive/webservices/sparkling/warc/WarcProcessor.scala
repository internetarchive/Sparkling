package org.archive.webservices.sparkling.warc

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.archive.webservices.sparkling.budget.BudgetRddManager
import org.archive.webservices.sparkling.cdx.CdxRecord
import org.archive.webservices.sparkling.compression.{Gzip, GzipBytes, Zstd}
import org.archive.webservices.sparkling.io.{CleanupInputStream, HdfsIO, IOUtil}
import org.archive.webservices.sparkling.logging.{Log, LogContext}
import org.archive.webservices.sparkling.util._

import java.io._
import scala.reflect.ClassTag

object WarcProcessor {
  implicit val logContext: LogContext = LogContext(this)

  import org.archive.webservices.sparkling.Sparkling._

  var repartitionBufferSize: Int = prop(10000000)(repartitionBufferSize, repartitionBufferSize = _) // number of accumulated records
  var perRecordTimeoutMillis: Int = prop(1000 * 60 * 60)(perRecordTimeoutMillis, perRecordTimeoutMillis = _) // 1 hour

  type WarcAccessor = (WarcRecord => Any) => Any

  def repartitionAndExtractWarcsByCdx(cdx: RDD[CdxRecord], warcHeader: WarcFileMeta, destPath: String, maxFileSize: Long, generateCdx: Boolean = true, gzipped: Boolean = true)(
      request: (String, Long, Long) => ManagedVal[InputStream]
  ): Long = {
    HdfsIO.ensureOutDir(destPath, ensureNew = false)

    val bytesReservedForWarcHeader = {
      val headerBytes = WarcHeaders.warcFile(warcHeader, StringUtil.stripSuffixes(new Path(destPath).getName, GzipExt, WarcExt, ArcExt) + "-00000-WARC" + WarcExt + GzipExt)
      val compressedHeader = if (gzipped) GzipBytes(headerBytes) else headerBytes
      compressedHeader.length
    }

    val warcs = cdx.filter { record =>
      val location = record.locationFromAdditionalFields._1
      StringUtil.stripSuffixes(location.toLowerCase, GzipExt, ZstdExt).endsWith(WarcExt)
    }
    val warcsRepartitioned = BudgetRddManager.repartitionByBudget(warcs, maxFileSize - bytesReservedForWarcHeader, repartitionBufferSize)(_.compressedSize)
    val warcCount = extractWarcsByCdx(warcsRepartitioned, warcHeader, destPath, generateCdx, gzipped, checkPath = false)(request)

    val bytesReservedForArcHeader = {
      val headerBytes = WarcHeaders.arcFile(warcHeader, StringUtil.stripSuffixes(new Path(destPath).getName, GzipExt, WarcExt, ArcExt) + "-00000-ARC" + ArcExt + GzipExt)
      val compressedHeader = if (gzipped) GzipBytes(headerBytes) else headerBytes
      compressedHeader.length
    }

    val arcs = cdx.filter { record =>
      val location = record.locationFromAdditionalFields._1
      StringUtil.stripSuffix(location.toLowerCase, GzipExt).endsWith(ArcExt)
    }
    val arcsRepartitioned = BudgetRddManager.repartitionByBudget(arcs, maxFileSize - bytesReservedForArcHeader, repartitionBufferSize)(_.compressedSize)
    val arcCount = extractWarcsByCdx(arcsRepartitioned, warcHeader, destPath, generateCdx, gzipped, checkPath = false)(request)

    warcCount + arcCount
  }

  def extractWarcsByCdx(cdx: RDD[CdxRecord], warcHeader: WarcFileMeta, destPath: String, generateCdx: Boolean = true, gzipped: Boolean = true, checkPath: Boolean = true)(
      request: (String, Long, Long) => ManagedVal[InputStream]
  ): Long = {
    HdfsIO.ensureOutDir(destPath, ensureNew = checkPath)
    val filePrefix = StringUtil.stripSuffixes(new Path(destPath).getName, GzipExt, WarcExt, ArcExt)
    initPartitions(cdx).mapPartitionsWithIndex { (idx, records) =>
      val processed = extractPartitionWarcsByCdx(records, warcHeader, destPath, filePrefix + "-" + StringUtil.padNum(idx, 5), generateCdx, gzipped)(request)
      Iterator(processed)
    }.reduce(_ + _)
  }

  def extractPartitionWarcsByCdx(partition: TraversableOnce[CdxRecord], warcHeader: WarcFileMeta, destPath: String, destFilePrefix: String, generateCdx: Boolean = true, gzipped: Boolean = true)(
      request: (String, Long, Long) => ManagedVal[InputStream]
  ): Long = {
    HdfsIO.ensureOutDir(destPath, ensureNew = false)

    val warcName = destFilePrefix + "-WARC"
    val arcName = destFilePrefix + "-ARC"

    val warcFile = warcName + WarcExt + (if (gzipped) GzipExt else "")
    val warcPath = new Path(destPath, warcFile).toString

    val arcFile = arcName + ArcExt + (if (gzipped) GzipExt else "")
    val arcPath = new Path(destPath, arcFile).toString

    val warcCdxPath = new Path(destPath, warcName + WarcExt + CdxExt + GzipExt).toString
    val arcCdxPath = new Path(destPath, arcName + ArcExt + CdxExt + GzipExt).toString

    val warcErrPath = new Path(destPath, warcName + WarcExt + CdxExt + ErrLogExt).toString
    val arcErrPath = new Path(destPath, arcName + ArcExt + CdxExt + ErrLogExt).toString

    val warcComplete = HdfsIO.exists(warcPath) && !HdfsIO.exists(warcCdxPath)
    val arcComplete = HdfsIO.exists(arcPath) && !HdfsIO.exists(arcCdxPath)

    if (warcComplete && arcComplete) 0L
    else {
      var warcPosition = 0L
      val warcOut = Common.lazyValWithCleanup({
        val out = HdfsIO.out(warcPath, compress = false, overwrite = true)
        val headerBytes = WarcHeaders.warcFile(warcHeader, warcFile)
        val compressedHeader = if (gzipped) GzipBytes(headerBytes) else headerBytes
        out.write(compressedHeader)
        warcPosition += compressedHeader.length
        out
      })(_.close)

      var arcPosition = 0L
      val arcOut = Common.lazyValWithCleanup({
        val out = HdfsIO.out(arcPath, compress = false, overwrite = true)
        val headerBytes = WarcHeaders.arcFile(warcHeader, arcFile)
        val compressedHeader = if (gzipped) GzipBytes(headerBytes) else headerBytes
        out.write(compressedHeader)
        arcPosition += compressedHeader.length
        out
      })(_.close)

      val warcCdxOut = Common.lazyValWithCleanup(IOUtil.print(HdfsIO.out(warcCdxPath, overwrite = true)))(_.close)
      val arcCdxOut = Common.lazyValWithCleanup(IOUtil.print(HdfsIO.out(arcCdxPath, overwrite = true)))(_.close)

      val warcErrOut = Common.lazyValWithCleanup(IOUtil.print(HdfsIO.out(warcErrPath, overwrite = true)))(_.close)
      val arcErrOut = Common.lazyValWithCleanup(IOUtil.print(HdfsIO.out(arcErrPath, overwrite = true)))(_.close)

      var isArc = false
      val processed = partition.map { record =>
        try {
          val (location, offset) = record.locationFromAdditionalFields
          isArc = StringUtil.stripSuffix(location, GzipExt).toLowerCase.endsWith(ArcExt)
          if ((isArc && arcComplete) || (!isArc && warcComplete)) 0L
          else {
            val length = record.compressedSize
            val status = "Copying " + location + ":" + offset + "+" + length + "..."
            Common.timeout(perRecordTimeoutMillis, Some(status)) {
              Log.debug(status)
              if (isArc) {
                request(location, offset, length)(IOUtil.copy(_, arcOut.get))
                if (generateCdx) arcCdxOut.get.println(record.toCdxString(Seq(arcPosition.toString, arcFile)))
                arcPosition += length
              } else {
                if (location.toLowerCase.endsWith(ZstdExt)) {
                  Zstd.ifInit(location, close = true) {
                    val r = request(location, 0, -1)
                    new CleanupInputStream(r.get, () => r.clear(false))
                  }
                  val compressedSize = Gzip.countCompressOut(warcOut.get) { compressOut =>
                    request(location, offset, length)(in => IOUtil.copy(Zstd.decompress(in), compressOut))
                  }
                  if (generateCdx) warcCdxOut.get.println(record.copy(compressedSize = compressedSize).toCdxString(Seq(warcPosition.toString, warcFile)))
                  warcPosition += compressedSize
                } else {
                  request(location, offset, length)(IOUtil.copy(_, warcOut.get))
                  if (generateCdx) warcCdxOut.get.println(record.toCdxString(Seq(warcPosition.toString, warcFile)))
                  warcPosition += length
                }
              }
              Log.debug("Copying " + location + ":" + offset + ":" + length + " - done.")
              1L
            }
          }
        } catch {
          case e: Exception =>
            val errOut = if (isArc) arcErrOut.get else warcErrOut.get
            errOut.println(record.toCdxString)
            errOut.println("# " + Log.prependTimestamp("Failed: " + e.getMessage))
            0L
        }
      }.sum

      Log.info("Copying done.")

      warcOut.clear(true)
      arcOut.clear(true)
      warcCdxOut.clear(true)
      arcCdxOut.clear(true)
      warcErrOut.clear(true)
      arcErrOut.clear(true)

      processed
    }
  }

  def runByCdx[R: ClassTag](records: RDD[CdxRecord], open: CdxRecord => WarcAccessor)(action: WarcRecord => Option[R]): RDD[R] = {
    initPartitions(records).flatMap { record => Common.timeout(perRecordTimeoutMillis) { open(record) { warc => action(warc) }.asInstanceOf[Option[R]] } }
  }

  def run[R: ClassTag](path: String)(action: WarcRecord => TraversableOnce[R]): RDD[R] = runByFile(path) { (_, records) => records.flatMap(action) }

  def runByFile[R: ClassTag](path: String)(action: (String, Iterator[WarcRecord]) => TraversableOnce[R]): RDD[R] = {
    RddUtil.loadBinary(path, decompress = false, close = false) { (file, in) =>
      Log.info("Processing " + file + "...")
      IteratorUtil.cleanup(
        action(file, WarcLoader.load(in)).toIterator,
        () => {
          in.close()
          Log.info("Done. (" + file + ")")
        }
      )
    }
  }
}
