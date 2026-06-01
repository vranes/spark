/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import java.math.{BigDecimal => JBigDecimal, RoundingMode}
import java.time.{ZoneId, ZoneOffset}

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.{ByteType, DataType, DayTimeIntervalType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType}

/**
 * Physical node for the `BIN BY` relation operator. For each input row it emits one output row per
 * bin overlapping `[rangeStart, rangeEnd)`.
 *
 * Per output sub-row:
 *  - the range columns are clipped to the bin's intersection with `[rangeStart, rangeEnd)`
 *    (half-open: a row ending exactly on a boundary does not spawn an empty trailing bin);
 *  - the DISTRIBUTE UNIFORM columns are scaled by the overlap fraction;
 *  - all other input columns are replicated unchanged;
 *  - `bin_start`, `bin_end`, and `bin_distribute_ratio` are appended.
 *
 * Bin boundaries are computed with [[DateTimeUtils.timeBucketDTInterval]] /
 * [[DateTimeUtils.timestampAddDayTime]], so they match `time_bucket`: sub-day widths use UTC
 * microsecond arithmetic, multi-day widths use civil-time arithmetic in the session zone (UTC for
 * TIMESTAMP_NTZ).
 *
 * `bin_distribute_ratio` and the DISTRIBUTE scaling factor are `overlap / total` measured in
 * elapsed microseconds, where `total = rangeEnd - rangeStart`. As a result, a civil day shortened
 * or lengthened by a DST transition receives a proportionally smaller or larger share even though
 * its `bin_start`/`bin_end` look like equal calendar days.
 *
 * Scaling is per-type: integer, DECIMAL, and DAY-TIME INTERVAL columns use exact rational scaling
 * (`value * overlap / total`, rounded HALF_UP); FLOAT/DOUBLE use ordinary IEEE multiplication.
 * Integer scaling rounds each sub-row independently, so the scaled parts are not guaranteed to sum
 * back to the original integer value (a deliberate choice for bit-for-bit parity with the Photon
 * kernel). DECIMAL and DAY-TIME INTERVAL behave the same way.
 *
 * Per-row edge cases: a NULL in either range column emits one row with NULL appended columns and
 * the DISTRIBUTE columns passed through; a zero-length range emits one row with ratio 1.0 and the
 * DISTRIBUTE columns passed through; an inverted range (`rangeStart > rangeEnd`) raises
 * `BIN_BY_INVALID_RANGE`.
 */
case class BinByExec(
    binWidthMicros: Long,
    originMicros: Long,
    rangeStartIdx: Int,
    rangeEndIdx: Int,
    distributeColumnIndices: Seq[Int],
    timeZoneId: Option[String],
    appendedAttributes: Seq[Attribute],
    child: SparkPlan)
  extends UnaryExecNode {

  require(binWidthMicros > 0,
    s"BIN BY requires a positive bin width in micros, got $binWidthMicros")

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def output: Seq[Attribute] = child.output ++ appendedAttributes

  // BIN BY rewrites (clips) the range columns, so any child partitioning that references them no
  // longer holds; drop it in that case. Partitioning on passthrough columns is preserved. Output
  // ordering is intentionally not preserved (one ordered input row expands into several rows), so
  // the UnaryExecNode default of Nil is correct.
  override def outputPartitioning: Partitioning = {
    val clipped = AttributeSet(Seq(child.output(rangeStartIdx), child.output(rangeEndIdx)))
    child.outputPartitioning match {
      case e: Expression if e.references.intersect(clipped).nonEmpty =>
        UnknownPartitioning(child.outputPartitioning.numPartitions)
      case other => other
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): BinByExec =
    copy(child = newChild)

  protected override def doExecute(): RDD[InternalRow] = {
    val rsIdx = rangeStartIdx
    val reIdx = rangeEndIdx
    val width = binWidthMicros
    val origin = originMicros
    val zone = timeZoneId.map(DateTimeUtils.getZoneId).getOrElse(ZoneOffset.UTC)
    val childTypes = child.output.map(_.dataType).toArray
    val numInput = childTypes.length
    val numAppended = appendedAttributes.length
    val distIdx = distributeColumnIndices.toArray
    val outputTypes = output.map(_.dataType).toArray
    val numOutputRows = longMetric("numOutputRows")

    child.execute().mapPartitions { rows =>
      // UnsafeProjection returns a single shared, mutable UnsafeRow, so it must only be applied in
      // the final lazy map below (one row consumed at a time), never into a buffer.
      val toUnsafe = UnsafeProjection.create(outputTypes)
      rows.flatMap { row =>
        // Passthrough values for the input columns, read once and reused for every sub-row.
        val base = new Array[Any](numInput)
        var i = 0
        while (i < numInput) {
          base(i) = row.get(i, childTypes(i))
          i += 1
        }

        val rowIter: Iterator[InternalRow] =
          if (row.isNullAt(rsIdx) || row.isNullAt(reIdx)) {
            // NULL range: one row, appended columns NULL, DISTRIBUTE columns pass through.
            Iterator.single(appendRow(base, null, null, null))
          } else {
            val rs = row.getLong(rsIdx)
            val re = row.getLong(reIdx)
            if (rs > re) {
              throw QueryExecutionErrors.binByInvalidRangeError(
                formatTimestamp(rs, zone), formatTimestamp(re, zone))
            } else if (rs == re) {
              // Zero-length range: one row, ratio 1.0, value unscaled, bin = bin containing rs.
              val binStart = DateTimeUtils.timeBucketDTInterval(width, rs, origin, zone)
              val binEnd = DateTimeUtils.timestampAddDayTime(binStart, width, zone)
              Iterator.single(appendRow(base, binStart, binEnd, 1.0d))
            } else {
              // Lazily yield one row per overlapping bin so a wide range with a small width does
              // not materialize every sub-row at once.
              val total = Math.subtractExact(re, rs)
              new Iterator[InternalRow] {
                private var curStart = DateTimeUtils.timeBucketDTInterval(width, rs, origin, zone)

                override def hasNext: Boolean = curStart < re

                override def next(): InternalRow = {
                  val curEnd = DateTimeUtils.timestampAddDayTime(curStart, width, zone)
                  val clipStart = math.max(rs, curStart)
                  val clipEnd = math.min(re, curEnd)
                  val overlap = clipEnd - clipStart

                  val full = new Array[Any](numInput + numAppended)
                  System.arraycopy(base, 0, full, 0, numInput)
                  full(rsIdx) = clipStart
                  full(reIdx) = clipEnd
                  var d = 0
                  while (d < distIdx.length) {
                    val idx = distIdx(d)
                    full(idx) = scaleValue(row, idx, childTypes(idx), overlap, total)
                    d += 1
                  }
                  full(numInput) = curStart
                  full(numInput + 1) = curEnd
                  full(numInput + 2) = overlap.toDouble / total.toDouble

                  curStart = curEnd
                  new GenericInternalRow(full)
                }
              }
            }
          }
        rowIter.map { r =>
          numOutputRows += 1
          toUnsafe(r)
        }
      }
    }
  }

  private def appendRow(base: Array[Any], binStart: Any, binEnd: Any, ratio: Any): InternalRow = {
    val full = new Array[Any](base.length + appendedAttributes.length)
    System.arraycopy(base, 0, full, 0, base.length)
    full(base.length) = binStart
    full(base.length + 1) = binEnd
    full(base.length + 2) = ratio
    new GenericInternalRow(full)
  }

  private def formatTimestamp(micros: Long, zone: ZoneId): String =
    TimestampFormatter.getFractionFormatter(zone).format(micros)

  // Integer and DAY-TIME INTERVAL columns use exact rational scaling; FLOAT/DOUBLE use ordinary
  // IEEE multiplication by the ratio (float math is already lossy). This matches the planned
  // Photon kernel bit-for-bit on the exact types.
  private def scaleValue(
      row: InternalRow, idx: Int, dt: DataType, numer: Long, denom: Long): Any = dt match {
    case ByteType => scaleIntegral(row.getByte(idx).toLong, numer, denom).toByte
    case ShortType => scaleIntegral(row.getShort(idx).toLong, numer, denom).toShort
    case IntegerType => scaleIntegral(row.getInt(idx).toLong, numer, denom).toInt
    case LongType => scaleIntegral(row.getLong(idx), numer, denom)
    case _: DayTimeIntervalType => scaleIntegral(row.getLong(idx), numer, denom)
    case dec: DecimalType =>
      // overlap <= total, so the scaled magnitude never exceeds the input and precision
      // cannot grow.
      val v = row.getDecimal(idx, dec.precision, dec.scale).toJavaBigDecimal
      val scaled = v.multiply(JBigDecimal.valueOf(numer))
        .divide(JBigDecimal.valueOf(denom), dec.scale, RoundingMode.HALF_UP)
      Decimal(scaled, dec.precision, dec.scale)
    case FloatType => (row.getFloat(idx) * (numer.toDouble / denom.toDouble)).toFloat
    case DoubleType => row.getDouble(idx) * (numer.toDouble / denom.toDouble)
    case other =>
      throw SparkException.internalError(s"BIN BY does not support DISTRIBUTE column type $other")
  }

  // Exact rational scaling `value * numer / denom`, rounded half-up. Done in BigDecimal so the
  // 128-bit intermediate product cannot overflow.
  private def scaleIntegral(value: Long, numer: Long, denom: Long): Long =
    new JBigDecimal(value)
      .multiply(new JBigDecimal(numer))
      .divide(new JBigDecimal(denom), 0, RoundingMode.HALF_UP)
      .longValueExact()
}
