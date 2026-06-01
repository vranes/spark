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

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class BinByExecSuite extends QueryTest with SharedSparkSession {

  private def withUtc(f: => Unit): Unit =
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC")(f)

  test("single-bin pass-through emits one row with ratio 1.0") {
    withUtc {
      val df = sql(
        """SELECT CAST(ts_start AS STRING), CAST(ts_end AS STRING), value,
          |       CAST(bin_start AS STRING), CAST(bin_end AS STRING), bin_distribute_ratio
          |FROM VALUES
          |  (TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:05:00', 100)
          |  AS t(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end
          |  BIN WIDTH INTERVAL '5' MINUTE
          |  ALIGN TO TIMESTAMP '2024-01-01 00:00:00'
          |  DISTRIBUTE UNIFORM (value)
          |)""".stripMargin)
      checkAnswer(df, Seq(
        Row("2024-01-01 00:00:00", "2024-01-01 00:05:00", 100,
          "2024-01-01 00:00:00", "2024-01-01 00:05:00", 1.0)))
    }
  }

  test("splits a row across bins and redistributes proportionally") {
    withUtc {
      val df = sql(
        """SELECT CAST(ts_start AS STRING), CAST(ts_end AS STRING), value,
          |       CAST(bin_start AS STRING), CAST(bin_end AS STRING), bin_distribute_ratio
          |FROM VALUES
          |  (TIMESTAMP '2024-01-01 00:02:00', TIMESTAMP '2024-01-01 00:12:00', 100)
          |  AS t(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end
          |  BIN WIDTH INTERVAL '5' MINUTE
          |  ALIGN TO TIMESTAMP '2024-01-01 00:00:00'
          |  DISTRIBUTE UNIFORM (value)
          |)""".stripMargin)
      checkAnswer(df, Seq(
        Row("2024-01-01 00:02:00", "2024-01-01 00:05:00", 30,
          "2024-01-01 00:00:00", "2024-01-01 00:05:00", 0.3),
        Row("2024-01-01 00:05:00", "2024-01-01 00:10:00", 50,
          "2024-01-01 00:05:00", "2024-01-01 00:10:00", 0.5),
        Row("2024-01-01 00:10:00", "2024-01-01 00:12:00", 20,
          "2024-01-01 00:10:00", "2024-01-01 00:15:00", 0.2)))
    }
  }

  test("zero-length range emits one row with ratio 1.0 and passthrough value") {
    withUtc {
      val df = sql(
        """SELECT CAST(ts_start AS STRING), CAST(ts_end AS STRING), value,
          |       CAST(bin_start AS STRING), CAST(bin_end AS STRING), bin_distribute_ratio
          |FROM VALUES
          |  (TIMESTAMP '2024-01-01 00:02:00', TIMESTAMP '2024-01-01 00:02:00', 100)
          |  AS t(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end
          |  BIN WIDTH INTERVAL '5' MINUTE
          |  ALIGN TO TIMESTAMP '2024-01-01 00:00:00'
          |  DISTRIBUTE UNIFORM (value)
          |)""".stripMargin)
      checkAnswer(df, Seq(
        Row("2024-01-01 00:02:00", "2024-01-01 00:02:00", 100,
          "2024-01-01 00:00:00", "2024-01-01 00:05:00", 1.0)))
    }
  }

  test("inverted range raises BIN_BY_INVALID_RANGE") {
    withUtc {
      val df = sql(
        """SELECT * FROM VALUES
          |  (TIMESTAMP '2024-01-01 00:10:00', TIMESTAMP '2024-01-01 00:05:00', 100)
          |  AS t(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end
          |  BIN WIDTH INTERVAL '5' MINUTE
          |  DISTRIBUTE UNIFORM (value)
          |)""".stripMargin)
      val ex = intercept[SparkThrowable](df.collect())
      assert(ex.getCondition == "BIN_BY_INVALID_RANGE")
    }
  }

  test("NULL range column emits one row with NULL appended columns") {
    withUtc {
      val df = sql(
        """SELECT CAST(ts_start AS STRING), CAST(ts_end AS STRING), value,
          |       CAST(bin_start AS STRING), CAST(bin_end AS STRING), bin_distribute_ratio
          |FROM VALUES
          |  (CAST(NULL AS TIMESTAMP), TIMESTAMP '2024-01-01 00:05:00', 100)
          |  AS t(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end
          |  BIN WIDTH INTERVAL '5' MINUTE
          |  DISTRIBUTE UNIFORM (value)
          |)""".stripMargin)
      checkAnswer(df, Seq(Row(null, "2024-01-01 00:05:00", 100, null, null, null)))
    }
  }

  test("scales multi-column DISTRIBUTE of integer, decimal, double, float, and interval types") {
    withUtc {
      val df = sql(
        """SELECT i, d, dbl, flt, s, bin_distribute_ratio
          |FROM VALUES
          |  (TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:10:00',
          |   100, CAST(100.00 AS DECIMAL(5,2)), 100.0D, CAST(100.0 AS FLOAT), INTERVAL '10' SECOND)
          |  AS t(ts_start, ts_end, i, d, dbl, flt, s)
          |BIN BY (
          |  RANGE ts_start TO ts_end
          |  BIN WIDTH INTERVAL '5' MINUTE
          |  ALIGN TO TIMESTAMP '2024-01-01 00:00:00'
          |  DISTRIBUTE UNIFORM (i, d, dbl, flt, s)
          |)""".stripMargin)
      // Two 5-minute bins, each gets half the range (ratio 0.5), so each value is halved.
      val half = Row(50, new java.math.BigDecimal("50.00"), 50.0d, 50.0f,
        java.time.Duration.ofSeconds(5), 0.5d)
      checkAnswer(df, Seq(half, half))
    }
  }

  test("output column renames flow through to the result schema") {
    withUtc {
      val df = sql(
        """SELECT CAST(ws AS STRING), CAST(we AS STRING), frac
          |FROM VALUES
          |  (TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:05:00', 100)
          |  AS t(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end
          |  BIN WIDTH INTERVAL '5' MINUTE
          |  ALIGN TO TIMESTAMP '2024-01-01 00:00:00'
          |  DISTRIBUTE UNIFORM (value)
          |  BIN_START AS ws
          |  BIN_END AS we
          |  BIN_DISTRIBUTE_RATIO AS frac
          |)""".stripMargin)
      checkAnswer(df, Seq(Row("2024-01-01 00:00:00", "2024-01-01 00:05:00", 1.0)))
    }
  }

  test("TIMESTAMP_NTZ inputs bin in UTC regardless of session zone") {
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      val df = sql(
        """SELECT CAST(ts_start AS STRING), CAST(ts_end AS STRING), value,
          |       CAST(bin_start AS STRING), CAST(bin_end AS STRING), bin_distribute_ratio
          |FROM VALUES
          |  (TIMESTAMP_NTZ '2024-01-01 00:02:00', TIMESTAMP_NTZ '2024-01-01 00:12:00', 100)
          |  AS t(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end
          |  BIN WIDTH INTERVAL '5' MINUTE
          |  ALIGN TO TIMESTAMP_NTZ '2024-01-01 00:00:00'
          |  DISTRIBUTE UNIFORM (value)
          |)""".stripMargin)
      checkAnswer(df, Seq(
        Row("2024-01-01 00:02:00", "2024-01-01 00:05:00", 30,
          "2024-01-01 00:00:00", "2024-01-01 00:05:00", 0.3),
        Row("2024-01-01 00:05:00", "2024-01-01 00:10:00", 50,
          "2024-01-01 00:05:00", "2024-01-01 00:10:00", 0.5),
        Row("2024-01-01 00:10:00", "2024-01-01 00:12:00", 20,
          "2024-01-01 00:10:00", "2024-01-01 00:15:00", 0.2)))
    }
  }

  test("LTZ multi-day bins keep local-midnight boundaries across spring-forward") {
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      // Spring-forward is 2024-03-10 in LA. With civil-time arithmetic the bin boundaries stay at
      // local midnight; naive UTC arithmetic would drift the second boundary to 01:00.
      val df = sql(
        """SELECT CAST(bin_start AS STRING), CAST(bin_end AS STRING)
          |FROM VALUES
          |  (TIMESTAMP '2024-03-09 00:00:00', TIMESTAMP '2024-03-11 00:00:00', 100)
          |  AS t(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end
          |  BIN WIDTH INTERVAL '1' DAY
          |  ALIGN TO TIMESTAMP '2024-03-09 00:00:00'
          |  DISTRIBUTE UNIFORM (value)
          |)""".stripMargin)
      checkAnswer(df, Seq(
        Row("2024-03-09 00:00:00", "2024-03-10 00:00:00"),
        Row("2024-03-10 00:00:00", "2024-03-11 00:00:00")))
    }
  }

  test("LTZ multi-day bins keep local-midnight boundaries across fall-back") {
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      // Fall-back is 2024-11-03 in LA.
      val df = sql(
        """SELECT CAST(bin_start AS STRING), CAST(bin_end AS STRING)
          |FROM VALUES
          |  (TIMESTAMP '2024-11-02 00:00:00', TIMESTAMP '2024-11-04 00:00:00', 100)
          |  AS t(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end
          |  BIN WIDTH INTERVAL '1' DAY
          |  ALIGN TO TIMESTAMP '2024-11-02 00:00:00'
          |  DISTRIBUTE UNIFORM (value)
          |)""".stripMargin)
      checkAnswer(df, Seq(
        Row("2024-11-02 00:00:00", "2024-11-03 00:00:00"),
        Row("2024-11-03 00:00:00", "2024-11-04 00:00:00")))
    }
  }

  test("bare SELECT * consumes BinByExec output directly (UnsafeRow conversion)") {
    withUtc {
      // No projection on top of BIN BY, so the collect path consumes BinByExec rows directly and
      // would hit a GenericInternalRow -> UnsafeRow ClassCastException without the conversion.
      val rows = sql(
        """SELECT * FROM VALUES
          |  (TIMESTAMP '2024-01-01 00:02:00', TIMESTAMP '2024-01-01 00:12:00', 100)
          |  AS t(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end
          |  BIN WIDTH INTERVAL '5' MINUTE
          |  ALIGN TO TIMESTAMP '2024-01-01 00:00:00'
          |  DISTRIBUTE UNIFORM (value)
          |)""".stripMargin).collect()
      assert(rows.length == 3)
    }
  }

  test("scales TINYINT and SMALLINT DISTRIBUTE columns") {
    withUtc {
      val df = sql(
        """SELECT b, s, bin_distribute_ratio FROM VALUES
          |  (TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:10:00',
          |   CAST(100 AS TINYINT), CAST(100 AS SMALLINT))
          |  AS t(ts_start, ts_end, b, s)
          |BIN BY (
          |  RANGE ts_start TO ts_end
          |  BIN WIDTH INTERVAL '5' MINUTE
          |  ALIGN TO TIMESTAMP '2024-01-01 00:00:00'
          |  DISTRIBUTE UNIFORM (b, s)
          |)""".stripMargin)
      val half = Row(50.toByte, 50.toShort, 0.5)
      checkAnswer(df, Seq(half, half))
    }
  }

  test("scales high-precision DECIMAL(38,2) DISTRIBUTE column") {
    withUtc {
      val df = sql(
        """SELECT amount, bin_distribute_ratio FROM VALUES
          |  (TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:10:00',
          |   CAST(100.00 AS DECIMAL(38,2)))
          |  AS t(ts_start, ts_end, amount)
          |BIN BY (
          |  RANGE ts_start TO ts_end
          |  BIN WIDTH INTERVAL '5' MINUTE
          |  ALIGN TO TIMESTAMP '2024-01-01 00:00:00'
          |  DISTRIBUTE UNIFORM (amount)
          |)""".stripMargin)
      val half = Row(new java.math.BigDecimal("50.00"), 0.5)
      checkAnswer(df, Seq(half, half))
    }
  }

  test("both NULL range columns emit one row with NULL appended columns") {
    withUtc {
      val df = sql(
        """SELECT CAST(ts_start AS STRING), value, CAST(bin_start AS STRING), bin_distribute_ratio
          |FROM VALUES
          |  (CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP), 100)
          |  AS t(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end
          |  BIN WIDTH INTERVAL '5' MINUTE
          |  DISTRIBUTE UNIFORM (value)
          |)""".stripMargin)
      checkAnswer(df, Seq(Row(null, 100, null, null)))
    }
  }

  test("pre-1970 range spanning the epoch bins correctly") {
    withUtc {
      val df = sql(
        """SELECT CAST(bin_start AS STRING), value, bin_distribute_ratio
          |FROM VALUES
          |  (TIMESTAMP '1969-12-31 23:50:00', TIMESTAMP '1970-01-01 00:05:00', 300)
          |  AS t(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end
          |  BIN WIDTH INTERVAL '5' MINUTE
          |  ALIGN TO TIMESTAMP '1970-01-01 00:00:00'
          |  DISTRIBUTE UNIFORM (value)
          |)""".stripMargin)
      val third = 1.0 / 3.0
      checkAnswer(df, Seq(
        Row("1969-12-31 23:50:00", 100, third),
        Row("1969-12-31 23:55:00", 100, third),
        Row("1970-01-01 00:00:00", 100, third)))
    }
  }

  test("range aligned exactly to one bin emits a single full bin (no empty trailing bin)") {
    withUtc {
      val df = sql(
        """SELECT CAST(bin_start AS STRING), CAST(bin_end AS STRING), bin_distribute_ratio
          |FROM VALUES
          |  (TIMESTAMP '2024-01-01 00:05:00', TIMESTAMP '2024-01-01 00:10:00', 100)
          |  AS t(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end
          |  BIN WIDTH INTERVAL '5' MINUTE
          |  ALIGN TO TIMESTAMP '2024-01-01 00:00:00'
          |  DISTRIBUTE UNIFORM (value)
          |)""".stripMargin)
      checkAnswer(df, Seq(Row("2024-01-01 00:05:00", "2024-01-01 00:10:00", 1.0)))
    }
  }

  test("an inverted row among valid rows still raises BIN_BY_INVALID_RANGE") {
    withUtc {
      val df = sql(
        """SELECT * FROM VALUES
          |  (TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:05:00', 1),
          |  (TIMESTAMP '2024-01-01 00:10:00', TIMESTAMP '2024-01-01 00:05:00', 2)
          |  AS t(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end
          |  BIN WIDTH INTERVAL '5' MINUTE
          |  DISTRIBUTE UNIFORM (value)
          |)""".stripMargin)
      val ex = intercept[SparkThrowable](df.collect())
      assert(ex.getCondition == "BIN_BY_INVALID_RANGE")
    }
  }

  test("BIN_BY_INVALID_RANGE reports session-zone wall-clock timestamps") {
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      val df = sql(
        """SELECT * FROM VALUES
          |  (TIMESTAMP '2024-01-01 00:10:00', TIMESTAMP '2024-01-01 00:05:00', 1)
          |  AS t(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end
          |  BIN WIDTH INTERVAL '5' MINUTE
          |  DISTRIBUTE UNIFORM (value)
          |)""".stripMargin)
      val ex = intercept[SparkThrowable](df.collect())
      assert(ex.getCondition == "BIN_BY_INVALID_RANGE")
      assert(ex.getMessageParameters.get("rangeStart") == "2024-01-01 00:10:00")
      assert(ex.getMessageParameters.get("rangeEnd") == "2024-01-01 00:05:00")
    }
  }

  test("numOutputRows metric counts the emitted rows on every path") {
    withSQLConf(
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val clause =
        """RANGE ts_start TO ts_end
          |BIN WIDTH INTERVAL '5' MINUTE
          |ALIGN TO TIMESTAMP '2024-01-01 00:00:00'
          |DISTRIBUTE UNIFORM (value)""".stripMargin
      def metricFor(row: String): Long = {
        val df = sql(
          s"SELECT * FROM VALUES $row AS t(ts_start, ts_end, value) BIN BY ($clause)")
        df.collect()
        df.queryExecution.executedPlan.collect { case b: BinByExec => b }
          .map(_.metrics("numOutputRows").value).sum
      }
      // Multi-bin split, zero-length, and NULL all funnel through the same increment site.
      assert(metricFor(
        "(TIMESTAMP '2024-01-01 00:02:00', TIMESTAMP '2024-01-01 00:12:00', 100)") == 3)
      assert(metricFor(
        "(TIMESTAMP '2024-01-01 00:02:00', TIMESTAMP '2024-01-01 00:02:00', 100)") == 1)
      assert(metricFor("(CAST(NULL AS TIMESTAMP), TIMESTAMP '2024-01-01 00:05:00', 100)") == 1)
    }
  }

  test("wide range with small width yields many bins lazily") {
    withUtc {
      // A full day at 1-minute bins is 1440 sub-rows; exercises the lazy iterator at scale and
      // guards against an off-by-one in bin advancement.
      val df = sql(
        """SELECT bin_distribute_ratio FROM VALUES
          |  (TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-02 00:00:00', 1440)
          |  AS t(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end
          |  BIN WIDTH INTERVAL '1' MINUTE
          |  ALIGN TO TIMESTAMP '2024-01-01 00:00:00'
          |  DISTRIBUTE UNIFORM (value)
          |)""".stripMargin)
      val ratios = df.collect().map(_.getDouble(0))
      assert(ratios.length == 1440)
      assert(math.abs(ratios.sum - 1.0) < 1e-9)
    }
  }

  test("BinByExec rejects a non-positive bin width") {
    val child = spark.range(1).queryExecution.sparkPlan
    intercept[IllegalArgumentException] {
      BinByExec(0L, 0L, 0, 0, Seq.empty, None, Seq.empty, child)
    }
  }
}
