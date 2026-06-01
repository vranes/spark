-- BIN BY relation operator: end-to-end execution and output snapshots.
-- Per-row edge cases (inverted / NULL / zero-length ranges) and the exhaustive scaling and DST
-- variants are covered by BinByExecSuite; this file snapshots the main scenarios.

SET TIME ZONE 'UTC';

CREATE OR REPLACE TEMP VIEW metrics AS
SELECT * FROM VALUES
  (TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:05:00', 100),
  (TIMESTAMP '2024-01-01 00:02:00', TIMESTAMP '2024-01-01 00:12:00', 300)
AS metrics(ts_start, ts_end, value);


-- Single-bin pass-through (row 1) and a multi-bin split with proportional redistribution (row 2).
SELECT * FROM metrics BIN BY (
  RANGE ts_start TO ts_end
  BIN WIDTH INTERVAL '5' MINUTE
  ALIGN TO TIMESTAMP '2024-01-01 00:00:00'
  DISTRIBUTE UNIFORM (value)
);


-- Composability: feed BIN BY into a downstream GROUP BY.
SELECT bin_start, SUM(value) AS total
FROM metrics BIN BY (
  RANGE ts_start TO ts_end
  BIN WIDTH INTERVAL '5' MINUTE
  ALIGN TO TIMESTAMP '2024-01-01 00:00:00'
  DISTRIBUTE UNIFORM (value)
)
GROUP BY bin_start
ORDER BY bin_start;


-- Custom ALIGN TO origin and renamed output columns.
SELECT * FROM VALUES
  (TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:08:00', 100)
  AS t(ts_start, ts_end, value)
BIN BY (
  RANGE ts_start TO ts_end
  BIN WIDTH INTERVAL '5' MINUTE
  ALIGN TO TIMESTAMP '2024-01-01 00:01:00'
  DISTRIBUTE UNIFORM (value)
  BIN_START AS window_start
  BIN_END AS window_end
  BIN_DISTRIBUTE_RATIO AS frac
);


-- Multi-column DISTRIBUTE with numeric, decimal, and day-time interval types.
SELECT * FROM VALUES
  (TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:10:00',
   100, CAST(100.00 AS DECIMAL(5,2)), INTERVAL '10' SECOND)
  AS t(ts_start, ts_end, cnt, amount, span)
BIN BY (
  RANGE ts_start TO ts_end
  BIN WIDTH INTERVAL '5' MINUTE
  ALIGN TO TIMESTAMP '2024-01-01 00:00:00'
  DISTRIBUTE UNIFORM (cnt, amount, span)
);


-- TIMESTAMP_NTZ input bins in UTC regardless of session zone.
SELECT * FROM VALUES
  (TIMESTAMP_NTZ '2024-01-01 00:02:00', TIMESTAMP_NTZ '2024-01-01 00:12:00', 300)
  AS t(ts_start, ts_end, value)
BIN BY (
  RANGE ts_start TO ts_end
  BIN WIDTH INTERVAL '5' MINUTE
  DISTRIBUTE UNIFORM (value)
);


DROP VIEW metrics;


-- Non-UTC LTZ section, kept last so no later test inherits the zone. Multi-day bins keep
-- local-midnight boundaries across the spring-forward transition (2024-03-10 in Los Angeles).
SET TIME ZONE 'America/Los_Angeles';

SELECT * FROM VALUES
  (TIMESTAMP '2024-03-09 00:00:00', TIMESTAMP '2024-03-11 00:00:00', 100)
  AS t(ts_start, ts_end, value)
BIN BY (
  RANGE ts_start TO ts_end
  BIN WIDTH INTERVAL '1' DAY
  ALIGN TO TIMESTAMP '2024-03-09 00:00:00'
  DISTRIBUTE UNIFORM (value)
);
