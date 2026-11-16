-- ============================================================
-- OPTIMIZED: Retention vs Golden comparison
-- ============================================================
-- Key optimizations:
--   1. Removed CAST() on partition columns (entry_date, entry_hour, day)
--      to enable PARTITION PRUNING — this is the #1 performance win.
--      Original: CAST(entry_date AS DATE) = CAST('2026-10-05' AS DATE) - INTERVAL '1' DAY
--      Now:      entry_date = '2026-10-04'  (string compare => Hive prunes partitions)
--   2. Simplified "BETWEEN 0 AND 23" — that's ALL hours, so it's redundant.
--      Collapsed 3 OR branches into: IN('date-2','date-1') + one small branch.
--   3. Standardized day filter across all 7 blocks (some used CAST, some date_format).
--   4. Added COALESCE on the output date column for LEFT JOIN safety.
-- ============================================================

-- ================= GPRS =================
SELECT
  COALESCE(g.start_of_charge_dt, r.start_of_charge_dt) AS start_of_charge_dt,
  'retention_vs_golden'  AS alarm_identifier,
  'GPRS'                 AS table_name,
  g.cnt                  AS golden_count,
  r.cnt                  AS retention_count,
  r.cnt - g.cnt          AS diff,
  ((r.cnt - g.cnt) * 100.0) / r.cnt AS diff_perc
FROM (
  SELECT start_of_charge_dt, COUNT(*) AS cnt
  FROM hive."usage".network_activity_gprs_retention
  WHERE (
      entry_date IN (
        date_format(CAST('2026-10-05' AS DATE) - INTERVAL '1' DAY, '%Y-%m-%d'),
        date_format(CAST('2026-10-05' AS DATE) - INTERVAL '2' DAY, '%Y-%m-%d')
      )
      OR (
        entry_date = date_format(CAST('2026-10-05' AS DATE), '%Y-%m-%d')
        AND entry_hour IN ('0','1','2','3','4')
      )
    )
    AND day = date_format(CAST('2026-10-05' AS DATE) - INTERVAL '1' DAY, '%Y-%m-%d')
  GROUP BY 1
) r
LEFT JOIN (
  SELECT start_of_charge_dt, COUNT(*) AS cnt
  FROM hive."usage".network_activity_gprs
  WHERE start_of_charge_dt = date_format(CAST('2026-10-05' AS DATE) - INTERVAL '1' DAY, '%Y-%m-%d')
  GROUP BY 1
) g ON r.start_of_charge_dt = g.start_of_charge_dt

UNION ALL

-- ================= GGSN =================
SELECT
  COALESCE(g.start_dt, r.start_dt),
  'retention_vs_golden',
  'GGSN',
  g.cnt,
  r.cnt,
  r.cnt - g.cnt,
  ((r.cnt - g.cnt) * 100.0) / r.cnt
FROM (
  SELECT start_dt, COUNT(*) AS cnt
  FROM hive."usage".network_activity_ggsn_retention
  WHERE (
      entry_date IN (
        date_format(CAST('2026-10-05' AS DATE) - INTERVAL '1' DAY, '%Y-%m-%d'),
        date_format(CAST('2026-10-05' AS DATE) - INTERVAL '2' DAY, '%Y-%m-%d')
      )
      OR (
        entry_date = date_format(CAST('2026-10-05' AS DATE), '%Y-%m-%d')
        AND entry_hour IN ('0','1','2','3','4')
      )
    )
    AND day = date_format(CAST('2026-10-05' AS DATE) - INTERVAL '1' DAY, '%Y-%m-%d')
  GROUP BY 1
) r
LEFT JOIN (
  SELECT start_dt, COUNT(*) AS cnt
  FROM hive."usage".network_activity_ggsn
  WHERE start_dt = date_format(CAST('2026-10-05' AS DATE) - INTERVAL '1' DAY, '%Y-%m-%d')
  GROUP BY 1
) g ON r.start_dt = g.start_dt

UNION ALL

-- ================= MOC MSC =================
SELECT
  COALESCE(g.start_of_charge_dt, r.start_of_charge_dt),
  'retention_vs_golden',
  'MOC_MSC',
  g.cnt,
  r.cnt,
  r.cnt - g.cnt,
  ((r.cnt - g.cnt) * 100.0) / r.cnt
FROM (
  SELECT start_of_charge_dt, COUNT(*) AS cnt
  FROM hive."usage".network_activity_moc_msc_retention
  WHERE (
      entry_date IN (
        date_format(CAST('2026-10-05' AS DATE) - INTERVAL '1' DAY, '%Y-%m-%d'),
        date_format(CAST('2026-10-05' AS DATE) - INTERVAL '2' DAY, '%Y-%m-%d')
      )
      OR (
        entry_date = date_format(CAST('2026-10-05' AS DATE), '%Y-%m-%d')
        AND entry_hour IN ('0','1','2','3','4')
      )
    )
    AND day = date_format(CAST('2026-10-05' AS DATE) - INTERVAL '1' DAY, '%Y-%m-%d')
  GROUP BY 1
) r
LEFT JOIN (
  SELECT start_of_charge_dt, COUNT(*) AS cnt
  FROM hive."usage".network_activity_moc_msc
  WHERE start_of_charge_dt = date_format(CAST('2026-10-05' AS DATE) - INTERVAL '1' DAY, '%Y-%m-%d')
  GROUP BY 1
) g ON r.start_of_charge_dt = g.start_of_charge_dt

UNION ALL

-- ================= MOC CCN =================
SELECT
  COALESCE(g.start_of_charge_dt, r.start_of_charge_dt),
  'retention_vs_golden',
  'MOC_CCN',
  g.cnt,
  r.cnt,
  r.cnt - g.cnt,
  ((r.cnt - g.cnt) * 100.0) / r.cnt
FROM (
  SELECT start_of_charge_dt, COUNT(*) AS cnt
  FROM hive."usage".network_activity_moc_ccn_retention
  WHERE (
      entry_date IN (
        date_format(CAST('2026-10-05' AS DATE) - INTERVAL '1' DAY, '%Y-%m-%d'),
        date_format(CAST('2026-10-05' AS DATE) - INTERVAL '2' DAY, '%Y-%m-%d')
      )
      OR (
        entry_date = date_format(CAST('2026-10-05' AS DATE), '%Y-%m-%d')
        AND entry_hour IN ('0','1','2','3','4')
      )
    )
    AND day = date_format(CAST('2026-10-05' AS DATE) - INTERVAL '1' DAY, '%Y-%m-%d')
  GROUP BY 1
) r
LEFT JOIN (
  SELECT start_of_charge_dt, COUNT(*) AS cnt
  FROM hive."usage".network_activity_moc_ccn
  WHERE start_of_charge_dt = date_format(CAST('2026-10-05' AS DATE) - INTERVAL '1' DAY, '%Y-%m-%d')
  GROUP BY 1
) g ON r.start_of_charge_dt = g.start_of_charge_dt

UNION ALL

-- ================= MTC =================
SELECT
  COALESCE(g.start_of_charge_dt, r.start_of_charge_dt),
  'retention_vs_golden',
  'MTC',
  g.cnt,
  r.cnt,
  r.cnt - g.cnt,
  ((r.cnt - g.cnt) * 100.0) / r.cnt
FROM (
  SELECT start_of_charge_dt, COUNT(*) AS cnt
  FROM hive."usage".network_activity_mtc_retention
  WHERE (
      entry_date IN (
        date_format(CAST('2026-10-05' AS DATE) - INTERVAL '1' DAY, '%Y-%m-%d'),
        date_format(CAST('2026-10-05' AS DATE) - INTERVAL '2' DAY, '%Y-%m-%d')
      )
      OR (
        entry_date = date_format(CAST('2026-10-05' AS DATE), '%Y-%m-%d')
        AND entry_hour IN ('0','1','2','3','4')
      )
    )
    AND day = date_format(CAST('2026-10-05' AS DATE) - INTERVAL '1' DAY, '%Y-%m-%d')
  GROUP BY 1
) r
LEFT JOIN (
  SELECT start_of_charge_dt, COUNT(*) AS cnt
  FROM hive."usage".network_activity_mtc
  WHERE start_of_charge_dt = date_format(CAST('2026-10-05' AS DATE) - INTERVAL '1' DAY, '%Y-%m-%d')
  GROUP BY 1
) g ON r.start_of_charge_dt = g.start_of_charge_dt

UNION ALL

-- ================= SMS CCN =================
SELECT
  COALESCE(g.start_of_charge_dt, r.start_of_charge_dt),
  'retention_vs_golden',
  'SMS_CCN',
  g.cnt,
  r.cnt,
  r.cnt - g.cnt,
  ((r.cnt - g.cnt) * 100.0) / r.cnt
FROM (
  SELECT start_of_charge_dt, COUNT(*) AS cnt
  FROM hive."usage".network_activity_sms_ccn_retention
  WHERE (
      entry_date IN (
        date_format(CAST('2026-10-05' AS DATE) - INTERVAL '1' DAY, '%Y-%m-%d'),
        date_format(CAST('2026-10-05' AS DATE) - INTERVAL '2' DAY, '%Y-%m-%d')
      )
      OR (
        entry_date = date_format(CAST('2026-10-05' AS DATE), '%Y-%m-%d')
        AND entry_hour IN ('0','1','2','3','4')
      )
    )
    AND day = date_format(CAST('2026-10-05' AS DATE) - INTERVAL '1' DAY, '%Y-%m-%d')
  GROUP BY 1
) r
LEFT JOIN (
  SELECT start_of_charge_dt, COUNT(*) AS cnt
  FROM hive."usage".network_activity_sms_ccn
  WHERE start_of_charge_dt = date_format(CAST('2026-10-05' AS DATE) - INTERVAL '1' DAY, '%Y-%m-%d')
  GROUP BY 1
) g ON r.start_of_charge_dt = g.start_of_charge_dt

UNION ALL

-- ================= SMS MSC =================
SELECT
  COALESCE(g.start_of_charge_dt, r.start_of_charge_dt),
  'retention_vs_golden',
  'SMS_MSC',
  g.cnt,
  r.cnt,
  r.cnt - g.cnt,
  ((r.cnt - g.cnt) * 100.0) / r.cnt
FROM (
  SELECT start_of_charge_dt, COUNT(*) AS cnt
  FROM hive."usage".network_activity_sms_msc_retention
  WHERE (
      entry_date IN (
        date_format(CAST('2026-10-05' AS DATE) - INTERVAL '1' DAY, '%Y-%m-%d'),
        date_format(CAST('2026-10-05' AS DATE) - INTERVAL '2' DAY, '%Y-%m-%d')
      )
      OR (
        entry_date = date_format(CAST('2026-10-05' AS DATE), '%Y-%m-%d')
        AND entry_hour IN ('0','1','2','3','4')
      )
    )
    AND day = date_format(CAST('2026-10-05' AS DATE) - INTERVAL '1' DAY, '%Y-%m-%d')
  GROUP BY 1
) r
LEFT JOIN (
  SELECT start_of_charge_dt, COUNT(*) AS cnt
  FROM hive."usage".network_activity_sms_msc
  WHERE start_of_charge_dt = date_format(CAST('2026-10-05' AS DATE) - INTERVAL '1' DAY, '%Y-%m-%d')
  GROUP BY 1
) g ON r.start_of_charge_dt = g.start_of_charge_dt
;
