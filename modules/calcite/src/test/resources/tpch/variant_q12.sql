-- noinspection SqlDialectInspectionForFile
-- noinspection SqlNoDataSourceInspectionForFile

-- This variant replaces the CASE statement from the Functional Query Definition with equivalent DECODE() syntax

SELECT
    l_shipmode,
    sum(decode(o_orderpriority, '1-URGENT', 1, '2-HIGH', 1, 0)) as
        high_line_count,
    sum(decode(o_orderpriority, '1-URGENT', 0, '2-HIGH', 0, 1)) as
        low_line_count
FROM
    orders,
    lineitem
WHERE
        o_orderkey = l_orderkey
  AND l_shipmode IN ('MAIL', 'SHIP')
  AND l_commitdate < l_receiptdate
  AND l_shipdate < l_commitdate
  AND l_receiptdate >= DATE '1994-01-01'
  AND l_receiptdate < DATE '1994-01-01' + INTERVAL '1' YEAR
GROUP BY
    l_shipmode
ORDER BY
    l_shipmode
