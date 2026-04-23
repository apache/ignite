-- noinspection SqlDialectInspectionForFile
-- noinspection SqlNoDataSourceInspectionForFile

-- -- This variant replaces the CASE statement from the Functional Query Definition with equivalent DECODE() syntax

SELECT
    100.00 * sum(decode(substring(p_type from 1 for 5), 'PROMO',
                        l_extendedprice * (1-l_discount), 0)) /
    sum(l_extendedprice * (1-l_discount)) as promo_revenue
FROM
    lineitem,
    part
WHERE
        l_partkey = p_partkey
  AND l_shipdate >= DATE '1995-09-01'
  AND l_shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH
