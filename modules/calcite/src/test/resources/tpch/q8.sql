-- noinspection SqlDialectInspectionForFile
-- noinspection SqlNoDataSourceInspectionForFile

SELECT
    o_year,
    sum(CASE
            WHEN nation = 'BRAZIL'
                THEN volume
            ELSE 0
        END) / sum(volume) AS mkt_share
FROM (
         SELECT
             extract(YEAR FROM o_orderdate)     AS o_year,
             l_extendedprice * (1 - l_discount) AS volume,
             n2.n_name                          AS nation
         FROM
             part,
             supplier /*+ NO_INDEX(S_NK_proxy) */,
             lineitem,
             orders /*+ NO_INDEX(_key_PK_proxy) */,
             customer /*+ NO_INDEX(_key_PK_proxy) */,
             nation /*+ NO_INDEX(_key_PK_proxy) */ n1,
             nation /*+ NO_INDEX(_key_PK_proxy) */ n2,
             region /*+ NO_INDEX(_key_PK_proxy) */
         WHERE
                 p_partkey = l_partkey
           AND s_suppkey = l_suppkey
           AND l_orderkey = o_orderkey
           AND o_custkey = c_custkey
           AND c_nationkey = n1.n_nationkey
           AND n1.n_regionkey = r_regionkey
           AND r_name = 'AMERICA'
           AND s_nationkey = n2.n_nationkey
           AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
           AND p_type = 'ECONOMY ANODIZED STEEL'
     ) AS all_nations
GROUP BY
    o_year
ORDER BY
    o_year
