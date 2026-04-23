-- noinspection SqlDialectInspectionForFile
-- noinspection SqlNoDataSourceInspectionForFile

SELECT
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) AS revenue
FROM (
         SELECT
             n1.n_name                          AS supp_nation,
             n2.n_name                          AS cust_nation,
             extract(YEAR FROM l_shipdate)      AS l_year,
             l_extendedprice * (1 - l_discount) AS volume
         FROM
             supplier /*+ NO_INDEX(S_NK_proxy) */,
             lineitem,
             orders /*+ NO_INDEX(_key_PK_proxy), NO_INDEX(O_CK_proxy) */,
             customer /*+ NO_INDEX(_key_PK_proxy), NO_INDEX(C_NK_proxy) */,
             nation /*+ NO_INDEX(_key_PK_proxy) */ n1,
             nation /*+ NO_INDEX(_key_PK_proxy) */ n2
         WHERE
                 s_suppkey = l_suppkey
           AND o_orderkey = l_orderkey
           AND c_custkey = o_custkey
           AND s_nationkey = n1.n_nationkey
           AND c_nationkey = n2.n_nationkey
           AND (
                 (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
                 OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
             )
           AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
     ) AS shipping
GROUP BY
    supp_nation,
    cust_nation,
    l_year
ORDER BY
    supp_nation,
    cust_nation,
    l_year
