-- noinspection SqlDialectInspectionForFile
-- noinspection SqlNoDataSourceInspectionForFile

SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    customer /*+ NO_INDEX(_key_PK_proxy), NO_INDEX(C_NK_proxy) */,
    orders,
    lineitem,
    supplier /*+ NO_INDEX(_key_PK_proxy), NO_INDEX(S_NK_proxy) */,
    nation /*+ NO_INDEX(_key_PK_proxy), NO_INDEX(N_RK_proxy) */,
    region /*+ NO_INDEX(_key_PK_proxy) */
WHERE
        c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'ASIA'
  AND o_orderdate >= DATE '1994-01-01'
  AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR
GROUP BY
    n_name
ORDER BY
    revenue DESC
