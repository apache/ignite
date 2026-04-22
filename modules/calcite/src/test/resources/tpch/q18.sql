-- noinspection SqlDialectInspectionForFile
-- noinspection SqlNoDataSourceInspectionForFile

SELECT
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
FROM
    customer /*+ NO_INDEX(_key_PK_proxy), NO_INDEX(C_NK_proxy) */,
    orders /*+ NO_INDEX(_key_PK_proxy), NO_INDEX(O_CK_proxy) */,
    lineitem
WHERE
        o_orderkey IN (
        SELECT l_orderkey
        FROM
            lineitem
        GROUP BY
            l_orderkey
        HAVING
                sum(l_quantity) > 300
    )
  AND c_custkey = o_custkey
  AND o_orderkey = l_orderkey
GROUP BY
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
ORDER BY
    o_totalprice DESC,
    o_orderdate
    LIMIT 100
