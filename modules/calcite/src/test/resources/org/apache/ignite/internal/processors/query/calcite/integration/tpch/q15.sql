-- using default substitutions
-- $ID$
-- TPC-H/TPC-R Top Supplier Query (Q15)
-- Functional Query Definition
-- Approved February 1998

create OR REPLACE view revenue0 as
    select
        l_suppkey as supplier_no,
        sum(l_extendedprice * (1 - l_discount)) as total_revenue
    from
        lineitem
    where
        l_shipdate >= date '1996-01-01'
--        and l_shipdate < date '1996-01-01' + interval '3' month
        and l_shipdate < TIMESTAMPADD(MONTH, 3, date '1996-01-01')
    group by
        l_suppkey;


select
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
from
    supplier,
    revenue0
where
    s_suppkey = supplier_no
    and total_revenue = (
        select
            max(total_revenue)
        from
            revenue0
    )
order by
    s_suppkey;
