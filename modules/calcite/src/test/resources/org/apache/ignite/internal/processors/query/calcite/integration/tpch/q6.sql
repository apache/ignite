-- using default substitutions
-- $ID$
-- TPC-H/TPC-R Forecasting Revenue Change Query (Q6)
-- Functional Query Definition
-- Approved February 1998


select
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem
where
    l_shipdate >= date '1994-01-01'
--    and l_shipdate < date '1994-01-01' + interval '1' year
    and l_shipdate < TIMESTAMPADD(YEAR, 1, date '1994-01-01')
    and l_discount between .06 - 0.01 and .06 + 0.01
    and l_quantity < 24;
