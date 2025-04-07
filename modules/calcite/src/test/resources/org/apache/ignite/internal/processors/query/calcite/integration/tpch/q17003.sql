-- using default substitutions
-- $ID$
-- TPC-H/TPC-R Small-Quantity-Order Revenue Query (Q17)
-- Functional Query Definition
-- Approved February 1998
--
-- IGNITE: adoption of q17 with an outer-join.


select
    sum(l_extendedprice) / 7.0 as avg_yearly
from
    lineitem
full outer join part
    on p_partkey = l_partkey
where
    l_quantity < (
        select
            0.2 * avg(l_quantity)
        from
            lineitem
        where
            l_partkey = p_partkey
    );
