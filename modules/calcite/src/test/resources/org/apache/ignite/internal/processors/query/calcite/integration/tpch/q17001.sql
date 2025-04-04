-- using default substitutions
-- $ID$
-- TPC-H/TPC-R Small-Quantity-Order Revenue Query (Q17)
-- Functional Query Definition
-- Approved February 1998
--
-- IGNITE: adoption of q17 with a left-join.


select
    sum(l_extendedprice) / 7.0 as avg_yearly
from
    part
left join lineitem
    on p_partkey = l_partkey
where
    p_brand = 'Brand#23'
    and p_container = 'MED BOX'
    and l_quantity < (
        select
            0.2 * avg(l_quantity)
        from
            lineitem
        where
            l_partkey = p_partkey
    );
