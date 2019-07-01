-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select ifnull(null, '1') x1, ifnull(null, null) xn, ifnull('a', 'b') xa;
> X1 XN   XA
> -- ---- --
> 1  null a
> rows: 1

select isnull(null, '1') x1, isnull(null, null) xn, isnull('a', 'b') xa;
> X1 XN   XA
> -- ---- --
> 1  null a
> rows: 1

CREATE MEMORY TABLE S(D DOUBLE) AS VALUES NULL;
> ok

CREATE MEMORY TABLE T AS SELECT IFNULL(D, D) FROM S;
> ok

SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'T';
>> DOUBLE

DROP TABLE S, T;
> ok
