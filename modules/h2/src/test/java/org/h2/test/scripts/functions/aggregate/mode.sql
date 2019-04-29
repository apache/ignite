-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(V INT);
> ok

SELECT MODE(V) FROM TEST;
>> null

SELECT MODE(DISTINCT V) FROM TEST;
> exception SYNTAX_ERROR_2

INSERT INTO TEST VALUES (NULL);
> update count: 1

SELECT MODE(V) FROM TEST;
>> null

INSERT INTO TEST VALUES (1), (2), (3), (1), (2), (1);
> update count: 6

SELECT MODE(V), MODE() WITHIN GROUP (ORDER BY V DESC) FROM TEST;
> MODE() WITHIN GROUP (ORDER BY V) MODE() WITHIN GROUP (ORDER BY V DESC)
> -------------------------------- -------------------------------------
> 1                                1
> rows: 1

SELECT MODE(V) FILTER (WHERE (V > 1)), MODE(V) FILTER (WHERE (V < 0)) FROM TEST;
> MODE() WITHIN GROUP (ORDER BY V) FILTER (WHERE (V > 1)) MODE() WITHIN GROUP (ORDER BY V) FILTER (WHERE (V < 0))
> ------------------------------------------------------- -------------------------------------------------------
> 2                                                       null
> rows: 1

-- Oracle compatibility
SELECT STATS_MODE(V) FROM TEST;
>> 1

INSERT INTO TEST VALUES (2), (3), (3);
> update count: 3

SELECT MODE(V ORDER BY V) FROM TEST;
>> 1

SELECT MODE(V ORDER BY V ASC) FROM TEST;
>> 1

SELECT MODE(V ORDER BY V DESC) FROM TEST;
>> 3

SELECT MODE(V ORDER BY V + 1) FROM TEST;
> exception IDENTICAL_EXPRESSIONS_SHOULD_BE_USED

SELECT MODE() WITHIN GROUP (ORDER BY V) FROM TEST;
>> 1

SELECT MODE() WITHIN GROUP (ORDER BY V ASC) FROM TEST;
>> 1

SELECT MODE() WITHIN GROUP (ORDER BY V DESC) FROM TEST;
>> 3

SELECT
    MODE() WITHIN GROUP (ORDER BY V) OVER () MA,
    MODE() WITHIN GROUP (ORDER BY V DESC) OVER () MD,
    MODE() WITHIN GROUP (ORDER BY V) OVER (ORDER BY V) MWA,
    MODE() WITHIN GROUP (ORDER BY V DESC) OVER (ORDER BY V) MWD,
    V FROM TEST;
> MA MD MWA  MWD  V
> -- -- ---- ---- ----
> 1  3  1    1    1
> 1  3  1    1    1
> 1  3  1    1    1
> 1  3  1    2    2
> 1  3  1    2    2
> 1  3  1    2    2
> 1  3  1    3    3
> 1  3  1    3    3
> 1  3  1    3    3
> 1  3  null null null
> rows: 10

DROP TABLE TEST;
> ok

CREATE TABLE TEST (N NUMERIC) AS VALUES (0), (0.0), (NULL);
> ok

SELECT MODE(N) FROM TEST;
>> 0

DROP TABLE TEST;
> ok
