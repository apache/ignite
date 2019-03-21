-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: Alex Nordlund
--

-- with filter condition

create table test(v varchar);
> ok

insert into test values ('1'), ('2'), ('3'), ('4'), ('5'), ('6'), ('7'), ('8'), ('9');
> update count: 9

select array_agg(v order by v asc),
    array_agg(v order by v desc) filter (where v >= '4')
    from test where v >= '2';
> ARRAY_AGG(V ORDER BY V)  ARRAY_AGG(V ORDER BY V DESC) FILTER (WHERE (V >= '4'))
> ------------------------ ------------------------------------------------------
> [2, 3, 4, 5, 6, 7, 8, 9] [9, 8, 7, 6, 5, 4]
> rows: 1

create index test_idx on test(v);
> ok

select ARRAY_AGG(v order by v asc),
    ARRAY_AGG(v order by v desc) filter (where v >= '4')
    from test where v >= '2';
> ARRAY_AGG(V ORDER BY V)  ARRAY_AGG(V ORDER BY V DESC) FILTER (WHERE (V >= '4'))
> ------------------------ ------------------------------------------------------
> [2, 3, 4, 5, 6, 7, 8, 9] [9, 8, 7, 6, 5, 4]
> rows: 1

select ARRAY_AGG(v order by v asc),
    ARRAY_AGG(v order by v desc) filter (where v >= '4')
    from test;
> ARRAY_AGG(V ORDER BY V)     ARRAY_AGG(V ORDER BY V DESC) FILTER (WHERE (V >= '4'))
> --------------------------- ------------------------------------------------------
> [1, 2, 3, 4, 5, 6, 7, 8, 9] [9, 8, 7, 6, 5, 4]
> rows: 1

drop table test;
> ok

create table test (id int auto_increment primary key, v int);
> ok

insert into test(v) values (7), (2), (8), (3), (7), (3), (9), (-1);
> update count: 8

select array_agg(v) from test;
> ARRAY_AGG(V)
> -------------------------
> [7, 2, 8, 3, 7, 3, 9, -1]
> rows: 1

select array_agg(distinct v) from test;
> ARRAY_AGG(DISTINCT V)
> ---------------------
> [-1, 2, 3, 7, 8, 9]
> rows: 1

select array_agg(distinct v order by v desc) from test;
> ARRAY_AGG(DISTINCT V ORDER BY V DESC)
> -------------------------------------
> [9, 8, 7, 3, 2, -1]
> rows: 1

drop table test;
> ok

CREATE TABLE TEST (ID INT PRIMARY KEY, NAME VARCHAR);
> ok

INSERT INTO TEST VALUES (1, 'a'), (2, 'a'), (3, 'b'), (4, 'c'), (5, 'c'), (6, 'c');
> update count: 6

SELECT ARRAY_AGG(ID), NAME FROM TEST;
> exception MUST_GROUP_BY_COLUMN_1

SELECT ARRAY_AGG(ID ORDER BY ID), NAME FROM TEST GROUP BY NAME;
> ARRAY_AGG(ID ORDER BY ID) NAME
> ------------------------- ----
> [1, 2]                    a
> [3]                       b
> [4, 5, 6]                 c
> rows: 3

SELECT ARRAY_AGG(ID ORDER BY ID) OVER (), NAME FROM TEST;
> ARRAY_AGG(ID ORDER BY ID) OVER () NAME
> --------------------------------- ----
> [1, 2, 3, 4, 5, 6]                a
> [1, 2, 3, 4, 5, 6]                a
> [1, 2, 3, 4, 5, 6]                b
> [1, 2, 3, 4, 5, 6]                c
> [1, 2, 3, 4, 5, 6]                c
> [1, 2, 3, 4, 5, 6]                c
> rows: 6

SELECT ARRAY_AGG(ID ORDER BY ID) OVER (PARTITION BY NAME), NAME FROM TEST;
> ARRAY_AGG(ID ORDER BY ID) OVER (PARTITION BY NAME) NAME
> -------------------------------------------------- ----
> [1, 2]                                             a
> [1, 2]                                             a
> [3]                                                b
> [4, 5, 6]                                          c
> [4, 5, 6]                                          c
> [4, 5, 6]                                          c
> rows: 6

SELECT
    ARRAY_AGG(ID ORDER BY ID) FILTER (WHERE ID < 3 OR ID > 4) OVER (PARTITION BY NAME) A,
    ARRAY_AGG(ID ORDER BY ID) FILTER (WHERE ID < 3 OR ID > 4) OVER (PARTITION BY NAME ORDER BY ID) AO,
    ID, NAME FROM TEST ORDER BY ID;
> A      AO     ID NAME
> ------ ------ -- ----
> [1, 2] [1]    1  a
> [1, 2] [1, 2] 2  a
> null   null   3  b
> [5, 6] null   4  c
> [5, 6] [5]    5  c
> [5, 6] [5, 6] 6  c
> rows (ordered): 6

SELECT
    ARRAY_AGG(ID ORDER BY ID) FILTER (WHERE ID < 3 OR ID > 4)
    OVER (ORDER BY ID ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) A,
    ID FROM TEST ORDER BY ID;
> A      ID
> ------ --
> [1, 2] 1
> [1, 2] 2
> [2]    3
> [5]    4
> [5, 6] 5
> [5, 6] 6
> rows (ordered): 6

SELECT ARRAY_AGG(SUM(ID)) OVER () FROM TEST;
> ARRAY_AGG(SUM(ID)) OVER ()
> --------------------------
> [21]
> rows: 1

SELECT ARRAY_AGG(ID ORDER BY ID) OVER() FROM TEST GROUP BY ID ORDER BY ID;
> ARRAY_AGG(ID ORDER BY ID) OVER ()
> ---------------------------------
> [1, 2, 3, 4, 5, 6]
> [1, 2, 3, 4, 5, 6]
> [1, 2, 3, 4, 5, 6]
> [1, 2, 3, 4, 5, 6]
> [1, 2, 3, 4, 5, 6]
> [1, 2, 3, 4, 5, 6]
> rows (ordered): 6

SELECT ARRAY_AGG(NAME) OVER(PARTITION BY NAME) FROM TEST GROUP BY NAME;
> ARRAY_AGG(NAME) OVER (PARTITION BY NAME)
> ----------------------------------------
> [a]
> [b]
> [c]
> rows: 3

SELECT ARRAY_AGG(ARRAY_AGG(ID ORDER BY ID)) OVER (PARTITION BY NAME), NAME FROM TEST GROUP BY NAME;
> ARRAY_AGG(ARRAY_AGG(ID ORDER BY ID)) OVER (PARTITION BY NAME) NAME
> ------------------------------------------------------------- ----
> [[1, 2]]                                                      a
> [[3]]                                                         b
> [[4, 5, 6]]                                                   c
> rows: 3

SELECT ARRAY_AGG(ARRAY_AGG(ID ORDER BY ID)) OVER (PARTITION BY NAME), NAME FROM TEST
    WHERE ID <> 5
    GROUP BY NAME HAVING ARRAY_AGG(ID ORDER BY ID)[1] > 1
    QUALIFY ARRAY_AGG(ARRAY_AGG(ID ORDER BY ID)) OVER (PARTITION BY NAME) <> ARRAY[ARRAY[3]];
> ARRAY_AGG(ARRAY_AGG(ID ORDER BY ID)) OVER (PARTITION BY NAME) NAME
> ------------------------------------------------------------- ----
> [[4, 6]]                                                      c
> rows: 1

EXPLAIN
    SELECT ARRAY_AGG(ARRAY_AGG(ID ORDER BY ID)) OVER (PARTITION BY NAME), NAME FROM TEST
    WHERE ID <> 5
    GROUP BY NAME HAVING ARRAY_AGG(ID ORDER BY ID)[1] > 1
    QUALIFY ARRAY_AGG(ARRAY_AGG(ID ORDER BY ID)) OVER (PARTITION BY NAME) <> ARRAY[ARRAY[3]];
>> SELECT ARRAY_AGG(ARRAY_AGG("ID" ORDER BY "ID")) OVER (PARTITION BY "NAME"), "NAME" FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */ WHERE "ID" <> 5 GROUP BY "NAME" HAVING ARRAY_GET(ARRAY_AGG("ID" ORDER BY "ID"), 1) > 1 QUALIFY ARRAY_AGG(ARRAY_AGG("ID" ORDER BY "ID")) OVER (PARTITION BY "NAME") <> ARRAY [ARRAY [3]]

SELECT ARRAY_AGG(ARRAY_AGG(ID ORDER BY ID)) OVER (PARTITION BY NAME), NAME FROM TEST
    GROUP BY NAME ORDER BY NAME OFFSET 1 ROW;
> ARRAY_AGG(ARRAY_AGG(ID ORDER BY ID)) OVER (PARTITION BY NAME) NAME
> ------------------------------------------------------------- ----
> [[3]]                                                         b
> [[4, 5, 6]]                                                   c
> rows (ordered): 2

SELECT ARRAY_AGG(ARRAY_AGG(ID ORDER BY ID)) FILTER (WHERE NAME > 'b') OVER (PARTITION BY NAME), NAME FROM TEST
    GROUP BY NAME ORDER BY NAME;
> ARRAY_AGG(ARRAY_AGG(ID ORDER BY ID)) FILTER (WHERE (NAME > 'b')) OVER (PARTITION BY NAME) NAME
> ----------------------------------------------------------------------------------------- ----
> null                                                                                      a
> null                                                                                      b
> [[4, 5, 6]]                                                                               c
> rows (ordered): 3

SELECT ARRAY_AGG(ARRAY_AGG(ID ORDER BY ID)) FILTER (WHERE NAME > 'c') OVER (PARTITION BY NAME), NAME FROM TEST
    GROUP BY NAME ORDER BY NAME;
> ARRAY_AGG(ARRAY_AGG(ID ORDER BY ID)) FILTER (WHERE (NAME > 'c')) OVER (PARTITION BY NAME) NAME
> ----------------------------------------------------------------------------------------- ----
> null                                                                                      a
> null                                                                                      b
> null                                                                                      c
> rows (ordered): 3

SELECT ARRAY_AGG(ARRAY_AGG(ID ORDER BY ID)) FILTER (WHERE NAME > 'b') OVER () FROM TEST GROUP BY NAME ORDER BY NAME;
> ARRAY_AGG(ARRAY_AGG(ID ORDER BY ID)) FILTER (WHERE (NAME > 'b')) OVER ()
> ------------------------------------------------------------------------
> [[4, 5, 6]]
> [[4, 5, 6]]
> [[4, 5, 6]]
> rows (ordered): 3

SELECT ARRAY_AGG(ARRAY_AGG(ID ORDER BY ID)) FILTER (WHERE NAME > 'c') OVER () FROM TEST GROUP BY NAME ORDER BY NAME;
> ARRAY_AGG(ARRAY_AGG(ID ORDER BY ID)) FILTER (WHERE (NAME > 'c')) OVER ()
> ------------------------------------------------------------------------
> null
> null
> null
> rows (ordered): 3

SELECT ARRAY_AGG(ID) OVER() FROM TEST GROUP BY NAME;
> exception MUST_GROUP_BY_COLUMN_1

SELECT ARRAY_AGG(ID) OVER(PARTITION BY NAME ORDER BY ID), NAME FROM TEST;
> ARRAY_AGG(ID) OVER (PARTITION BY NAME ORDER BY ID) NAME
> -------------------------------------------------- ----
> [1, 2]                                             a
> [1]                                                a
> [3]                                                b
> [4, 5, 6]                                          c
> [4, 5]                                             c
> [4]                                                c
> rows: 6

SELECT ARRAY_AGG(ID) OVER(PARTITION BY NAME ORDER BY ID DESC), NAME FROM TEST;
> ARRAY_AGG(ID) OVER (PARTITION BY NAME ORDER BY ID DESC) NAME
> ------------------------------------------------------- ----
> [2, 1]                                                  a
> [2]                                                     a
> [3]                                                     b
> [6, 5, 4]                                               c
> [6, 5]                                                  c
> [6]                                                     c
> rows: 6

SELECT
    ARRAY_AGG(ID ORDER BY ID) OVER(PARTITION BY NAME ORDER BY ID DESC) A,
    ARRAY_AGG(ID) OVER(PARTITION BY NAME ORDER BY ID DESC) D,
    NAME FROM TEST;
> A         D         NAME
> --------- --------- ----
> [1, 2]    [2, 1]    a
> [2]       [2]       a
> [3]       [3]       b
> [4, 5, 6] [6, 5, 4] c
> [5, 6]    [6, 5]    c
> [6]       [6]       c
> rows: 6

SELECT ARRAY_AGG(SUM(ID)) OVER(ORDER BY ID) FROM TEST GROUP BY ID;
> ARRAY_AGG(SUM(ID)) OVER (ORDER BY ID)
> -------------------------------------
> [1, 2, 3, 4, 5, 6]
> [1, 2, 3, 4, 5]
> [1, 2, 3, 4]
> [1, 2, 3]
> [1, 2]
> [1]
> rows: 6

DROP TABLE TEST;
> ok

CREATE TABLE TEST(ID INT, G INT);
> ok

INSERT INTO TEST VALUES
    (1, 1),
    (2, 2),
    (3, 2),
    (4, 2),
    (5, 3);
> update count: 5

SELECT
    ARRAY_AGG(ID) OVER (ORDER BY G RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) D,
    ARRAY_AGG(ID) OVER (ORDER BY G RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) R,
    ARRAY_AGG(ID) OVER (ORDER BY G RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) G,
    ARRAY_AGG(ID) OVER (ORDER BY G RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) T,
    ARRAY_AGG(ID) OVER (ORDER BY G RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE NO OTHERS) N
    FROM TEST;
> D               R            G            T               N
> --------------- ------------ ------------ --------------- ---------------
> [1, 2, 3, 4, 5] [1, 2, 3, 4] [1, 2, 3, 4] [1, 2, 3, 4, 5] [1, 2, 3, 4, 5]
> [1, 2, 3, 4, 5] [1, 2, 3, 5] [1, 5]       [1, 4, 5]       [1, 2, 3, 4, 5]
> [1, 2, 3, 4, 5] [1, 2, 4, 5] [1, 5]       [1, 3, 5]       [1, 2, 3, 4, 5]
> [1, 2, 3, 4, 5] [1, 3, 4, 5] [1, 5]       [1, 2, 5]       [1, 2, 3, 4, 5]
> [1, 2, 3, 4, 5] [2, 3, 4, 5] [2, 3, 4, 5] [1, 2, 3, 4, 5] [1, 2, 3, 4, 5]
> rows: 5

DROP TABLE TEST;
> ok

CREATE TABLE TEST(ID INT, VALUE INT);
> ok

INSERT INTO TEST VALUES
    (1, 1),
    (2, 1),
    (3, 5),
    (4, 8),
    (5, 8),
    (6, 8),
    (7, 9),
    (8, 9);
> update count: 8

SELECT *,
    ARRAY_AGG(ID) OVER (ORDER BY VALUE ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) R_ID,
    ARRAY_AGG(VALUE) OVER (ORDER BY VALUE ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) R_V,
    ARRAY_AGG(ID) OVER (ORDER BY VALUE RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) V_ID,
    ARRAY_AGG(VALUE) OVER (ORDER BY VALUE RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) V_V,
    ARRAY_AGG(VALUE) OVER (ORDER BY VALUE DESC RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) V_V_R,
    ARRAY_AGG(ID) OVER (ORDER BY VALUE GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) G_ID,
    ARRAY_AGG(VALUE) OVER (ORDER BY VALUE GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) G_V
    FROM TEST;
> ID VALUE R_ID      R_V       V_ID            V_V             V_V_R           G_ID               G_V
> -- ----- --------- --------- --------------- --------------- --------------- ------------------ ------------------
> 1  1     [1, 2]    [1, 1]    [1, 2]          [1, 1]          [1, 1]          [1, 2, 3]          [1, 1, 5]
> 2  1     [1, 2, 3] [1, 1, 5] [1, 2]          [1, 1]          [1, 1]          [1, 2, 3]          [1, 1, 5]
> 3  5     [2, 3, 4] [1, 5, 8] [3]             [5]             [5]             [1, 2, 3, 4, 5, 6] [1, 1, 5, 8, 8, 8]
> 4  8     [3, 4, 5] [5, 8, 8] [4, 5, 6, 7, 8] [8, 8, 8, 9, 9] [9, 9, 8, 8, 8] [3, 4, 5, 6, 7, 8] [5, 8, 8, 8, 9, 9]
> 5  8     [4, 5, 6] [8, 8, 8] [4, 5, 6, 7, 8] [8, 8, 8, 9, 9] [9, 9, 8, 8, 8] [3, 4, 5, 6, 7, 8] [5, 8, 8, 8, 9, 9]
> 6  8     [5, 6, 7] [8, 8, 9] [4, 5, 6, 7, 8] [8, 8, 8, 9, 9] [9, 9, 8, 8, 8] [3, 4, 5, 6, 7, 8] [5, 8, 8, 8, 9, 9]
> 7  9     [6, 7, 8] [8, 9, 9] [4, 5, 6, 7, 8] [8, 8, 8, 9, 9] [9, 9, 8, 8, 8] [4, 5, 6, 7, 8]    [8, 8, 8, 9, 9]
> 8  9     [7, 8]    [9, 9]    [4, 5, 6, 7, 8] [8, 8, 8, 9, 9] [9, 9, 8, 8, 8] [4, 5, 6, 7, 8]    [8, 8, 8, 9, 9]
> rows: 8

SELECT *,
    ARRAY_AGG(ID ORDER BY ID) OVER (ORDER BY VALUE RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) A1,
    ARRAY_AGG(ID) OVER (ORDER BY VALUE RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) A2
    FROM TEST;
> ID VALUE A1                       A2
> -- ----- ------------------------ ------------------------
> 1  1     [1, 2, 3, 4, 5, 6, 7, 8] [1, 2]
> 2  1     [1, 2, 3, 4, 5, 6, 7, 8] [1, 2]
> 3  5     [3, 4, 5, 6, 7, 8]       [1, 2, 3]
> 4  8     [4, 5, 6, 7, 8]          [1, 2, 3, 4, 5, 6, 7, 8]
> 5  8     [4, 5, 6, 7, 8]          [1, 2, 3, 4, 5, 6, 7, 8]
> 6  8     [4, 5, 6, 7, 8]          [1, 2, 3, 4, 5, 6, 7, 8]
> 7  9     [4, 5, 6, 7, 8]          [1, 2, 3, 4, 5, 6, 7, 8]
> 8  9     [4, 5, 6, 7, 8]          [1, 2, 3, 4, 5, 6, 7, 8]
> rows: 8

SELECT *, ARRAY_AGG(ID) OVER (ORDER BY VALUE ROWS -1 PRECEDING) FROM TEST;
> exception INVALID_PRECEDING_OR_FOLLOWING_1

SELECT *, ARRAY_AGG(ID) OVER (ORDER BY ID ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) FROM TEST FETCH FIRST 4 ROWS ONLY;
> ID VALUE ARRAY_AGG(ID) OVER (ORDER BY ID ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
> -- ----- -------------------------------------------------------------------------
> 1  1     null
> 2  1     [1]
> 3  5     [1, 2]
> 4  8     [2, 3]
> rows: 4

SELECT *, ARRAY_AGG(ID) OVER (ORDER BY ID ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) FROM TEST OFFSET 4 ROWS;
> ID VALUE ARRAY_AGG(ID) OVER (ORDER BY ID ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING)
> -- ----- -------------------------------------------------------------------------
> 5  8     [6, 7]
> 6  8     [7, 8]
> 7  9     [8]
> 8  9     null
> rows: 4

SELECT *, ARRAY_AGG(ID) OVER (ORDER BY ID RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING) FROM TEST FETCH FIRST 4 ROWS ONLY;
> ID VALUE ARRAY_AGG(ID) OVER (ORDER BY ID RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING)
> -- ----- --------------------------------------------------------------------------
> 1  1     null
> 2  1     [1]
> 3  5     [1, 2]
> 4  8     [2, 3]
> rows: 4

SELECT *, ARRAY_AGG(ID) OVER (ORDER BY ID RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING) FROM TEST OFFSET 4 ROWS;
> ID VALUE ARRAY_AGG(ID) OVER (ORDER BY ID RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING)
> -- ----- --------------------------------------------------------------------------
> 5  8     [6, 7]
> 6  8     [7, 8]
> 7  9     [8]
> 8  9     null
> rows: 4

SELECT *,
    ARRAY_AGG(ID) OVER (ORDER BY VALUE GROUPS BETWEEN 0 PRECEDING AND 0 FOLLOWING) N,
    ARRAY_AGG(ID) OVER (ORDER BY VALUE GROUPS BETWEEN 0 PRECEDING AND 0 FOLLOWING EXCLUDE TIES) T,
    ARRAY_AGG(ID) OVER (ORDER BY VALUE GROUPS BETWEEN 1 PRECEDING AND 0 FOLLOWING EXCLUDE TIES) T1
    FROM TEST;
> ID VALUE N         T   T1
> -- ----- --------- --- ------------
> 1  1     [1, 2]    [1] [1]
> 2  1     [1, 2]    [2] [2]
> 3  5     [3]       [3] [1, 2, 3]
> 4  8     [4, 5, 6] [4] [3, 4]
> 5  8     [4, 5, 6] [5] [3, 5]
> 6  8     [4, 5, 6] [6] [3, 6]
> 7  9     [7, 8]    [7] [4, 5, 6, 7]
> 8  9     [7, 8]    [8] [4, 5, 6, 8]
> rows: 8

SELECT *,
    ARRAY_AGG(ID) OVER (ORDER BY VALUE GROUPS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) U_P,
    ARRAY_AGG(ID) OVER (ORDER BY VALUE GROUPS BETWEEN 2 PRECEDING AND 1 PRECEDING) P,
    ARRAY_AGG(ID) OVER (ORDER BY VALUE GROUPS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) F,
    ARRAY_AGG(ID ORDER BY ID) OVER (ORDER BY VALUE GROUPS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) U_F
    FROM TEST;
> ID VALUE U_P                P            F               U_F
> -- ----- ------------------ ------------ --------------- ------------------
> 1  1     null               null         [3, 4, 5, 6]    [3, 4, 5, 6, 7, 8]
> 2  1     null               null         [3, 4, 5, 6]    [3, 4, 5, 6, 7, 8]
> 3  5     [1, 2]             [1, 2]       [4, 5, 6, 7, 8] [4, 5, 6, 7, 8]
> 4  8     [1, 2, 3]          [1, 2, 3]    [7, 8]          [7, 8]
> 5  8     [1, 2, 3]          [1, 2, 3]    [7, 8]          [7, 8]
> 6  8     [1, 2, 3]          [1, 2, 3]    [7, 8]          [7, 8]
> 7  9     [1, 2, 3, 4, 5, 6] [3, 4, 5, 6] null            null
> 8  9     [1, 2, 3, 4, 5, 6] [3, 4, 5, 6] null            null
> rows: 8

SELECT *,
    ARRAY_AGG(ID) OVER (ORDER BY VALUE GROUPS BETWEEN 1 PRECEDING AND 0 PRECEDING) P,
    ARRAY_AGG(ID) OVER (ORDER BY VALUE GROUPS BETWEEN 0 FOLLOWING AND 1 FOLLOWING) F
    FROM TEST;
> ID VALUE P               F
> -- ----- --------------- ---------------
> 1  1     [1, 2]          [1, 2, 3]
> 2  1     [1, 2]          [1, 2, 3]
> 3  5     [1, 2, 3]       [3, 4, 5, 6]
> 4  8     [3, 4, 5, 6]    [4, 5, 6, 7, 8]
> 5  8     [3, 4, 5, 6]    [4, 5, 6, 7, 8]
> 6  8     [3, 4, 5, 6]    [4, 5, 6, 7, 8]
> 7  9     [4, 5, 6, 7, 8] [7, 8]
> 8  9     [4, 5, 6, 7, 8] [7, 8]
> rows: 8

SELECT ID, VALUE,
    ARRAY_AGG(ID) OVER (ORDER BY VALUE ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING EXCLUDE GROUP) G,
    ARRAY_AGG(ID) OVER (ORDER BY VALUE ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING EXCLUDE TIES) T
    FROM TEST;
> ID VALUE G            T
> -- ----- ------------ ---------------
> 1  1     [3]          [1, 3]
> 2  1     [3, 4]       [2, 3, 4]
> 3  5     [1, 2, 4, 5] [1, 2, 3, 4, 5]
> 4  8     [2, 3]       [2, 3, 4]
> 5  8     [3, 7]       [3, 5, 7]
> 6  8     [7, 8]       [6, 7, 8]
> 7  9     [5, 6]       [5, 6, 7]
> 8  9     [6]          [6, 8]
> rows: 8

SELECT ID, VALUE, ARRAY_AGG(ID) OVER(ORDER BY VALUE ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING EXCLUDE GROUP) G
    FROM TEST ORDER BY ID FETCH FIRST 3 ROWS ONLY;
> ID VALUE G
> -- ----- ------
> 1  1     [3]
> 2  1     [3, 4]
> 3  5     [4, 5]
> rows (ordered): 3

SELECT ID, VALUE, ARRAY_AGG(ID) OVER(ORDER BY VALUE ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING EXCLUDE GROUP) G
    FROM TEST ORDER BY ID FETCH FIRST 3 ROWS ONLY;
> ID VALUE G
> -- ----- ------
> 1  1     null
> 2  1     null
> 3  5     [1, 2]
> rows (ordered): 3

SELECT ID, VALUE, ARRAY_AGG(ID) OVER (ORDER BY VALUE RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING) A
    FROM TEST;
> ID VALUE A
> -- ----- ---------
> 1  1     null
> 2  1     null
> 3  5     null
> 4  8     null
> 5  8     null
> 6  8     null
> 7  9     [4, 5, 6]
> 8  9     [4, 5, 6]
> rows: 8

SELECT ID, VALUE,
    ARRAY_AGG(ID) OVER (ORDER BY VALUE ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) CP,
    ARRAY_AGG(ID ORDER BY ID) OVER (ORDER BY VALUE ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) CF,
    ARRAY_AGG(ID) OVER (ORDER BY VALUE RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) RP,
    ARRAY_AGG(ID ORDER BY ID) OVER (ORDER BY VALUE RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) RF,
    ARRAY_AGG(ID) OVER (ORDER BY VALUE GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) GP,
    ARRAY_AGG(ID ORDER BY ID) OVER (ORDER BY VALUE GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) GF
    FROM TEST;
> ID VALUE CP                       CF                       RP                       RF                       GP                       GF
> -- ----- ------------------------ ------------------------ ------------------------ ------------------------ ------------------------ ------------------------
> 1  1     [1]                      [1, 2, 3, 4, 5, 6, 7, 8] [1, 2]                   [1, 2, 3, 4, 5, 6, 7, 8] [1, 2]                   [1, 2, 3, 4, 5, 6, 7, 8]
> 2  1     [1, 2]                   [2, 3, 4, 5, 6, 7, 8]    [1, 2]                   [1, 2, 3, 4, 5, 6, 7, 8] [1, 2]                   [1, 2, 3, 4, 5, 6, 7, 8]
> 3  5     [1, 2, 3]                [3, 4, 5, 6, 7, 8]       [1, 2, 3]                [3, 4, 5, 6, 7, 8]       [1, 2, 3]                [3, 4, 5, 6, 7, 8]
> 4  8     [1, 2, 3, 4]             [4, 5, 6, 7, 8]          [1, 2, 3, 4, 5, 6]       [4, 5, 6, 7, 8]          [1, 2, 3, 4, 5, 6]       [4, 5, 6, 7, 8]
> 5  8     [1, 2, 3, 4, 5]          [5, 6, 7, 8]             [1, 2, 3, 4, 5, 6]       [4, 5, 6, 7, 8]          [1, 2, 3, 4, 5, 6]       [4, 5, 6, 7, 8]
> 6  8     [1, 2, 3, 4, 5, 6]       [6, 7, 8]                [1, 2, 3, 4, 5, 6]       [4, 5, 6, 7, 8]          [1, 2, 3, 4, 5, 6]       [4, 5, 6, 7, 8]
> 7  9     [1, 2, 3, 4, 5, 6, 7]    [7, 8]                   [1, 2, 3, 4, 5, 6, 7, 8] [7, 8]                   [1, 2, 3, 4, 5, 6, 7, 8] [7, 8]
> 8  9     [1, 2, 3, 4, 5, 6, 7, 8] [8]                      [1, 2, 3, 4, 5, 6, 7, 8] [7, 8]                   [1, 2, 3, 4, 5, 6, 7, 8] [7, 8]
> rows: 8

SELECT *, ARRAY_AGG(ID) OVER (ORDER BY ID RANGE BETWEEN CURRENT ROW AND 1 PRECEDING) FROM TEST;
> exception SYNTAX_ERROR_1

DROP TABLE TEST;
> ok

CREATE TABLE TEST (ID INT, VALUE INT);
> ok

INSERT INTO TEST VALUES
    (1, 1),
    (2, 1),
    (3, 2),
    (4, 2),
    (5, 3),
    (6, 3),
    (7, 4),
    (8, 4);
> update count: 8

SELECT *, ARRAY_AGG(ID) OVER (ORDER BY VALUE RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING) FROM TEST;
> ID VALUE ARRAY_AGG(ID) OVER (ORDER BY VALUE RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING)
> -- ----- -----------------------------------------------------------------------------
> 1  1     null
> 2  1     null
> 3  2     [1, 2]
> 4  2     [1, 2]
> 5  3     [1, 2, 3, 4]
> 6  3     [1, 2, 3, 4]
> 7  4     [3, 4, 5, 6]
> 8  4     [3, 4, 5, 6]
> rows: 8

SELECT *, ARRAY_AGG(ID) OVER (ORDER BY VALUE RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING) FROM TEST;
> ID VALUE ARRAY_AGG(ID) OVER (ORDER BY VALUE RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING)
> -- ----- -----------------------------------------------------------------------------
> 1  1     [3, 4, 5, 6]
> 2  1     [3, 4, 5, 6]
> 3  2     [5, 6, 7, 8]
> 4  2     [5, 6, 7, 8]
> 5  3     [7, 8]
> 6  3     [7, 8]
> 7  4     null
> 8  4     null
> rows: 8

SELECT ID, VALUE, ARRAY_AGG(ID) OVER (ORDER BY VALUE RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING EXCLUDE CURRENT ROW) A
    FROM TEST;
> ID VALUE A
> -- ----- ------------
> 1  1     null
> 2  1     null
> 3  2     [1, 2]
> 4  2     [1, 2]
> 5  3     [1, 2, 3, 4]
> 6  3     [1, 2, 3, 4]
> 7  4     [3, 4, 5, 6]
> 8  4     [3, 4, 5, 6]
> rows: 8

SELECT ID, VALUE, ARRAY_AGG(ID) OVER (ORDER BY VALUE RANGE BETWEEN 1 FOLLOWING AND 1 FOLLOWING EXCLUDE CURRENT ROW) A
    FROM TEST;
> ID VALUE A
> -- ----- ------
> 1  1     [3, 4]
> 2  1     [3, 4]
> 3  2     [5, 6]
> 4  2     [5, 6]
> 5  3     [7, 8]
> 6  3     [7, 8]
> 7  4     null
> 8  4     null
> rows: 8

SELECT ID, VALUE,
    ARRAY_AGG(ID) OVER (ORDER BY VALUE ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) CP,
    ARRAY_AGG(ID ORDER BY ID) OVER (ORDER BY VALUE ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) CF,
    ARRAY_AGG(ID) OVER (ORDER BY VALUE RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) RP,
    ARRAY_AGG(ID ORDER BY ID) OVER (ORDER BY VALUE RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) RF,
    ARRAY_AGG(ID) OVER (ORDER BY VALUE GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) GP,
    ARRAY_AGG(ID ORDER BY ID) OVER (ORDER BY VALUE GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) GF
    FROM TEST;
> ID VALUE CP                       CF                       RP                       RF                       GP                       GF
> -- ----- ------------------------ ------------------------ ------------------------ ------------------------ ------------------------ ------------------------
> 1  1     [1]                      [1, 2, 3, 4, 5, 6, 7, 8] [1, 2]                   [1, 2, 3, 4, 5, 6, 7, 8] [1, 2]                   [1, 2, 3, 4, 5, 6, 7, 8]
> 2  1     [1, 2]                   [2, 3, 4, 5, 6, 7, 8]    [1, 2]                   [1, 2, 3, 4, 5, 6, 7, 8] [1, 2]                   [1, 2, 3, 4, 5, 6, 7, 8]
> 3  2     [1, 2, 3]                [3, 4, 5, 6, 7, 8]       [1, 2, 3, 4]             [3, 4, 5, 6, 7, 8]       [1, 2, 3, 4]             [3, 4, 5, 6, 7, 8]
> 4  2     [1, 2, 3, 4]             [4, 5, 6, 7, 8]          [1, 2, 3, 4]             [3, 4, 5, 6, 7, 8]       [1, 2, 3, 4]             [3, 4, 5, 6, 7, 8]
> 5  3     [1, 2, 3, 4, 5]          [5, 6, 7, 8]             [1, 2, 3, 4, 5, 6]       [5, 6, 7, 8]             [1, 2, 3, 4, 5, 6]       [5, 6, 7, 8]
> 6  3     [1, 2, 3, 4, 5, 6]       [6, 7, 8]                [1, 2, 3, 4, 5, 6]       [5, 6, 7, 8]             [1, 2, 3, 4, 5, 6]       [5, 6, 7, 8]
> 7  4     [1, 2, 3, 4, 5, 6, 7]    [7, 8]                   [1, 2, 3, 4, 5, 6, 7, 8] [7, 8]                   [1, 2, 3, 4, 5, 6, 7, 8] [7, 8]
> 8  4     [1, 2, 3, 4, 5, 6, 7, 8] [8]                      [1, 2, 3, 4, 5, 6, 7, 8] [7, 8]                   [1, 2, 3, 4, 5, 6, 7, 8] [7, 8]
> rows: 8

SELECT ID, VALUE,
    ARRAY_AGG(ID ORDER BY ID) OVER (ORDER BY ID RANGE BETWEEN UNBOUNDED PRECEDING AND VALUE FOLLOWING) RG,
    ARRAY_AGG(ID ORDER BY ID) OVER (ORDER BY ID RANGE BETWEEN VALUE PRECEDING AND UNBOUNDED FOLLOWING) RGR,
    ARRAY_AGG(ID ORDER BY ID) OVER (ORDER BY ID ROWS BETWEEN UNBOUNDED PRECEDING AND VALUE FOLLOWING) R,
    ARRAY_AGG(ID ORDER BY ID) OVER (ORDER BY ID ROWS BETWEEN VALUE PRECEDING AND UNBOUNDED FOLLOWING) RR
    FROM TEST;
> ID VALUE RG                       RGR                      R                        RR
> -- ----- ------------------------ ------------------------ ------------------------ ------------------------
> 1  1     [1, 2]                   [1, 2, 3, 4, 5, 6, 7, 8] [1, 2]                   [1, 2, 3, 4, 5, 6, 7, 8]
> 2  1     [1, 2, 3]                [1, 2, 3, 4, 5, 6, 7, 8] [1, 2, 3]                [1, 2, 3, 4, 5, 6, 7, 8]
> 3  2     [1, 2, 3, 4, 5]          [1, 2, 3, 4, 5, 6, 7, 8] [1, 2, 3, 4, 5]          [1, 2, 3, 4, 5, 6, 7, 8]
> 4  2     [1, 2, 3, 4, 5, 6]       [2, 3, 4, 5, 6, 7, 8]    [1, 2, 3, 4, 5, 6]       [2, 3, 4, 5, 6, 7, 8]
> 5  3     [1, 2, 3, 4, 5, 6, 7, 8] [2, 3, 4, 5, 6, 7, 8]    [1, 2, 3, 4, 5, 6, 7, 8] [2, 3, 4, 5, 6, 7, 8]
> 6  3     [1, 2, 3, 4, 5, 6, 7, 8] [3, 4, 5, 6, 7, 8]       [1, 2, 3, 4, 5, 6, 7, 8] [3, 4, 5, 6, 7, 8]
> 7  4     [1, 2, 3, 4, 5, 6, 7, 8] [3, 4, 5, 6, 7, 8]       [1, 2, 3, 4, 5, 6, 7, 8] [3, 4, 5, 6, 7, 8]
> 8  4     [1, 2, 3, 4, 5, 6, 7, 8] [4, 5, 6, 7, 8]          [1, 2, 3, 4, 5, 6, 7, 8] [4, 5, 6, 7, 8]
> rows: 8

SELECT ID, VALUE,
    ARRAY_AGG(ID ORDER BY ID) OVER
        (PARTITION BY VALUE ORDER BY ID ROWS BETWEEN VALUE / 3 PRECEDING AND VALUE / 3 FOLLOWING) A,
    ARRAY_AGG(ID ORDER BY ID) OVER
        (PARTITION BY VALUE ORDER BY ID ROWS BETWEEN UNBOUNDED PRECEDING AND VALUE / 3 FOLLOWING) AP,
    ARRAY_AGG(ID ORDER BY ID) OVER
        (PARTITION BY VALUE ORDER BY ID ROWS BETWEEN VALUE / 3 PRECEDING AND UNBOUNDED FOLLOWING) AF
    FROM TEST;
> ID VALUE A      AP     AF
> -- ----- ------ ------ ------
> 1  1     [1]    [1]    [1, 2]
> 2  1     [2]    [1, 2] [2]
> 3  2     [3]    [3]    [3, 4]
> 4  2     [4]    [3, 4] [4]
> 5  3     [5, 6] [5, 6] [5, 6]
> 6  3     [5, 6] [5, 6] [5, 6]
> 7  4     [7, 8] [7, 8] [7, 8]
> 8  4     [7, 8] [7, 8] [7, 8]
> rows: 8

DROP TABLE TEST;
> ok
