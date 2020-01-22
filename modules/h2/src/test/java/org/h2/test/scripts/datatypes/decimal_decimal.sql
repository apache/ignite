-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--
-- h2.bigDecimalIsDecimal=true
--

create memory table orders ( orderid varchar(10), name varchar(20),  customer_id varchar(10), completed numeric(1) not null, verified numeric(1) );
> ok

select * from information_schema.columns where table_name = 'ORDERS';
> TABLE_CATALOG TABLE_SCHEMA TABLE_NAME COLUMN_NAME ORDINAL_POSITION COLUMN_DEFAULT IS_NULLABLE DATA_TYPE CHARACTER_MAXIMUM_LENGTH CHARACTER_OCTET_LENGTH NUMERIC_PRECISION NUMERIC_PRECISION_RADIX NUMERIC_SCALE CHARACTER_SET_NAME COLLATION_NAME TYPE_NAME NULLABLE IS_COMPUTED SELECTIVITY CHECK_CONSTRAINT SEQUENCE_NAME REMARKS SOURCE_DATA_TYPE COLUMN_TYPE         COLUMN_ON_UPDATE
> ------------- ------------ ---------- ----------- ---------------- -------------- ----------- --------- ------------------------ ---------------------- ----------------- ----------------------- ------------- ------------------ -------------- --------- -------- ----------- ----------- ---------------- ------------- ------- ---------------- ------------------- ----------------
> SCRIPT        PUBLIC       ORDERS     COMPLETED   4                null           NO          3         1                        1                      1                 10                      0             Unicode            OFF            DECIMAL   0        FALSE       50                           null                  null             NUMERIC(1) NOT NULL null
> SCRIPT        PUBLIC       ORDERS     CUSTOMER_ID 3                null           YES         12        10                       10                     10                10                      0             Unicode            OFF            VARCHAR   1        FALSE       50                           null                  null             VARCHAR(10)         null
> SCRIPT        PUBLIC       ORDERS     NAME        2                null           YES         12        20                       20                     20                10                      0             Unicode            OFF            VARCHAR   1        FALSE       50                           null                  null             VARCHAR(20)         null
> SCRIPT        PUBLIC       ORDERS     ORDERID     1                null           YES         12        10                       10                     10                10                      0             Unicode            OFF            VARCHAR   1        FALSE       50                           null                  null             VARCHAR(10)         null
> SCRIPT        PUBLIC       ORDERS     VERIFIED    5                null           YES         3         1                        1                      1                 10                      0             Unicode            OFF            DECIMAL   1        FALSE       50                           null                  null             NUMERIC(1)          null
> rows: 5

drop table orders;
> ok

CREATE TABLE TEST(ID INT, X1 BIT, XT TINYINT, X_SM SMALLINT, XB BIGINT, XD DECIMAL(10,2), XD2 DOUBLE PRECISION, XR REAL);
> ok

INSERT INTO TEST VALUES(?, ?, ?, ?, ?, ?, ?, ?);
{
0,FALSE,0,0,0,0.0,0.0,0.0
1,TRUE,1,1,1,1.0,1.0,1.0
4,TRUE,4,4,4,4.0,4.0,4.0
-1,FALSE,-1,-1,-1,-1.0,-1.0,-1.0
NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL
};
> update count: 5

SELECT ID, CAST(XT AS NUMBER(10,1)),
CAST(X_SM AS NUMBER(10,1)), CAST(XB AS NUMBER(10,1)), CAST(XD AS NUMBER(10,1)),
CAST(XD2 AS NUMBER(10,1)), CAST(XR AS NUMBER(10,1)) FROM TEST;
> ID   CAST(XT AS DECIMAL(10, 1)) CAST(X_SM AS DECIMAL(10, 1)) CAST(XB AS DECIMAL(10, 1)) CAST(XD AS DECIMAL(10, 1)) CAST(XD2 AS DECIMAL(10, 1)) CAST(XR AS DECIMAL(10, 1))
> ---- -------------------------- ---------------------------- -------------------------- -------------------------- --------------------------- --------------------------
> -1   -1.0                       -1.0                         -1.0                       -1.0                       -1.0                        -1.0
> 0    0.0                        0.0                          0.0                        0.0                        0.0                         0.0
> 1    1.0                        1.0                          1.0                        1.0                        1.0                         1.0
> 4    4.0                        4.0                          4.0                        4.0                        4.0                         4.0
> null null                       null                         null                       null                       null                        null
> rows: 5
