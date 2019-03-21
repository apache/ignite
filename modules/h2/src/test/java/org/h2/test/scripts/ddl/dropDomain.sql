-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE DOMAIN E AS ENUM('A', 'B');
> ok

CREATE DOMAIN E_NN AS ENUM('A', 'B') NOT NULL;
> ok

CREATE TABLE TEST(I INT PRIMARY KEY, E1 E, E2 E NOT NULL, E3 E_NN, E4 E_NN NULL);
> ok

INSERT INTO TEST VALUES (1, 'A', 'B', 'A', 'B');
> update count: 1

SELECT COLUMN_NAME, DOMAIN_CATALOG, DOMAIN_SCHEMA, DOMAIN_NAME, NULLABLE, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DOMAIN_CATALOG DOMAIN_SCHEMA DOMAIN_NAME NULLABLE COLUMN_TYPE
> ----------- -------------- ------------- ----------- -------- ---------------
> I           null           null          null        0        INT NOT NULL
> E1          SCRIPT         PUBLIC        E           1        "E"
> E2          SCRIPT         PUBLIC        E           0        "E" NOT NULL
> E3          SCRIPT         PUBLIC        E_NN        0        "E_NN" NOT NULL
> E4          SCRIPT         PUBLIC        E_NN        1        "E_NN" NULL
> rows (ordered): 5

DROP DOMAIN E RESTRICT;
> exception CANNOT_DROP_2

DROP DOMAIN E CASCADE;
> ok

DROP DOMAIN E_NN CASCADE;
> ok

SELECT COLUMN_NAME, DOMAIN_CATALOG, DOMAIN_SCHEMA, DOMAIN_NAME, NULLABLE, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DOMAIN_CATALOG DOMAIN_SCHEMA DOMAIN_NAME NULLABLE COLUMN_TYPE
> ----------- -------------- ------------- ----------- -------- -----------------------
> I           null           null          null        0        INT NOT NULL
> E1          null           null          null        1        ENUM('A', 'B')
> E2          null           null          null        0        ENUM('A', 'B') NOT NULL
> E3          null           null          null        0        ENUM('A', 'B') NOT NULL
> E4          null           null          null        1        ENUM('A', 'B')
> rows (ordered): 5

DROP TABLE TEST;
> ok
