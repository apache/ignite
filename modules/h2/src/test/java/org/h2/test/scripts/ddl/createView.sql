-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE VIEW TEST_VIEW(A) AS SELECT 'a';
> ok

CREATE OR REPLACE VIEW TEST_VIEW(B, C) AS SELECT 'b', 'c';
> ok

SELECT * FROM TEST_VIEW;
> B C
> - -
> b c
> rows: 1
