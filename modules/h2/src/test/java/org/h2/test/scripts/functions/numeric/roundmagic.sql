-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select roundmagic(null) en, roundmagic(cast(3.11 as double) - 3.1) e001, roundmagic(3.11-3.1-0.01) e000, roundmagic(2000000000000) e20x;
> EN   E001 E000 E20X
> ---- ---- ---- ------
> null 0.01 0.0  2.0E12
> rows: 1
