-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT * FROM SYSTEM_RANGE(1, 10) ORDER BY 1;
> X
> --
> 1
> 2
> 3
> 4
> 5
> 6
> 7
> 8
> 9
> 10
> rows (ordered): 10

SELECT COUNT(*) FROM SYSTEM_RANGE(1, 10);
>> 10

SELECT * FROM SYSTEM_RANGE(1, 10, 2) ORDER BY 1;
> X
> -
> 1
> 3
> 5
> 7
> 9
> rows (ordered): 5

SELECT COUNT(*) FROM SYSTEM_RANGE(1, 10, 2);
>> 5

SELECT * FROM SYSTEM_RANGE(1, 9, 2) ORDER BY 1;
> X
> -
> 1
> 3
> 5
> 7
> 9
> rows (ordered): 5

SELECT COUNT(*) FROM SYSTEM_RANGE(1, 9, 2);
>> 5

SELECT * FROM SYSTEM_RANGE(10, 1, -2) ORDER BY 1 DESC;
> X
> --
> 10
> 8
> 6
> 4
> 2
> rows (ordered): 5

SELECT COUNT(*) FROM SYSTEM_RANGE(10, 1, -2);
>> 5

SELECT * FROM SYSTEM_RANGE(10, 2, -2) ORDER BY 1 DESC;
> X
> --
> 10
> 8
> 6
> 4
> 2
> rows (ordered): 5

SELECT COUNT(*) FROM SYSTEM_RANGE(10, 2, -2);
>> 5

SELECT * FROM SYSTEM_RANGE(1, 1);
> X
> -
> 1
> rows: 1

SELECT COUNT(*) FROM SYSTEM_RANGE(1, 1);
>> 1

SELECT * FROM SYSTEM_RANGE(1, 1, -1);
> X
> -
> 1
> rows: 1

SELECT COUNT(*) FROM SYSTEM_RANGE(1, 1, -1);
>> 1

SELECT * FROM SYSTEM_RANGE(2, 1);
> X
> -
> rows: 0

SELECT COUNT(*) FROM SYSTEM_RANGE(2, 1);
>> 0

SELECT * FROM SYSTEM_RANGE(2, 1, 2);
> X
> -
> rows: 0

SELECT COUNT(*) FROM SYSTEM_RANGE(2, 1, 2);
>> 0

SELECT * FROM SYSTEM_RANGE(1, 2, 0);
> exception

SELECT COUNT(*) FROM SYSTEM_RANGE(1, 2, 0);
> exception

SELECT * FROM SYSTEM_RANGE(2, 1, 0);
> exception

SELECT COUNT(*) FROM SYSTEM_RANGE(2, 1, 0);
> exception
