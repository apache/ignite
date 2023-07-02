# Presto Flex Connector 
[![Build Status](https://github.com/ebyhr/presto-flex/workflows/CI/badge.svg)](https://github.com/ebyhr/presto-flex/actions?query=workflow%3ACI+event%3Apush+branch%3Amaster)
This is a presto connector to access local file (e.g. csv, tsv). Please keep in mind that this is not production ready and it was created for tests.

# Query
You need to specify file type by schema name and use absolute path.
```sql
select
  * 
from 
 flex.csv."file:///tmp/numbers-2.csv"
;

select
  * 
from 
 flex.csv."https://raw.githubusercontent.com/ebyhr/presto-flex/master/src/test/resources/example-data/numbers-2.csv"
;
``` 

Supported schemas are below.
- `tsv`
- `csv`
- `txt`
- `raw`
- `excel`

`tsv` plugin extract each line with `\t` delimiter. Currently first line is used as column names.
```sql
select * from flex.tsv."https://raw.githubusercontent.com/ebyhr/presto-flex/master/src/test/resources/example-data/numbers.tsv";
``` 
```
  one  | 1 
-------+---
 two   | 2 
 three | 3
(2 rows)
```


`csv` plugin extract each line with `,` delimiter. Currently first line is used as column names.
```sql
select * from flex.csv."https://raw.githubusercontent.com/ebyhr/presto-flex/master/src/test/resources/example-data/numbers-2.csv";
```
```
  ten   | 10 
--------+----
 eleven | 11 
 twelve | 12
(2 rows)
```

`txt` plugin doesn't extract each line. Currently column name is always `value`.
```sql
select * from flex.txt."https://raw.githubusercontent.com/ebyhr/presto-flex/master/src/test/resources/example-data/numbers.tsv";
``` 
```
 value  
--------
 one    1   
 two    2   
 three  3
(3 rows)
```

`raw` plugin doesn't extract each line. Currently column name is always `data`. This connector is similar to `txt` plugin. 
The main difference is `txt` plugin may return multiple rows, but `raw` plugin always return only one row.
```sql
select * from flex.raw."https://raw.githubusercontent.com/ebyhr/presto-flex/master/src/test/resources/example-data/numbers.tsv";
``` 
```
  data  
--------
 one    1   
 two    2   
 three  3 
(1 row)
```

`excel` plugin currently read first sheet.
```sql
select * from flex.excel."https://raw.githubusercontent.com/ebyhr/presto-flex/master/src/test/resources/example-data/sample.xlsx";
``` 
```
  data  
--------
 one    1   
 two    2   
 three  3 
(1 row)
```

# Build
Run all the unit test classes.
```
mvn test
```

Creates a deployable jar file
```
mvn clean compile package
```

Copy jar files in target directory to use flex connector in your presto cluster.
```
cp -p target/*.jar ${PLUGIN_DIRECTORY}/flex/
```
