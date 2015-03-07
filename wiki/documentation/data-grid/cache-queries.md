<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

Ignite supports a very elegant query API with support for

  * [Predicate-based Scan Queries](#scan-queries)
  * [SQL Queries](#sql-queries)
  * [Text Queries](#text-queries)
  * [Continuous Queries](#continuous-queries)
  
For SQL queries ignites supports in-memory indexing, so all the data lookups are extremely fast. If you are caching your data in [off-heap memory](doc:off-heap-memory), then query indexes will also be cached in off-heap memory as well.

Ignite also provides support for custom indexing via `IndexingSpi` and `SpiQuery` class.
[block:api-header]
{
  "type": "basic",
  "title": "Main Abstractions"
}
[/block]
`IgniteCache` has several query methods all of which receive some sublcass of `Query` class and return `QueryCursor`.
##Query
`Query` abstract class represents an abstract paginated query to be executed on the distributed cache. You can set the page size for the returned cursor via `Query.setPageSize(...)` method (default is `1024`).

##QueryCursor
`QueryCursor` represents query result set and allows for transparent page-by-page iteration. Whenever user starts iterating over the last page, it will automatically request the next page in the background. For cases when pagination is not needed, you can use `QueryCursor.getAll()` method which will fetch the whole query result and store it in a collection.
[block:callout]
{
  "type": "info",
  "title": "Closing Cursors",
  "body": "Cursors will close automatically if you iterate to the end of the result set. If you need to stop iteration sooner, you must close() the cursor explicitly or use `AutoCloseable` syntax."
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Scan Queries"
}
[/block]
Scan queries allow for querying cache in distributed form based on some user defined predicate. 
[block:code]
{
  "codes": [
    {
      "code": "IgniteCache<Long, Person> cache = ignite.jcache(\"mycache\");\n\n// Find only persons earning more than 1,000.\ntry (QueryCursor cursor = cache.query(new ScanQuery((k, p) -> p.getSalary() > 1000)) {\n  for (Person p : cursor)\n    System.out.println(p.toString());\n}",
      "language": "java",
      "name": "scan"
    },
    {
      "code": "IgniteCache<Long, Person> cache = ignite.jcache(\"mycache\");\n\n// Find only persons earning more than 1,000.\nIgniteBiPredicate<Long, Person> filter = new IgniteByPredicate<>() {\n  @Override public boolean apply(Long key, Perons p) {\n  \treturn p.getSalary() > 1000;\n\t}\n};\n\ntry (QueryCursor cursor = cache.query(new ScanQuery(filter)) {\n  for (Person p : cursor)\n    System.out.println(p.toString());\n}",
      "language": "java",
      "name": "java7 scan"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "SQL Queries"
}
[/block]
Ignite supports free-form SQL queries virtually without any limitations. SQL syntax is ANSI-99 compliant. You can use any SQL function, any aggregation, any grouping and Ignite will figure out where to fetch the results from.

##SQL Joins
Ignite supports distributed SQL joins. Moreover, if data resides in different caches, Ignite allows for cross-cache joins as well. 

Joins between `PARTITIONED` and `REPLICATED` caches always work without any limitations. However, if you do a join between two `PARTITIONED` data sets, then you must make sure that the keys you are joining on are **collocated**. 

##Field Queries
Instead of selecting the whole object, you can choose to select only specific fields in order to minimize network and serialization overhead. For this purpose Ignite has a concept of `fields queries`.
[block:code]
{
  "codes": [
    {
      "code": "IgniteCache<Long, Person> cache = ignite.jcache(\"mycache\");\n\nSqlQuery sql = new SqlQuery(Person.class, \"salary > ?\");\n\n// Find only persons earning more than 1,000.\ntry (QueryCursor<Entry<Long, Person> cursor = cache.query(sql.setArgs(1000)) {\n  for (Entry<Long, Person> e : cursor)\n    System.out.println(e.getValue().toString());\n}",
      "language": "java",
      "name": "sql"
    },
    {
      "code": "IgniteCache<Long, Person> cache = ignite.jcache(\"mycache\");\n\n// SQL join on Person and Organization.\nSqlQuery sql = new SqlQuery(Person.class,\n  \"from Person, Organization \"\n  + \"where Person.orgId = Organization.id \"\n  + \"and lower(Organization.name) = lower(?)\");\n\n// Find all persons working for Ignite organization.\ntry (QueryCursor<Entry<Long, Person> cursor = cache.query(sql.setArgs(\"Ignite\")) {\n  for (Entry<Long, Person> e : cursor)\n    System.out.println(e.getValue().toString());\n}",
      "language": "java",
      "name": "sql join"
    },
    {
      "code": "IgniteCache<Long, Person> cache = ignite.jcache(\"mycache\");\n\nSqlFieldsQuery sql = new SqlFieldsQuery(\"select concat(firstName, ' ', lastName) from Person\");\n\n// Select concatinated first and last name for all persons.\ntry (QueryCursor<List<?>> cursor = cache.query(sql) {\n  for (List<?> row : cursor)\n    System.out.println(\"Full name: \" + row.get(0));\n}",
      "language": "java",
      "name": "sql fields"
    },
    {
      "code": "IgniteCache<Long, Person> cache = ignite.jcache(\"mycache\");\n\n// Select with join between Person and Organization.\nSqlFieldsQuery sql = new SqlFieldsQuery(\n  \"select concat(firstName, ' ', lastName), Organization.name \"\n  + \"from Person, Organization where \"\n  + \"Person.orgId = Organization.id and \"\n  + \"Person.salary > ?\");\n\n// Only find persons with salary > 1000.\ntry (QueryCursor<List<?>> cursor = cache.query(sql.setArgs(1000)) {\n  for (List<?> row : cursor)\n    System.out.println(\"personName=\" + row.get(0) + \", orgName=\" + row.get(1));\n}",
      "language": "java",
      "name": "sql fields & join"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Text Queries"
}
[/block]
Ignite also supports text-based queries based on Lucene indexing.
[block:code]
{
  "codes": [
    {
      "code": "IgniteCache<Long, Person> cache = ignite.jcache(\"mycache\");\n\n// Query for all people with \"Master Degree\" in their resumes.\nTextQuery txt = new TextQuery(Person.class, \"Master Degree\");\n\ntry (QueryCursor<Entry<Long, Person>> masters = cache.query(txt)) {\n  for (Entry<Long, Person> e : cursor)\n    System.out.println(e.getValue().toString());\n}",
      "language": "java",
      "name": "text query"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Continuous Queries"
}
[/block]
Continuous queries are good for cases when you want to execute a query and then continue to get notified about the data changes that fall into your query filter.

Continuous queries are supported via `ContinuousQuery` class, which supports the following:
## Initial Query
Whenever executing continuous query, you have an option to execution initial query before starting to listen to updates. The initial query can be set via `ContinuousQuery.setInitialQuery(Query)` method and can be of any query type, [Scan](#scan-queries), [SQL](#sql-queries), or [TEXT](#text-queries). This parameter is optional, and if not set, will not be used.
## Remote Filter
This filter is executed on the primary node for a given key and evaluates whether the event should be propagated to the listener. If the filter returns `true`, then the listener will be notified, otherwise the event will be skipped. Filtering events on the node on which they have occurred allows to minimize unnecessary network traffic for listener notifications. Remote filter can be set via `ContinuousQuery.setRemoteFilter(CacheEntryEventFilter<K, V>)` method.
## Local Listener
Whenever events pass the remote filter, they will be send to the client to notify the local listener there. Local listener is set via `ContinuousQuery.setLocalListener(CacheEntryUpdatedListener<K, V>)` method.
[block:code]
{
  "codes": [
    {
      "code": "IgniteCache<Integer, String> cache = ignite.jcache(\"mycache\");\n\n// Create new continuous query.\nContinuousQuery<Integer, String> qry = new ContinuousQuery<>();\n\n// Optional initial query to select all keys greater than 10.\nqry.setInitialQuery(new ScanQuery<Integer, String>((k, v) -> k > 10)):\n\n// Callback that is called locally when update notifications are received.\nqry.setLocalListener((evts) -> \n\tevts.stream().forEach(e -> System.out.println(\"key=\" + e.getKey() + \", val=\" + e.getValue())));\n\n// This filter will be evaluated remotely on all nodes.\n// Entry that pass this filter will be sent to the caller.\nqry.setRemoteFilter(e -> e.getKey() > 10);\n\n// Execute query.\ntry (QueryCursor<Cache.Entry<Integer, String>> cur = cache.query(qry)) {\n  // Iterate through existing data stored in cache.\n  for (Cache.Entry<Integer, String> e : cur)\n    System.out.println(\"key=\" + e.getKey() + \", val=\" + e.getValue());\n\n  // Add a few more keys and watch a few more query notifications.\n  for (int i = 5; i < 15; i++)\n    cache.put(i, Integer.toString(i));\n}\n",
      "language": "java",
      "name": "continuous query"
    },
    {
      "code": "IgniteCache<Integer, String> cache = ignite.jcache(CACHE_NAME);\n\n// Create new continuous query.\nContinuousQuery<Integer, String> qry = new ContinuousQuery<>();\n\nqry.setInitialQuery(new ScanQuery<Integer, String>(new IgniteBiPredicate<Integer, String>() {\n  @Override public boolean apply(Integer key, String val) {\n    return key > 10;\n  }\n}));\n\n// Callback that is called locally when update notifications are received.\nqry.setLocalListener(new CacheEntryUpdatedListener<Integer, String>() {\n  @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends String>> evts) {\n    for (CacheEntryEvent<Integer, String> e : evts)\n      System.out.println(\"key=\" + e.getKey() + \", val=\" + e.getValue());\n  }\n});\n\n// This filter will be evaluated remotely on all nodes.\n// Entry that pass this filter will be sent to the caller.\nqry.setRemoteFilter(new CacheEntryEventFilter<Integer, String>() {\n  @Override public boolean evaluate(CacheEntryEvent<? extends Integer, ? extends String> e) {\n    return e.getKey() > 10;\n  }\n});\n\n// Execute query.\ntry (QueryCursor<Cache.Entry<Integer, String>> cur = cache.query(qry)) {\n  // Iterate through existing data.\n  for (Cache.Entry<Integer, String> e : cur)\n    System.out.println(\"key=\" + e.getKey() + \", val=\" + e.getValue());\n\n  // Add a few more keys and watch more query notifications.\n  for (int i = keyCnt; i < keyCnt + 10; i++)\n    cache.put(i, Integer.toString(i));\n}",
      "language": "java",
      "name": "java7 continuous query"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Query Configuration"
}
[/block]
Queries can be configured from code by using `@QuerySqlField` annotations.
[block:code]
{
  "codes": [
    {
      "code": "public class Person implements Serializable {\n  /** Person ID (indexed). */\n  @QuerySqlField(index = true)\n  private long id;\n\n  /** Organization ID (indexed). */\n  @QuerySqlField(index = true)\n  private long orgId;\n\n  /** First name (not-indexed). */\n  @QuerySqlField\n  private String firstName;\n\n  /** Last name (not indexed). */\n  @QuerySqlField\n  private String lastName;\n\n  /** Resume text (create LUCENE-based TEXT index for this field). */\n  @QueryTextField\n  private String resume;\n\n  /** Salary (indexed). */\n  @QuerySqlField(index = true)\n  private double salary;\n  \n  ...\n}",
      "language": "java"
    }
  ]
}
[/block]