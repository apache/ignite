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

Given that the most common ways to cache data is in `PARTITIONED` caches, collocating compute with data or data with data can significantly improve performance and scalability of your application.
[block:api-header]
{
  "type": "basic",
  "title": "Collocate Data with Data"
}
[/block]
In many cases it is beneficial to colocate different cache keys together if they will be accessed together. Quite often your business logic will require access to more than one cache key. By collocating them together you can make sure that all keys with the same `affinityKey` will be cached on the same processing node, hence avoiding costly network trips to fetch data from remote nodes.

For example, let's say you have `Person` and `Company` objects and you want to collocate `Person` objects with `Company` objects for which this person works. To achieve that, cache key used to cache `Person` objects should have a field or method annotated with `@CacheAffinityKeyMapped` annotation, which will provide the value of the company key for collocation. For convenience, you can also optionally use `CacheAffinityKey` class
[block:code]
{
  "codes": [
    {
      "code": "public class PersonKey {\n    // Person ID used to identify a person.\n    private String personId;\n \n    // Company ID which will be used for affinity.\n    @GridCacheAffinityKeyMapped\n    private String companyId;\n    ...\n}\n\n// Instantiate person keys with the same company ID which is used as affinity key.\nObject personKey1 = new PersonKey(\"myPersonId1\", \"myCompanyId\");\nObject personKey2 = new PersonKey(\"myPersonId2\", \"myCompanyId\");\n \nPerson p1 = new Person(personKey1, ...);\nPerson p2 = new Person(personKey2, ...);\n \n// Both, the company and the person objects will be cached on the same node.\ncache.put(\"myCompanyId\", new Company(..));\ncache.put(personKey1, p1);\ncache.put(personKey2, p2);",
      "language": "java",
      "name": "using PersonKey"
    },
    {
      "code": "Object personKey1 = new CacheAffinityKey(\"myPersonId1\", \"myCompanyId\");\nObject personKey2 = new CacheAffinityKey(\"myPersonId2\", \"myCompanyId\");\n \nPerson p1 = new Person(personKey1, ...);\nPerson p2 = new Person(personKey2, ...);\n \n// Both, the company and the person objects will be cached on the same node.\ncache.put(\"myCompanyId\", new Company(..));\ncache.put(personKey1, p1);\ncache.put(personKey2, p2);",
      "language": "java",
      "name": "using CacheAffinityKey"
    }
  ]
}
[/block]

[block:callout]
{
  "type": "info",
  "title": "SQL Joins",
  "body": "When performing [SQL distributed joins](/docs/cache-queries#sql-queries) over data residing in partitioned caches, you must make sure that the join-keys are collocated."
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Collocating Compute with Data"
}
[/block]
It is also possible to route computations to the nodes where the data is cached. This concept is known as Collocation Of Computations And Data. It allows to route whole units of work to a certain node. 

To collocate compute with data you should use `IgniteCompute.affinityRun(...)` and `IgniteCompute.affinityCall(...)` methods.

Here is how you can collocate your computation with the same cluster node on which company and persons from the example above are cached.
[block:code]
{
  "codes": [
    {
      "code": "String companyId = \"myCompanyId\";\n \n// Execute Runnable on the node where the key is cached.\nignite.compute().affinityRun(\"myCache\", companyId, () -> {\n  Company company = cache.get(companyId);\n\n  // Since we collocated persons with the company in the above example,\n  // access to the persons objects is local.\n  Person person1 = cache.get(personKey1);\n  Person person2 = cache.get(personKey2);\n  ...  \n});",
      "language": "java",
      "name": "affinityRun"
    },
    {
      "code": "final String companyId = \"myCompanyId\";\n \n// Execute Runnable on the node where the key is cached.\nignite.compute().affinityRun(\"myCache\", companyId, new IgniteRunnable() {\n  @Override public void run() {\n    Company company = cache.get(companyId);\n    \n    Person person1 = cache.get(personKey1);\n    Person person2 = cache.get(personKey2);\n    ...\n  }\n};",
      "language": "java",
      "name": "java7 affinityRun"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "IgniteCompute vs EntryProcessor"
}
[/block]
Both, `IgniteCompute.affinityRun(...)` and `IgniteCache.invoke(...)` methods offer ability to collocate compute and data. The main difference is that `invoke(...)` methods is atomic and executes while holding a lock on a key. You should not access other keys from within the `EntryProcessor` logic as it may cause a deadlock. 

 `affinityRun(...)` and `affinityCall(...)`, on the other hand, do not hold any locks. For example, it is absolutely legal to start multiple transactions or execute cache queries from these methods without worrying about deadlocks. In this case Ignite will automatically detect that the processing is collocated and will employ a light-weight 1-Phase-Commit optimization for transactions (instead of 2-Phase-Commit).
[block:callout]
{
  "type": "info",
  "body": "See [JCache EntryProcessor](/docs/jcache#entryprocessor) documentation for more information about `IgniteCache.invoke(...)` method."
}
[/block]