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

Ignite supports 2 modes for cache operation, *transactional* and *atomic*. In `transactional` mode you are able to group multiple cache operations in a transaction, while `atomic` mode supports multiple atomic operations, one at a time. `Atomic` mode is more light-weight and generally has better performance over `transactional` caches.

However, regardless of which mode you use, as long as your cluster is alive, the data between different cluster nodes must remain consistent. This means that whichever node is being used to retrieve data, it will never get data that has been partially committed or that is inconsistent with other data.
[block:api-header]
{
  "type": "basic",
  "title": "IgniteTransactions"
}
[/block]
`IgniteTransactions` interface contains functionality for starting and completing transactions, as well as subscribing listeners or getting metrics.
[block:callout]
{
  "type": "info",
  "title": "Cross-Cache Transactions",
  "body": "You can combine multiple operations from different caches into one transaction. Note that this allows to update caches of different types, like `REPLICATED` and `PARTITIONED` caches, in one transaction."
}
[/block]
You can obtain an instance of `IgniteTransactions` as follows:
[block:code]
{
  "codes": [
    {
      "code": "Ignite ignite = Ignition.ignite();\n\nIgniteTransactions transactions = ignite.transactions();",
      "language": "java"
    }
  ]
}
[/block]
Here is an example of how transactions can be performed in Ignite:
[block:code]
{
  "codes": [
    {
      "code": "try (Transaction tx = transactions.txStart()) {\n    Integer hello = cache.get(\"Hello\");\n  \n    if (hello == 1)\n        cache.put(\"Hello\", 11);\n  \n    cache.put(\"World\", 22);\n  \n    tx.commit();\n}",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Two-Phase-Commit (2PC)"
}
[/block]
Ignite utilizes 2PC protocol for its transactions with many one-phase-commit optimizations whenever applicable. Whenever data is updated within a transaction, Ignite will keep transactional state in a local transaction map until `commit()` is called, at which point, if needed, the data is transferred to participating remote nodes.

For more information on how Ignite 2PC works, you can check out these blogs:
  * [Two-Phase-Commit for Distributed In-Memory Caches](http://gridgain.blogspot.com/2014/09/two-phase-commit-for-distributed-in.html)
  *  [Two-Phase-Commit for In-Memory Caches - Part II](http://gridgain.blogspot.com/2014/09/two-phase-commit-for-in-memory-caches.html) 
  * [One-Phase-Commit - Fast Transactions For In-Memory Caches](http://gridgain.blogspot.com/2014/09/one-phase-commit-fast-transactions-for.html) 
[block:callout]
{
  "type": "success",
  "body": "Ignite provides fully ACID (**A**tomicity, **C**onsistency, **I**solation, **D**urability) compliant transactions that ensure guaranteed consistency.",
  "title": "ACID Compliance"
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Optimistic and Pessimistic"
}
[/block]
Whenever `TRANSACTIONAL` atomicity mode is configured, Ignite supports `OPTIMISTIC` and `PESSIMISTIC` concurrency modes for transactions. The main difference is that in `PESSIMISTIC` mode locks are acquired at the time of access, while in `OPTIMISTIC` mode locks are acquired during the `commit` phase.

Ignite also supports the following isolation levels:
  * `READ_COMMITED` - data is always fetched from the primary node, even if it already has been accessed within the transaction.
  * `REPEATABLE_READ` - data is fetched form the primary node only once on first access and stored in the local transactional map. All consecutive access to the same data is local.
  * `SERIALIZABLE` - when combined with `OPTIMISTIC` concurrency, transactions may throw `TransactionOptimisticException` in case of concurrent updates. 
[block:code]
{
  "codes": [
    {
      "code": "IgniteTransactions txs = ignite.transactions();\n\n// Start transaction in pessimistic mode with repeatable read isolation level.\nTransaction tx = txs.txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ);",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Atomicity Mode"
}
[/block]
Ignite supports 2 atomicity modes defined in `CacheAtomicityMode` enum:
  * `TRANSACTIONAL`
  * `ATOMIC`

`TRANSACTIONAL` mode enables fully ACID-compliant transactions, however, when only atomic semantics are needed, it is recommended that  `ATOMIC` mode is used for better performance.

`ATOMIC` mode provides better performance by avoiding transactional locks, while still providing data atomicity and consistency. Another difference in `ATOMIC` mode is that bulk writes, such as `putAll(...)`and `removeAll(...)` methods are no longer executed in one transaction and can partially fail. In case of partial failure, `CachePartialUpdateException` will be thrown which will contain a list of keys for which the update failed.
[block:callout]
{
  "type": "info",
  "body": "Note that transactions are disabled whenever `ATOMIC` mode is used, which allows to achieve much higher performance and throughput in cases when transactions are not needed.",
  "title": "Performance"
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Configuration"
}
[/block]
Atomicity mode is defined in `CacheAtomicityMode` enum and can be configured via `atomicityMode` property of `CacheConfiguration`. 

Default atomicity mode is `ATOMIC`.
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.configuration.IgniteConfiguration\">\n    ...\n    <property name=\"cacheConfiguration\">\n        <bean class=\"org.apache.ignite.configuration.CacheConfiguration\">\n          \t<!-- Set a cache name. -->\n   \t\t\t\t\t<property name=\"name\" value=\"myCache\"/>\n\n            <!-- Set atomicity mode, can be ATOMIC or TRANSACTIONAL. -->\n    \t\t\t\t<property name=\"atomicityMode\" value=\"TRANSACTIONAL\"/>\n            ... \n        </bean\n    </property>\n</bean>",
      "language": "xml"
    },
    {
      "code": "CacheConfiguration cacheCfg = new CacheConfiguration();\n\ncacheCfg.setName(\"cacheName\");\n\ncacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);\n\nIgniteConfiguration cfg = new IgniteConfiguration();\n\ncfg.setCacheConfiguration(cacheCfg);\n\n// Start Ignite node.\nIgnition.start(cfg);",
      "language": "java"
    }
  ]
}
[/block]