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

Ignite supports distributed ***atomic long*** and ***atomic reference*** , similar to `java.util.concurrent.atomic.AtomicLong` and `java.util.concurrent.atomic.AtomicReference` respectively. 

Atomics in Ignite are distributed across the cluster, essentially enabling performing atomic operations (such as increment-and-get or compare-and-set) with the same globally-visible value. For example, you could update the value of an atomic long on one node and read it from another node.

##Features
  * Retrieve current value.
  * Atomically modify current value.
  * Atomically increment or decrement current value.
  * Atomically compare-and-set the current value to new value.

Distributed atomic long and atomic reference can be obtained via `IgniteAtomicLong` and `IgniteAtomicReference` interfaces respectively, as shown below:
[block:code]
{
  "codes": [
    {
      "code": "Ignite ignite = Ignition.ignite();\n \nIgniteAtomicLong atomicLong = ignite.atomicLong(\n    \"atomicName\", // Atomic long name.\n    0,        \t\t// Initial value.\n    false     \t\t// Create if it does not exist.\n)",
      "language": "java",
      "name": "AtomicLong"
    },
    {
      "code": "Ignite ignite = Ignition.ignite();\n\n// Create an AtomicReference.\nIgniteAtomicReference<Boolean> ref = ignite.atomicReference(\n    \"refName\",  // Reference name.\n    \"someVal\",  // Initial value for atomic reference.\n    true        // Create if it does not exist.\n);",
      "language": "java",
      "name": "AtomicReference"
    }
  ]
}
[/block]

Below is a usage example of `IgniteAtomicLong` and `IgniteAtomicReference`:

[block:code]
{
  "codes": [
    {
      "code": "Ignite ignite = Ignition.ignite();\n\n// Initialize atomic long.\nfinal IgniteAtomicLong atomicLong = ignite.atomicLong(\"atomicName\", 0, true);\n\n// Increment atomic long on local node.\nSystem.out.println(\"Incremented value: \" + atomicLong.incrementAndGet());\n",
      "language": "java",
      "name": "AtomicLong"
    },
    {
      "code": "Ignite ignite = Ignition.ignite();\n\n// Initialize atomic reference.\nIgniteAtomicReference<String> ref = ignite.atomicReference(\"refName\", \"someVal\", true);\n\n// Compare old value to new value and if they are equal,\n//only then set the old value to new value.\nref.compareAndSet(\"WRONG EXPECTED VALUE\", \"someNewVal\"); // Won't change.",
      "language": "java",
      "name": "AtomicReference"
    }
  ]
}
[/block]
All atomic operations provided by `IgniteAtomicLong` and `IgniteAtomicReference` are synchronous. The time an atomic operation will take depends on the number of nodes performing concurrent operations with the same instance of atomic long, the intensity of these operations, and network latency.
[block:callout]
{
  "type": "info",
  "title": "",
  "body": "`IgniteCache` interface has `putIfAbsent()` and `replace()` methods, which provide the same CAS functionality as atomic types."
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Atomic Configuration"
}
[/block]
Atomics in Ignite can be configured via `atomicConfiguration` property of `IgniteConfiguration`. The following configuration parameters can be used :
[block:parameters]
{
  "data": {
    "0-0": "`setBackups(int)`",
    "1-0": "`setCacheMode(CacheMode)`",
    "2-0": "`setAtomicSequenceReserveSize(int)`",
    "h-0": "Setter Method",
    "h-1": "Description",
    "h-2": "Default",
    "0-1": "Set number of backups.",
    "0-2": "0",
    "1-1": "Set cache mode for all atomic types.",
    "1-2": "`PARTITIONED`",
    "2-1": "Sets the number of sequence values reserved for `IgniteAtomicSequence` instances.",
    "2-2": "1000"
  },
  "cols": 3,
  "rows": 3
}
[/block]
##Example 
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.configuration.IgniteConfiguration\">\n    ...\n    <property name=\"atomicConfiguration\">\n        <bean class=\"org.apache.ignite.configuration.AtomicConfiguration\">\n            <!-- Set number of backups. -->\n            <property name=\"backups\" value=\"1\"/>\n          \t\n          \t<!-- Set number of sequence values to be reserved. -->\n          \t<property name=\"atomicSequenceReserveSize\" value=\"5000\"/>\n        </bean>\n    </property>\n</bean>",
      "language": "xml"
    },
    {
      "code": "AtomicConfiguration atomicCfg = new AtomicConfiguration();\n \n// Set number of backups.\natomicCfg.setBackups(1);\n\n// Set number of sequence values to be reserved. \natomicCfg.setAtomicSequenceReserveSize(5000);\n\nIgniteConfiguration cfg = new IgniteConfiguration();\n  \n// Use atomic configuration in Ignite configuration.\ncfg.setAtomicConfiguration(atomicCfg);\n  \n// Start Ignite node.\nIgnition.start(cfg);",
      "language": "java"
    }
  ]
}
[/block]