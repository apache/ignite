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

Distributed atomic sequence provided by `IgniteCacheAtomicSequence`  interface is similar to distributed atomic long, but its value can only go up. It also supports reserving a range of values to avoid costly network trips or cache updates every time a sequence must provide a next value. That is, when you perform `incrementAndGet()` (or any other atomic operation) on an atomic sequence, the data structure reserves ahead a range of values, which are guaranteed to be unique across the cluster for this sequence instance. 

Here is an example of how atomic sequence can be created:
[block:code]
{
  "codes": [
    {
      "code": "Ignite ignite = Ignition.ignite();\n \nIgniteAtomicSequence seq = ignite.atomicSequence(\n    \"seqName\", // Sequence name.\n    0,       // Initial value for sequence.\n    true     // Create if it does not exist.\n);",
      "language": "java"
    }
  ]
}
[/block]
Below is a simple usage example:
[block:code]
{
  "codes": [
    {
      "code": "Ignite ignite = Ignition.ignite();\n\n// Initialize atomic sequence.\nfinal IgniteAtomicSequence seq = ignite.atomicSequence(\"seqName\", 0, true);\n\n// Increment atomic sequence.\nfor (int i = 0; i < 20; i++) {\n  long currentValue = seq.get();\n  long newValue = seq.incrementAndGet();\n  \n  ...\n}",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Sequence Reserve Size"
}
[/block]
The key parameter of `IgniteAtomicSequence` is `atomicSequenceReserveSize` which is the number of sequence values reserved, per node .  When a node tries to obtain an instance of `IgniteAtomicSequence`, a number of sequence values will be reserved for that node and consequent increments of sequence will happen locally without communication with other nodes, until the next reservation has to be made. 

The default value for `atomicSequenceReserveSize` is `1000`. This default setting can be changed by modifying the `atomicSequenceReserveSize` property of `AtomicConfiguration`. 
[block:callout]
{
  "type": "info",
  "body": "Refer to [Atomic Configuration](/docs/atomic-types#atomic-configuration) for more information on various atomic configuration properties, and examples on how to configure them."
}
[/block]