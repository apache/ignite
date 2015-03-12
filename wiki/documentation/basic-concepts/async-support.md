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

All distributed methods on all Ignite APIs can be executed either synchronously or asynchronously. However, instead of having a duplicate asynchronous method for every synchronous one (like `get()` and `getAsync()`, or `put()` and `putAsync()`, etc.), Ignite chose a more elegant approach, where methods don't have to be duplicated.
[block:api-header]
{
  "type": "basic",
  "title": "IgniteAsyncSupport"
}
[/block]
`IgniteAsyncSupport` interface adds asynchronous mode to many Ignite APIs. For example, `IgniteCompute`, `IgniteServices`, `IgniteCache`, and `IgniteTransactions` all extend `IgniteAsyncSupport` interface.

To enable asynchronous mode, you should call `withAsync()` method. 

## Compute Grid Example
The example below illustrates the difference between synchronous and asynchronous computations.
[block:code]
{
  "codes": [
    {
      "code": "IgniteCompute compute = ignite.compute();\n\n// Execute a job and wait for the result.\nString res = compute.call(() -> {\n  // Print hello world on some cluster node.\n\tSystem.out.println(\"Hello World\");\n  \n  return \"Hello World\";\n});",
      "language": "java",
      "name": "Synchronous"
    }
  ]
}
[/block]
Here is how you would make the above invocation asynchronous:
[block:code]
{
  "codes": [
    {
      "code": "// Enable asynchronous mode.\nIgniteCompute asyncCompute = ignite.compute().withAsync();\n\n// Asynchronously execute a job.\nasyncCompute.call(() -> {\n  // Print hello world on some cluster node and wait for completion.\n\tSystem.out.println(\"Hello World\");\n  \n  return \"Hello World\";\n});\n\n// Get the future for the above invocation.\nIgniteFuture<String> fut = asyncCompute.future();\n\n// Asynchronously listen for completion and print out the result.\nfut.listenAsync(f -> System.out.println(\"Job result: \" + f.get()));",
      "language": "java",
      "name": "Asynchronous"
    }
  ]
}
[/block]
## Data Grid Example
Here is the data grid example for synchronous and asynchronous invocations.
[block:code]
{
  "codes": [
    {
      "code": "IgniteCache<String, Integer> cache = ignite.jcache(\"mycache\");\n\n// Synchronously store value in cache and get previous value.\nInteger val = cache.getAndPut(\"1\", 1);",
      "language": "java",
      "name": "Synchronous"
    }
  ]
}
[/block]
Here is how you would make the above invocation asynchronous.
[block:code]
{
  "codes": [
    {
      "code": "// Enable asynchronous mode.\nIgniteCache<String, Integer> asyncCache = ignite.jcache(\"mycache\").withAsync();\n\n// Asynhronously store value in cache.\nasyncCache.getAndPut(\"1\", 1);\n\n// Get future for the above invocation.\nIgniteFuture<Integer> fut = asyncCache.future();\n\n// Asynchronously listen for the operation to complete.\nfut.listenAsync(f -> System.out.println(\"Previous cache value: \" + f.get()));",
      "language": "java",
      "name": "Asynchronous"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "@IgniteAsyncSupported"
}
[/block]
Not every method on Ignite APIs is distributed and therefore does not really require asynchronous mode. To avoid confusion about which method is distributed, i.e. can be asynchronous, and which is not, all distributed methods in Ignite are annotated with `@IgniteAsyncSupported` annotation.
[block:callout]
{
  "type": "info",
  "body": "Note that, although not really needed, in async mode you can still get the future for non-distributed operations as well.  However, this future will always be completed."
}
[/block]