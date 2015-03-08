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

Ignite compute grid allows to broadcast and load-balance any closure within the cluster or a cluster group, including plain Java `runnables` and `callables`.
[block:api-header]
{
  "type": "basic",
  "title": "Broadcast Methods"
}
[/block]
All `broadcast(...)` methods broadcast a given job to all nodes in the cluster or cluster group. 
[block:code]
{
  "codes": [
    {
      "code": "final Ignite ignite = Ignition.ignite();\n\n// Limit broadcast to remote nodes only.\nIgniteCompute compute = ignite.compute(ignite.cluster().forRemotes());\n\n// Print out hello message on remote nodes in the cluster group.\ncompute.broadcast(() -> System.out.println(\"Hello Node: \" + ignite.cluster().localNode().id()));",
      "language": "java",
      "name": "broadcast"
    },
    {
      "code": "final Ignite ignite = Ignition.ignite();\n\n// Limit broadcast to remote nodes only and \n// enable asynchronous mode.\nIgniteCompute compute = ignite.compute(ignite.cluster().forRemotes()).withAsync();\n\n// Print out hello message on remote nodes in the cluster group.\ncompute.broadcast(() -> System.out.println(\"Hello Node: \" + ignite.cluster().localNode().id()));\n\nComputeTaskFuture<?> fut = compute.future():\n\nfut.listenAsync(f -> System.out.println(\"Finished sending broadcast job.\"));",
      "language": "java",
      "name": "async broadcast"
    },
    {
      "code": "final Ignite ignite = Ignition.ignite();\n\n// Limit broadcast to rmeote nodes only.\nIgniteCompute compute = ignite.compute(ignite.cluster.forRemotes());\n\n// Print out hello message on remote nodes in projection.\ncompute.broadcast(\n    new IgniteRunnable() {\n        @Override public void run() {\n            // Print ID of remote node on remote node.\n            System.out.println(\">>> Hello Node: \" + ignite.cluster().localNode().id());\n        }\n    }\n);",
      "language": "java",
      "name": "java7 broadcast"
    },
    {
      "code": "final Ignite ignite = Ignition.ignite();\n\n// Limit broadcast to remote nodes only and \n// enable asynchronous mode.\nIgniteCompute compute = ignite.compute(ignite.cluster.forRemotes()).withAsync();\n\n// Print out hello message on remote nodes in the cluster group.\ncompute.broadcast(\n    new IgniteRunnable() {\n        @Override public void run() {\n            // Print ID of remote node on remote node.\n            System.out.println(\">>> Hello Node: \" + ignite.cluster().localNode().id());\n        }\n    }\n);\n\nComputeTaskFuture<?> fut = compute.future():\n\nfut.listenAsync(new IgniteInClosure<? super ComputeTaskFuture<?>>() {\n    public void apply(ComputeTaskFuture<?> fut) {\n        System.out.println(\"Finished sending broadcast job to cluster.\");\n    }\n});",
      "language": "java",
      "name": "java7 async broadcast"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Call and Run Methods"
}
[/block]
All `call(...)` and `run(...)` methods execute either individual jobs or collections of jobs on the cluster or a cluster group.
[block:code]
{
  "codes": [
    {
      "code": "Collection<IgniteCallable<Integer>> calls = new ArrayList<>();\n \n// Iterate through all words in the sentence and create callable jobs.\nfor (String word : \"Count characters using callable\".split(\" \"))\n    calls.add(word::length);\n\n// Execute collection of callables on the cluster.\nCollection<Integer> res = ignite.compute().call(calls);\n\n// Add all the word lengths received from cluster nodes.\nint total = res.stream().mapToInt(Integer::intValue).sum(); ",
      "language": "java",
      "name": "call"
    },
    {
      "code": "IgniteCompute compute = Ignite.compute();\n\n// Iterate through all words and print \n// each word on a different cluster node.\nfor (String word : \"Print words on different cluster nodes\".split(\" \"))\n    // Run on some cluster node.\n    compute.run(() -> System.out.println(word));",
      "language": "java",
      "name": "run"
    },
    {
      "code": "Collection<IgniteCallable<Integer>> calls = new ArrayList<>();\n \n// Iterate through all words in the sentence and create callable jobs.\nfor (String word : \"Count characters using callable\".split(\" \"))\n    calls.add(word::length);\n\n// Enable asynchronous mode.\nIgniteCompute asyncCompute = ignite.compute().withAsync();\n\n// Asynchronously execute collection of callables on the cluster.\nasyncCompute.call(calls);\n\nasyncCompute.future().listenAsync(fut -> {\n    // Total number of characters.\n    int total = fut.get().stream().mapToInt(Integer::intValue).sum(); \n  \n    System.out.println(\"Total number of characters: \" + total);\n});",
      "language": "java",
      "name": "async call"
    },
    {
      "code": "IgniteCompute asyncCompute = ignite.compute().withAsync();\n\nCollection<ComputeTaskFuture<?>> futs = new ArrayList<>();\n\n// Iterate through all words and print \n// each word on a different cluster node.\nfor (String word : \"Print words on different cluster nodes\".split(\" \")) {\n    // Asynchronously run on some cluster node.\n    asyncCompute.run(() -> System.out.println(word));\n\n    futs.add(asyncCompute.future());\n}\n\n// Wait for completion of all futures.\nfuts.stream().forEach(ComputeTaskFuture::get);",
      "language": "java",
      "name": "async run"
    },
    {
      "code": "Collection<IgniteCallable<Integer>> calls = new ArrayList<>();\n \n// Iterate through all words in the sentence and create callable jobs.\nfor (final String word : \"Count characters using callable\".split(\" \")) {\n    calls.add(new GridCallable<Integer>() {\n        @Override public Integer call() throws Exception {\n            return word.length(); // Return word length.\n        }\n    });\n}\n \n// Execute collection of callables on the cluster.\nCollection<Integer> res = ignite.compute().call(calls);\n\nint total = 0;\n\n// Total number of characters.\n// Looks much better in Java 8.\nfor (Integer i : res)\n  total += i;",
      "language": "java",
      "name": "java7 call"
    },
    {
      "code": "IgniteCompute asyncCompute = ignite.compute().withAsync();\n\nCollection<ComputeTaskFuture<?>> futs = new ArrayList<>();\n\n// Iterate through all words and print\n// each word on a different cluster node.\nfor (String word : \"Print words on different cluster nodes\".split(\" \")) {\n    // Asynchronously run on some cluster node.\n    asyncCompute.run(new IgniteRunnable() {\n        @Override public void run() {\n            System.out.println(word);\n        }\n    });\n\n    futs.add(asyncCompute.future());\n}\n\n// Wait for completion of all futures.\nfor (ComputeTaskFuture<?> f : futs)\n  f.get();",
      "language": "java",
      "name": "java7 async run"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Apply Methods"
}
[/block]
A closure is a block of code that encloses its body and any outside variables used inside of it as a function object. You can then pass such function object anywhere you can pass a variable and execute it. All apply(...) methods execute closures on the cluster. 
[block:code]
{
  "codes": [
    {
      "code": "IgniteCompute compute  = ignite.compute();\n\n// Execute closure on all cluster nodes.\nCollection<Integer> res = ignite.compute().apply(\n    String::length,\n    Arrays.asList(\"Count characters using closure\".split(\" \"))\n);\n     \n// Add all the word lengths received from cluster nodes.\nint total = res.stream().mapToInt(Integer::intValue).sum(); ",
      "language": "java",
      "name": "apply"
    },
    {
      "code": "// Enable asynchronous mode.\nIgniteCompute asyncCompute = ignite.compute().withAsync();\n\n// Execute closure on all cluster nodes.\n// If the number of closures is less than the number of \n// parameters, then Ignite will create as many closures \n// as there are parameters.\nCollection<Integer> res = ignite.compute().apply(\n    String::length,\n    Arrays.asList(\"Count characters using closure\".split(\" \"))\n);\n     \nasyncCompute.future().listenAsync(fut -> {\n    // Total number of characters.\n    int total = fut.get().stream().mapToInt(Integer::intValue).sum(); \n  \n    System.out.println(\"Total number of characters: \" + total);\n});",
      "language": "java",
      "name": "async apply"
    },
    {
      "code": "// Execute closure on all cluster nodes.\n// If the number of closures is less than the number of \n// parameters, then Ignite will create as many closures \n// as there are parameters.\nCollection<Integer> res = ignite.compute().apply(\n    new IgniteClosure<String, Integer>() {\n        @Override public Integer apply(String word) {\n            // Return number of letters in the word.\n            return word.length();\n        }\n    },\n    Arrays.asList(\"Count characters using closure\".split(\" \"))\n).get();\n     \nint sum = 0;\n \n// Add up individual word lengths received from remote nodes\nfor (int len : res)\n    sum += len;",
      "language": "java",
      "name": "java7 apply"
    }
  ]
}
[/block]