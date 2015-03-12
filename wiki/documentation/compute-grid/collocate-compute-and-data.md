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

Collocation of computations with data allow for minimizing data serialization within network and can significantly improve performance and scalability of your application. Whenever possible, you should alway make best effort to colocate your computations with the cluster nodes caching the data that needs to be processed.
[block:api-header]
{
  "type": "basic",
  "title": "Affinity Call and Run Methods"
}
[/block]
`affinityCall(...)`  and `affinityRun(...)` methods co-locate jobs with nodes on which data is cached. In other words, given a cache name and affinity key these methods try to locate the node on which the key resides on Ignite the specified Ignite cache, and then execute the job there. 
[block:code]
{
  "codes": [
    {
      "code": "IgniteCache<Integer, String> cache = ignite.cache(CACHE_NAME);\n\nIgniteCompute compute = ignite.compute();\n\nfor (int key = 0; key < KEY_CNT; key++) {\n    // This closure will execute on the remote node where\n    // data with the 'key' is located.\n    compute.affinityRun(CACHE_NAME, key, () -> { \n        // Peek is a local memory lookup.\n        System.out.println(\"Co-located [key= \" + key + \", value= \" + cache.peek(key) +']');\n    });\n}",
      "language": "java",
      "name": "affinityRun"
    },
    {
      "code": "IgniteCache<Integer, String> cache = ignite.cache(CACHE_NAME);\n\nIgniteCompute asyncCompute = ignite.compute().withAsync();\n\nList<IgniteFuture<?>> futs = new ArrayList<>();\n\nfor (int key = 0; key < KEY_CNT; key++) {\n    // This closure will execute on the remote node where\n    // data with the 'key' is located.\n    asyncCompute.affinityRun(CACHE_NAME, key, () -> { \n        // Peek is a local memory lookup.\n        System.out.println(\"Co-located [key= \" + key + \", value= \" + cache.peek(key) +']');\n    });\n  \n    futs.add(asyncCompute.future());\n}\n\n// Wait for all futures to complete.\nfuts.stream().forEach(IgniteFuture::get);",
      "language": "java",
      "name": "async affinityRun"
    },
    {
      "code": "final IgniteCache<Integer, String> cache = ignite.cache(CACHE_NAME);\n\nIgniteCompute compute = ignite.compute();\n\nfor (int i = 0; i < KEY_CNT; i++) {\n    final int key = i;\n \n    // This closure will execute on the remote node where\n    // data with the 'key' is located.\n    compute.affinityRun(CACHE_NAME, key, new IgniteRunnable() {\n        @Override public void run() {\n            // Peek is a local memory lookup.\n            System.out.println(\"Co-located [key= \" + key + \", value= \" + cache.peek(key) +']');\n        }\n    });\n}",
      "language": "java",
      "name": "java7 affinityRun"
    }
  ]
}
[/block]