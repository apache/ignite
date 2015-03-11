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

If you are familiar with `java.util.concurrent.CountDownLatch` for synchronization between threads within a single JVM, Ignite provides `IgniteCountDownLatch` to allow similar behavior across cluster nodes. 

A distributed CountDownLatch in Ignite can be created as follows:
[block:code]
{
  "codes": [
    {
      "code": "Ignite ignite = Ignition.ignite();\n\nIgniteCountDownLatch latch = ignite.countDownLatch(\n    \"latchName\", // Latch name.\n    10,        \t // Initial count.\n    false        // Auto remove, when counter has reached zero.\n    true         // Create if it does not exist.\n);",
      "language": "java"
    }
  ]
}
[/block]
After the above code is executed, all nodes in the specified cache will be able to synchronize on the latch named - `latchName`. Below is an example of such synchronization:
[block:code]
{
  "codes": [
    {
      "code": "Ignite ignite = Ignition.ignite();\n\nfinal IgniteCountDownLatch latch = ignite.countDownLatch(\"latchName\", 10, false, true);\n\n// Execute jobs.\nfor (int i = 0; i < 10; i++)\n    // Execute a job on some remote cluster node.\n    ignite.compute().run(() -> {\n        int newCnt = latch.countDown();\n\n        System.out.println(\"Counted down: newCnt=\" + newCnt);\n    });\n\n// Wait for all jobs to complete.\nlatch.await();",
      "language": "java"
    }
  ]
}
[/block]