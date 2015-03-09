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

[block:api-header]
{
  "type": "basic",
  "title": "Prerequisites"
}
[/block]
Apache Ignite was officially tested on:
[block:parameters]
{
  "data": {
    "h-0": "Name",
    "h-1": "Value",
    "0-0": "JDK",
    "0-1": "Oracle JDK 7 and above",
    "1-0": "OS",
    "2-0": "Network",
    "1-1": "Linux (any flavor),\nMac OSX (10.6 and up)\nWindows (XP and up), \nWindows Server (2008 and up)",
    "2-1": "No restrictions (10G recommended)",
    "3-0": "Hardware",
    "3-1": "No restrictions"
  },
  "cols": 2,
  "rows": 3
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Installation"
}
[/block]
Here is the quick summary on installation of Apache Ignite:
  * Download Apache Ignite as ZIP archive from https://ignite.incubator.apache.org/
  * Unzip ZIP archive into the installation folder in your system
  * Set `IGNITE_HOME` environment variable to point to the installation folder and make sure there is no trailing `/` in the path (this step is optional)
[block:api-header]
{
  "type": "basic",
  "title": "Start From Command Line"
}
[/block]
An Ignite node can be started from command line either with default configuration or by passing a configuration file. You can start as many nodes as you like and they will all automatically discover each other. 

##With Default Configuration
To start a grid node with default configuration, open the command shell and, assuming you are in `IGNITE_HOME` (Ignite installation folder), just type this:
[block:code]
{
  "codes": [
    {
      "code": "$ bin/ignite.sh",
      "language": "shell"
    }
  ]
}
[/block]
and you will see the output similar to this:
[block:code]
{
  "codes": [
    {
      "code": "[02:49:12] Ignite node started OK (id=ab5d18a6)\n[02:49:12] Topology snapshot [ver=1, nodes=1, CPUs=8, heap=1.0GB]",
      "language": "text"
    }
  ]
}
[/block]
By default `ignite.sh` starts Ignite node with the default configuration: `config/default-config.xml`.

##Passing Configuration File 
To pass configuration file explicitly,  from command line, you can type ggstart.sh <path to configuration file> from within your Ignite installation folder. For example:
[block:code]
{
  "codes": [
    {
      "code": "$ bin/ignite.sh examples/config/example-cache.xml",
      "language": "shell"
    }
  ]
}
[/block]
Path to configuration file can be absolute, or relative to either `IGNITE_HOME` (Ignite installation folder) or `META-INF` folder in your classpath. 
[block:callout]
{
  "type": "success",
  "title": "Interactive Mode",
  "body": "To pick a configuration file in interactive mode just pass `-i` flag, like so: `ignite.sh -i`."
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Get It With Maven"
}
[/block]
Another easy way to get started with Apache Ignite in your project is to use Maven 2 dependency management.

Ignite requires only one `ignite-core` mandatory dependency. Usually you will also need to add `ignite-spring` for spring-based XML configuration and `ignite-indexing` for SQL querying.

Replace `${ignite-version}` with actual Ignite version.
[block:code]
{
  "codes": [
    {
      "code": "<dependency>\n    <groupId>org.apache.ignite</groupId>\n    <artifactId>ignite-core</artifactId>\n    <version>${ignite.version}</version>\n</dependency>\n<dependency>\n    <groupId>org.apache.ignite</groupId>\n    <artifactId>ignite-spring</artifactId>\n    <version>${ignite.version}</version>\n</dependency>\n<dependency>\n    <groupId>org.apache.ignite</groupId>\n    <artifactId>ignite-indexing</artifactId>\n    <version>${ignite.version}</version>\n</dependency>",
      "language": "xml"
    }
  ]
}
[/block]

[block:callout]
{
  "type": "success",
  "title": "Maven Setup",
  "body": "See [Maven Setup](/docs/maven-setup) for more information on how to include individual Ignite maven artifacts."
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "First Ignite Compute Application"
}
[/block]
Let's write our first grid application which will count a number of non-white-space characters in a sentence. As an example, we will take a sentence, split it into multiple words, and have every compute job count number of characters in each individual word. At the end we simply add up results received from individual jobs to get our total count.
[block:code]
{
  "codes": [
    {
      "code": "try (Ignite ignite = Ignition.start()) {\n  Collection<IgniteCallable<Integer>> calls = new ArrayList<>();\n\n  // Iterate through all the words in the sentence and create Callable jobs.\n  for (final String word : \"Count characters using callable\".split(\" \"))\n    calls.add(word::length);\n\n  // Execute collection of Callables on the grid.\n  Collection<Integer> res = ignite.compute().call(calls);\n\n  int sum = res.stream().mapToInt(Integer::intValue).sum();\n \n\tSystem.out.println(\"Total number of characters is '\" + sum + \"'.\");\n}",
      "language": "java",
      "name": "compute"
    },
    {
      "code": "try (Ignite ignite = Ignition.start()) {\n    Collection<IgniteCallable<Integer>> calls = new ArrayList<>();\n \n    // Iterate through all the words in the sentence and create Callable jobs.\n    for (final String word : \"Count characters using callable\".split(\" \")) {\n        calls.add(new IgniteCallable<Integer>() {\n            @Override public Integer call() throws Exception {\n                return word.length();\n            }\n        });\n    }\n \n    // Execute collection of Callables on the grid.\n    Collection<Integer> res = ignite.compute().call(calls);\n \n    int sum = 0;\n \n    // Add up individual word lengths received from remote nodes.\n    for (int len : res)\n        sum += len;\n \n    System.out.println(\">>> Total number of characters in the phrase is '\" + sum + \"'.\");\n}",
      "language": "java",
      "name": "java7 compute"
    }
  ]
}
[/block]

[block:callout]
{
  "type": "success",
  "body": "Note that because of  [Zero Deployment](doc:zero-deployment) feature, when running the above application from your IDE, remote nodes will execute received jobs without explicit deployment.",
  "title": "Zero Deployment"
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "First Ignite Data Grid Application"
}
[/block]
Now let's write a simple set of mini-examples which will put and get values to/from distributed cache, and perform basic transactions.

Since we are using cache in this example, we should make sure that it is configured. Let's use example configuration shipped with Ignite that already has several caches configured: 
[block:code]
{
  "codes": [
    {
      "code": "$ bin/ignite.sh examples/config/example-cache.xml",
      "language": "shell"
    }
  ]
}
[/block]

[block:code]
{
  "codes": [
    {
      "code": "try (Ignite ignite = Ignition.start(\"examples/config/example-cache.xml\")) {\n    IgniteCache<Integer, String> cache = ignite.jcache(CACHE_NAME);\n \n    // Store keys in cache (values will end up on different cache nodes).\n    for (int i = 0; i < 10; i++)\n        cache.put(i, Integer.toString(i));\n \n    for (int i = 0; i < 10; i++)\n        System.out.println(\"Got [key=\" + i + \", val=\" + cache.get(i) + ']');\n}",
      "language": "java",
      "name": "Put and Get"
    },
    {
      "code": "// Put-if-absent which returns previous value.\nInteger oldVal = cache.getAndPutIfAbsent(\"Hello\", 11);\n  \n// Put-if-absent which returns boolean success flag.\nboolean success = cache.putIfAbsent(\"World\", 22);\n  \n// Replace-if-exists operation (opposite of getAndPutIfAbsent), returns previous value.\noldVal = cache.getAndReplace(\"Hello\", 11);\n \n// Replace-if-exists operation (opposite of putIfAbsent), returns boolean success flag.\nsuccess = cache.replace(\"World\", 22);\n  \n// Replace-if-matches operation.\nsuccess = cache.replace(\"World\", 2, 22);\n  \n// Remove-if-matches operation.\nsuccess = cache.remove(\"Hello\", 1);",
      "language": "java",
      "name": "Atomic Operations"
    },
    {
      "code": "try (Transaction tx = ignite.transactions().txStart()) {\n    Integer hello = cache.get(\"Hello\");\n  \n    if (hello == 1)\n        cache.put(\"Hello\", 11);\n  \n    cache.put(\"World\", 22);\n  \n    tx.commit();\n}",
      "language": "java",
      "name": "Transactions"
    },
    {
      "code": "// Lock cache key \"Hello\".\nLock lock = cache.lock(\"Hello\");\n \nlock.lock();\n \ntry {\n    cache.put(\"Hello\", 11);\n    cache.put(\"World\", 22);\n}\nfinally {\n    lock.unlock();\n} ",
      "language": "java",
      "name": "Distributed Locks"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Ignite Visor Admin Console"
}
[/block]
The easiest way to examine the content of the data grid as well as perform a long list of other management and monitoring operations is to use Ignite Visor Command Line Utility.

To start Visor simply run:
[block:code]
{
  "codes": [
    {
      "code": "$ bin/ignitevisorcmd.sh",
      "language": "shell"
    }
  ]
}
[/block]