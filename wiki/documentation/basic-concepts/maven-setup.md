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

If you are using Maven to manage dependencies of your project, you can import individual Ignite modules a la carte.
[block:callout]
{
  "type": "info",
  "body": "In the examples below, please replace `${ignite.version}` with actual Apache Ignite version you are interested in."
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Common Dependencies"
}
[/block]
Ignite data fabric comes with one mandatory dependency on `ignite-core.jar`. 
[block:code]
{
  "codes": [
    {
      "code": "<dependency>\n    <groupId>org.apache.ignite</groupId>\n    <artifactId>ignite-core</artifactId>\n    <version>${ignite.version}</version>\n</dependency>",
      "language": "xml"
    }
  ]
}
[/block]
However, in many cases may wish to have more dependencies, for example, if you want to use Spring configuration or SQL queries.

Here are the most commonly used optional modules:
  * ignite-indexing (optional, add if you need SQL indexing)
  * ignite-spring (optional, add if you plan to use Spring configuration) 
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

[block:api-header]
{
  "type": "basic",
  "title": "Importing Individual Modules A La Carte"
}
[/block]
You can import Ignite modules a la carte, one by one. The only required module is `ignite-core`, all others are optional. All optional modules can be imported just like the core module, but with different artifact IDs.

The following modules are available:
  * `ignite-spring` (for Spring-based configuration support)
  * `ignite-indexing` (for SQL querying and indexing)
  * `ignite-geospatial` (for geospatial indexing)
  * `ignite-hibernate` (for Hibernate integration)
  * `ignite-web` (for Web Sessions Clustering)
  * `ignite-schedule` (for Cron-based task scheduling)
  * `ignite-logj4` (for Log4j logging)
  * `ignite-jcl` (for Apache Commons logging)
  * `ignite-jta` (for XA integration)
  * `ignite-hadoop2-integration` (Integration with HDFS 2.0)
  * `ignite-rest-http` (for HTTP REST messages)
  * `ignite-scalar` (for Ignite Scala API)
  * `ignite-sl4j` (for SL4J logging)
  * `ignite-ssh` (for starting grid nodes on remote machines)
  * `ignite-urideploy` (for URI-based deployment)
  * `ignite-aws` (for seamless cluster discovery on AWS S3)
  * `ignite-aop` (for AOP-based grid-enabling)
  * `ignite-visor-console`  (open source command line management and monitoring tool)