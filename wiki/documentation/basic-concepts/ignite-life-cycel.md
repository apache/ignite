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

Ignite is JVM-based. Single JVM represents one or more logical Ignite nodes (most of the time, however, a single JVM runs just one Ignite node). Throughout Ignite documentation we use term Ignite runtime and Ignite node almost interchangeably. For example, when we say that you can "run 5 nodes on this host" - in most cases it technically means that you can start 5 JVMs on this host each running a single Ignite node. Ignite also supports multiple Ignite nodes in a single JVM. In fact, that is exactly how most of the internal tests run for Ignite itself.
[block:callout]
{
  "type": "success",
  "body": "Ignite runtime == JVM process == Ignite node (in most cases)"
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Ignition Class"
}
[/block]
The `Ignition` class starts individual Ignite nodes in the network topology. Note that a physical server (like a computer on the network) can have multiple Ignite nodes running on it.

Here is how you can start grid node locally with all defaults
[block:code]
{
  "codes": [
    {
      "code": "Ignite ignite = Ignition.start();",
      "language": "java"
    }
  ]
}
[/block]
or by passing a configuration file:
[block:code]
{
  "codes": [
    {
      "code": "Ignite ignite = Ignition.start(\"examples/config/example-cache.xml\");",
      "language": "java"
    }
  ]
}
[/block]

Path to configuration file can be absolute, or relative to either `IGNITE_HOME` (Ignite installation folder) or `META-INF` folder in your classpath.
[block:api-header]
{
  "type": "basic",
  "title": "LifecycleBean"
}
[/block]
Sometimes you need to perform certain actions before or after the Ignite node starts or stops. This can be done by implementing `LifecycleBean` interface, and specifying the implementation bean in `lifecycleBeans` property of `IgniteConfiguration` in the spring XML file:
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.IgniteConfiguration\">\n    ...\n    <property name=\"lifecycleBeans\">\n        <list>\n            <bean class=\"com.mycompany.MyGridLifecycleBean\"/>\n        </list>\n    </property>\n    ...\n</bean>",
      "language": "xml"
    }
  ]
}
[/block]
`GridLifeCycleBean` can also configured programmatically the following way:
[block:code]
{
  "codes": [
    {
      "code": "// Create new configuration.\nIgniteConfiguration cfg = new IgniteConfiguration();\n \n// Provide lifecycle bean to configuration.\ncfg.setLifecycleBeans(new MyGridLifecycleBean());\n \n// Start Ignite node with given configuration.\nIgnite ignite = GridGain.start(cfg)",
      "language": "java"
    }
  ]
}
[/block]
An implementation of `LifecycleBean` may look like the following:
[block:code]
{
  "codes": [
    {
      "code": "public class MyLifecycleBean implements LifecycleBean {\n    @Override public void onLifecycleEvent(LifecycleEventType evt) {\n        if (evt == LifecycleEventType.BEFORE_GRID_START) {\n            // Do something.\n            ...\n        }\n    }\n}",
      "language": "java"
    }
  ]
}
[/block]
You can inject Ignite instance and other useful resources into a `LifecycleBean` implementation. Please refer to [Resource Injection](/docs/resource-injection) section for more information.
[block:api-header]
{
  "type": "basic",
  "title": "Lifecycle Event Types"
}
[/block]
The following lifecycle event types are supported:
[block:parameters]
{
  "data": {
    "h-0": "Event Type",
    "h-1": "Description",
    "0-0": "BEFORE_NODE_START",
    "0-1": "Invoked before Ignite node startup routine is initiated.",
    "1-0": "AFTER_NODE_START",
    "1-1": "Invoked right after Ignite node has started.",
    "2-0": "BEFORE_NODE_STOP",
    "2-1": "Invoked right before Ignite stop routine is initiated.",
    "3-0": "AFTER_NODE_STOP",
    "3-1": "Invoked right after Ignite node has stopped."
  },
  "cols": 2,
  "rows": 4
}
[/block]