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

Ignite distributed events functionality allows applications to receive notifications when a variety of events occur in the distributed grid environment. You can automatically get notified for task executions, read, write or query operations occurring on local or remote nodes within the cluster.

Distributed events functionality is provided via `IgniteEvents` interface. You can get an instance of `IgniteEvents` from Ignite as follows:
[block:code]
{
  "codes": [
    {
      "code": "Ignite ignite = Ignition.ignite();\n\nIgniteEvents evts = ignite.events();",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Subscribe for Events"
}
[/block]
Listen methods can be used to receive notification for specified events happening in the cluster. These methods register a listener on local or remotes nodes for the specified events. Whenever the event occurs on the node, the listener is notified. 
##Local Events
`localListen(...)`  method registers event listeners with specified events on local node only.
##Remote Events
`remoteListen(...)` method registers event listeners with specified events on all nodes within the cluster or cluster group. Following is an example of each method:
[block:code]
{
  "codes": [
    {
      "code": "Ignite ignite = Ignition.ignite();\n\n// Local listener that listenes to local events.\nIgnitePredicate<CacheEvent> locLsnr = evt -> {\n  System.out.println(\"Received event [evt=\" + evt.name() + \", key=\" + evt.key() + \n    \", oldVal=\" + evt.oldValue() + \", newVal=\" + evt.newValue());\n\n  return true; // Continue listening.\n};\n\n// Subscribe to specified cache events occuring on local node.\nignite.events().localListen(locLsnr,\n  EventType.EVT_CACHE_OBJECT_PUT,\n  EventType.EVT_CACHE_OBJECT_READ,\n  EventType.EVT_CACHE_OBJECT_REMOVED);\n\n// Get an instance of named cache.\nfinal IgniteCache<Integer, String> cache = ignite.jcache(\"cacheName\");\n\n// Generate cache events.\nfor (int i = 0; i < 20; i++)\n  cache.put(i, Integer.toString(i));\n",
      "language": "java",
      "name": "local listen"
    },
    {
      "code": "Ignite ignite = Ignition.ignite();\n\n// Get an instance of named cache.\nfinal IgniteCache<Integer, String> cache = ignite.jcache(\"cacheName\");\n\n// Sample remote filter which only accepts events for keys\n// that are greater than or equal to 10.\nIgnitePredicate<CacheEvent> rmtLsnr = evt -> evt.<Integer>key() >= 10;\n\n// Subscribe to specified cache events on all nodes that have cache running.\nignite.events(ignite.cluster().forCacheNodes(\"cacheName\")).remoteListen(null, rmtLsnr,                                                                 EventType.EVT_CACHE_OBJECT_PUT,\n  EventType.EVT_CACHE_OBJECT_READ,\n  EventType.EVT_CACHE_OBJECT_REMOVED);\n\n// Generate cache events.\nfor (int i = 0; i < 20; i++)\n  cache.put(i, Integer.toString(i));\n",
      "language": "java",
      "name": "remote listen"
    },
    {
      "code": "Ignite ignite = Ignition.ignite();\n \n// Get an instance of named cache.\nfinal IgniteCache<Integer, String> cache = ignite.jcache(\"cacheName\");\n \n// Sample remote filter which only accepts events for keys\n// that are greater than or equal to 10.\nIgnitePredicate<CacheEvent> rmtLsnr = new IgnitePredicate<CacheEvent>() {\n    @Override public boolean apply(CacheEvent evt) {\n        System.out.println(\"Cache event: \" + evt);\n \n        int key = evt.key();\n \n        return key >= 10;\n    }\n};\n \n// Subscribe to specified cache events occuring on \n// all nodes that have the specified cache running.\nignite.events(ignite.cluster().forCacheNodes(\"cacheName\")).remoteListen(null, rmtLsnr,                                                                 EVT_CACHE_OBJECT_PUT,                                      \t\t    \t\t   EVT_CACHE_OBJECT_READ,                                                     EVT_CACHE_OBJECT_REMOVED);\n \n// Generate cache events.\nfor (int i = 0; i < 20; i++)\n    cache.put(i, Integer.toString(i));",
      "language": "java",
      "name": "java7 listen"
    }
  ]
}
[/block]
In the above example `EVT_CACHE_OBJECT_PUT`,`EVT_CACHE_OBJECT_READ`, and `EVT_CACHE_OBJECT_REMOVED` are pre-defined event type constants defined in `EventType` interface.  
[block:callout]
{
  "type": "info",
  "body": "`EventType` interface defines various event type constants that can be used with listen methods. Refer to [javadoc](https://ignite.incubator.apache.org/releases/1.0.0/javadoc/) for complete list of these event types."
}
[/block]

[block:callout]
{
  "type": "warning",
  "body": "Event types passed in as parameter in  `localListen(...)` and `remoteListen(...)` methods must also be configured in `IgniteConfiguration`. See [configuration](#configuration) example below."
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Query for Events"
}
[/block]
All events generated in the system are kept locally on the local node. `IgniteEvents` API provides methods to query for these events.
##Local Events
`localQuery(...)`  method queries for events on the local node using the passed in predicate filter. If all predicates are satisfied, a collection of events happening on the local node is returned.
##Remote Events
`remoteQuery(...)`  method asynchronously queries for events on remote nodes in this projection using the passed in predicate filter. This operation is distributed and hence can fail on communication layer and generally can take much longer than local event notifications. Note that this method will not block and will return immediately with future.

[block:api-header]
{
  "type": "basic",
  "title": "Configuration"
}
[/block]
To get notified of any tasks or cache events occurring within the cluster, `includeEventTypes` property of `IgniteConfiguration` must be enabled.  
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.configuration.IgniteConfiguration\">\n \t\t... \n    <!-- Enable cache events. -->\n    <property name=\"includeEventTypes\">\n        <util:constant static-field=\"org.apache.ignite.events.EventType.EVTS_CACHE\"/>\n    </property>\n  \t...\n</bean>",
      "language": "xml"
    },
    {
      "code": "IgniteConfiguration cfg = new IgniteConfiguration();\n\n// Enable cache events.\ncfg.setIncludeEventTypes(EVTS_CACHE);\n\n// Start Ignite node.\nIgnition.start(cfg);",
      "language": "java"
    }
  ]
}
[/block]
By default, event notifications are turned off for performance reasons.
[block:callout]
{
  "type": "success",
  "body": "Since thousands of events per second are generated, it creates an additional load on the system. This can lead to significant performance degradation. Therefore, it is highly recommended to enable only those events that your application logic requires."
}
[/block]