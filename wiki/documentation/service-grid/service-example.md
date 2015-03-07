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
  "title": "Define Your Service Interface"
}
[/block]
As an example, let's define a simple counter service as a  `MyCounterService` interface. Note that this is a simple Java interface without any special annotations or methods.
[block:code]
{
  "codes": [
    {
      "code": "public class MyCounterService {\n    /**\n     * Increment counter value and return the new value.\n     */\n    int increment() throws CacheException;\n     \n    /**\n     * Get current counter value.\n     */\n    int get() throws CacheException;\n}",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Service Implementation"
}
[/block]
An implementation of a distributed service has to implement both, `Service` and `MyCounterService` interfaces. 

We implement our counter service by storing the counter value in cache. The key for this counter value is the name of the service. This allows us to reuse the same cache for multiple instances of the counter service.
[block:code]
{
  "codes": [
    {
      "code": "public class MyCounterServiceImpl implements Service, MyCounterService {\n  /** Auto-injected instance of Ignite. */\n  @IgniteInstanceResource\n  private Ignite ignite;\n\n  /** Distributed cache used to store counters. */\n  private IgniteCache<String, Integer> cache;\n\n  /** Service name. */\n  private String svcName;\n\n  /**\n   * Service initialization.\n   */\n  @Override public void init(ServiceContext ctx) {\n    // Pre-configured cache to store counters.\n    cache = ignite.cache(\"myCounterCache\");\n\n    svcName = ctx.name();\n\n    System.out.println(\"Service was initialized: \" + svcName);\n  }\n\n  /**\n   * Cancel this service.\n   */\n  @Override public void cancel(ServiceContext ctx) {\n    // Remove counter from cache.\n    cache.remove(svcName);\n    \n    System.out.println(\"Service was cancelled: \" + svcName);\n  }\n\n  /**\n   * Start service execution.\n   */\n  @Override public void execute(ServiceContext ctx) {\n    // Since our service is simply represented by a counter\n    // value stored in cache, there is nothing we need\n    // to do in order to start it up.\n    System.out.println(\"Executing distributed service: \" + svcName);\n  }\n\n  @Override public int get() throws CacheException {\n    Integer i = cache.get(svcName);\n\n    return i == null ? 0 : i;\n  }\n\n  @Override public int increment() throws CacheException {\n    return cache.invoke(svcName, new CounterEntryProcessor());\n  }\n\n  /**\n   * Entry processor which atomically increments value currently stored in cache.\n   */\n  private static class CounterEntryProcessor implements EntryProcessor<String, Integer, Integer> {\n    @Override public Integer process(MutableEntry<String, Integer> e, Object... args) {\n      int newVal = e.exists() ? e.getValue() + 1 : 1;\n      \n      // Update cache.\n      e.setValue(newVal);\n\n      return newVal;\n    }      \n  }\n}",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Service Deployment"
}
[/block]
We should deploy our counter service as per-node-singleton within the cluster group that has our cache "myCounterCache" deployed.
[block:code]
{
  "codes": [
    {
      "code": "// Cluster group which includes all caching nodes.\nClusterGroup cacheGrp = ignite.cluster().forCache(\"myCounterService\");\n\n// Get an instance of IgniteServices for the cluster group.\nIgniteServices svcs = ignite.services(cacheGrp);\n \n// Deploy per-node singleton. An instance of the service\n// will be deployed on every node within the cluster group.\nsvcs.deployNodeSingleton(\"myCounterService\", new MyCounterServiceImpl());",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Service Proxy"
}
[/block]
You can access an instance of the deployed service from any node within the cluster. If the service is deployed on that node, then the locally deployed instance will be returned. Otherwise, if service is not locally available, a remote proxy for the service will be created automatically.

# Sticky vs Not-Sticky Proxies
Proxies can be either *sticky* or not. If proxy is sticky, then Ignite will always go back to the same cluster node to contact a remotely deployed service. If proxy is *not-sticky*, then Ignite will load balance remote service proxy invocations among all cluster nodes on which the service is deployed.
[block:code]
{
  "codes": [
    {
      "code": "// Get service proxy for the deployed service.\nMyCounterService cntrSvc = ignite.services().\n  serviceProxy(\"myCounterService\", MyCounterService.class, /*not-sticky*/false);\n\n// Ivoke a method on 'MyCounterService' interface.\ncntrSvc.increment();\n\n// Print latest counter value from our counter service.\nSystem.out.println(\"Incremented value : \" + cntrSvc.get());",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Access Service from Computations"
}
[/block]
For convenience, you can inject an instance of service proxy into your computation using `@ServiceResource` annotation.
[block:code]
{
  "codes": [
    {
      "code": "IgniteCompute compute = igntie.compute();\n\ncompute.run(new IgniteRunnable() {\n  @ServiceResource(serviceName = \"myCounterService\");\n  private MyCounterService counterSvc;\n  \n  public void run() {\n\t\t// Ivoke a method on 'MyCounterService' interface.\n\t\tint newValue = cntrSvc.increment();\n\n\t\t// Print latest counter value from our counter service.\n\t\tSystem.out.println(\"Incremented value : \" + newValue);\n  }\n});",
      "language": "java"
    }
  ]
}
[/block]