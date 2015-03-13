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

Ignite provides HTTP REST client that gives you the ability to communicate with the grid over HTTP and HTTPS protocols using REST approach. REST APIs can be used to perform various operations like read/write from/to cache, execute tasks, get cache and node metrics  and more.

* [Returned value](#returned-value)
* [Log](#log)
* [Version](#version)
* [Decrement](#decrement)
* [Increment](#increment)
* [Cache metrics](#cache-metrics)
* [Compare-And-Swap](#compare-and-swap)
* [Prepend](#prepend)
* [Append](#append)
* [Replace](#replace)
* [Remove all](#remove-all)
* [Remove](#remove) 
* [Add](#add)
* [Put all](#put-all)
* [Put](#put) 
* [Get all](#get-all)
* [Get](#get)
* [Node](#node)
* [Topology](#topology)
* [Execute](#execute)
* [Result](#result)

[block:api-header]
{
  "type": "basic",
  "title": "Returned value"
}
[/block]
HTTP REST request returns JSON object which has similar structure for each command. This object has the following structure:
[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "description",
    "h-3": "example",
    "0-0": "affinityNodeId",
    "0-1": "string",
    "0-2": "Affinity node ID.",
    "0-3": "2bd7b049-3fa0-4c44-9a6d-b5c7a597ce37",
    "1-0": "error",
    "1-1": "string",
    "1-2": "The field contains description of error if server could not handle the request",
    "1-3": "specifically for each command",
    "2-0": "response",
    "2-1": "jsonObject",
    "2-2": "The field contains result of command.",
    "2-3": "specifically for each command",
    "3-0": "successStatus",
    "3-1": "integer",
    "3-2": "Exit status code. It might have the following values:\n  * success = 0\n  * failed = 1 \n  * authorization failed = 2\n  * security check failed = 3",
    "3-3": "0"
  },
  "cols": 4,
  "rows": 4
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Log"
}
[/block]
**Log** command shows server logs.
[block:code]
{
  "codes": [
    {
      "code": "http://host:port/ignite?cmd=log&from=10&to=100&path=/var/log/ignite.log",
      "language": "curl"
    }
  ]
}
[/block]
##Request Parameters
[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "optional",
    "h-3": "decription",
    "h-4": "example",
    "0-0": "cmd",
    "0-3": "Should be **log** lowercase.",
    "0-4": "",
    "0-1": "string",
    "0-2": "No",
    "1-0": "from",
    "1-1": "integer",
    "1-2": "Yes",
    "1-3": "Number of line to start from. Parameter is mandatory if **to** is passed.",
    "1-4": "0",
    "2-0": "path",
    "2-1": "string",
    "2-2": "Yes",
    "2-3": "The path to log file. If not provided, will be used the following value **work/log/ignite.log**",
    "2-4": "log/cache_server.log",
    "3-0": "to",
    "3-1": "integer",
    "3-2": "Yes",
    "3-3": "Number to line to finish on. Parameter is mandatory if **from** is passed.",
    "3-4": "1000"
  },
  "cols": 5,
  "rows": 4
}
[/block]
##Response example:
[block:code]
{
  "codes": [
    {
      "code": "{\n  \"error\": \"\",\n  \"response\": [\"[14:01:56,626][INFO ][test-runner][GridDiscoveryManager] Topology snapshot [ver=1, nodes=1, CPUs=8, heap=1.8GB]\"],\n  \"successStatus\": 0\n}",
      "language": "json"
    }
  ]
}
[/block]

[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "description",
    "h-3": "example",
    "0-0": "response",
    "0-3": "[\"[14:01:56,626][INFO ][test-runner][GridDiscoveryManager] Topology snapshot [ver=1, nodes=1, CPUs=8, heap=1.8GB]\"]",
    "0-1": "string",
    "0-2": "logs"
  },
  "cols": 4,
  "rows": 1
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Version"
}
[/block]
**Version**  command shows current Ignite version.
[block:code]
{
  "codes": [
    {
      "code": "http://host:port/ignite?cmd=version",
      "language": "curl"
    }
  ]
}
[/block]
##Request Parameters
[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "optional",
    "h-3": "description",
    "h-4": "example",
    "0-0": "cmd",
    "0-4": "",
    "0-1": "string",
    "0-3": "Should be **version** lowercase."
  },
  "cols": 5,
  "rows": 1
}
[/block]
##Response example
[block:code]
{
  "codes": [
    {
      "code": "{\n  \"error\": \"\",\n  \"response\": \"1.0.0\",\n  \"successStatus\": 0\n}",
      "language": "json"
    }
  ]
}
[/block]

[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "description",
    "h-3": "example",
    "0-0": "response",
    "0-3": "1.0.0",
    "0-1": "string",
    "0-2": "Ignite version"
  },
  "cols": 4,
  "rows": 1
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Decrement"
}
[/block]
**Decrement** command subtracts and gets current value of given atomic long.
[block:code]
{
  "codes": [
    {
      "code": "http://host:port/ignite?cmd=decr&cacheName=partionedCache&key=decrKey&init=15&delta=10",
      "language": "curl"
    }
  ]
}
[/block]
##Request parameters
[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "optional",
    "h-3": "decription",
    "h-4": "example",
    "0-0": "cmd",
    "0-4": "",
    "0-1": "string",
    "0-3": "Should be **decr** lowercase.",
    "1-0": "cacheName",
    "1-1": "string",
    "1-2": "Yes",
    "1-3": "Cache name. If not provided, default cache will be used.",
    "1-4": "partionedCache",
    "2-0": "key",
    "2-1": "string",
    "2-3": "The name of atomic long.",
    "2-4": "counter",
    "3-0": "init",
    "3-1": "long",
    "3-2": "Yes",
    "3-3": "Initial value.",
    "3-4": "15",
    "4-4": "42",
    "4-3": "Number to be subtracted.",
    "4-0": "delta",
    "4-1": "long"
  },
  "cols": 5,
  "rows": 5
}
[/block]
##Response example
[block:code]
{
  "codes": [
    {
      "code": "{\n  \"affinityNodeId\": \"e05839d5-6648-43e7-a23b-78d7db9390d5\",\n  \"error\": \"\",\n  \"response\": -42,\n  \"successStatus\": 0\n}",
      "language": "json"
    }
  ]
}
[/block]

[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "description",
    "h-3": "example",
    "0-0": "response",
    "0-1": "long",
    "0-2": "Value after operation.",
    "0-3": "-42"
  },
  "cols": 4,
  "rows": 1
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Increment"
}
[/block]
**Increment** command adds and gets current value of given atomic long.
[block:code]
{
  "codes": [
    {
      "code": "http://host:port/ignite?cmd=incr&cacheName=partionedCache&key=decrKey&init=15&delta=10",
      "language": "curl"
    }
  ]
}
[/block]
##Request parameters
[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "optional",
    "h-3": "decription",
    "h-4": "example",
    "0-0": "cmd",
    "0-4": "",
    "0-1": "string",
    "0-3": "Should be ** incr** lowercase.",
    "1-0": "cacheName",
    "1-1": "string",
    "1-2": "Yes",
    "1-3": "Cache name. If not provided, default cache will be used.",
    "1-4": "partionedCache",
    "2-0": "key",
    "2-1": "string",
    "2-3": "The name of atomic long.",
    "2-4": "counter",
    "3-0": "init",
    "3-1": "long",
    "3-2": "Yes",
    "3-3": "Initial value.",
    "3-4": "15",
    "4-4": "42",
    "4-3": "Number to be added.",
    "4-0": "delta",
    "4-1": "long"
  },
  "cols": 5,
  "rows": 5
}
[/block]
##Response example
[block:code]
{
  "codes": [
    {
      "code": "{\n  \"affinityNodeId\": \"e05839d5-6648-43e7-a23b-78d7db9390d5\",\n  \"error\": \"\",\n  \"response\": 42,\n  \"successStatus\": 0\n}",
      "language": "json"
    }
  ]
}
[/block]

[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "description",
    "h-3": "example",
    "0-0": "response",
    "0-1": "long",
    "0-2": "Value after operation.",
    "0-3": "42"
  },
  "cols": 4,
  "rows": 1
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Cache metrics"
}
[/block]
**Cache metrics** command shows metrics for Ignite cache.
[block:code]
{
  "codes": [
    {
      "code": "http://host:port/ignite?cmd=cache&cacheName=partionedCache&destId=8daab5ea-af83-4d91-99b6-77ed2ca06647",
      "language": "curl"
    }
  ]
}
[/block]
##Request parameters
[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "optional",
    "h-3": "decription",
    "h-4": "example",
    "0-0": "cmd",
    "0-4": "",
    "0-1": "string",
    "0-3": "Should be **cache** lowercase.",
    "1-0": "cacheName",
    "1-1": "string",
    "1-2": "Yes",
    "1-3": "Cache name. If not provided, default cache will be used.",
    "1-4": "partionedCache",
    "2-0": "destId",
    "2-1": "string",
    "2-3": "Node ID for which the metrics are to be returned.",
    "2-4": "8daab5ea-af83-4d91-99b6-77ed2ca06647",
    "2-2": "Yes"
  },
  "cols": 5,
  "rows": 3
}
[/block]
##Response example
[block:code]
{
  "codes": [
    {
      "code": "{\n  \"affinityNodeId\": \"\",\n  \"error\": \"\",\n  \"response\": {\n    \"createTime\": 1415179251551,\n    \"hits\": 0,\n    \"misses\": 0,\n    \"readTime\": 1415179251551,\n    \"reads\": 0,\n    \"writeTime\": 1415179252198,\n    \"writes\": 2\n  },\n  \"successStatus\": 0\n}",
      "language": "json"
    }
  ]
}
[/block]

[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "description",
    "h-3": "example",
    "0-0": "response",
    "0-1": "jsonObject",
    "0-2": "The JSON object contains cache metrics such as create time, count reads and etc.",
    "0-3": "{\n\"createTime\": 1415179251551, \"hits\": 0, \"misses\": 0, \"readTime\":1415179251551, \"reads\": 0,\"writeTime\": 1415179252198, \"writes\": 2\n}"
  },
  "cols": 4,
  "rows": 1
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Compare-And-Swap"
}
[/block]
**CAS** command stores given key-value pair in cache only if the previous value is equal to the expected value passed in.
[block:code]
{
  "codes": [
    {
      "code": "http://host:port/ignite?cmd=cas&key=casKey&val2=casOldVal&val1=casNewVal&cacheName=partionedCache&destId=8daab5ea-af83-4d91-99b6-77ed2ca06647",
      "language": "curl"
    }
  ]
}
[/block]
##Request parameters
[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "optional",
    "h-3": "decription",
    "h-4": "example",
    "0-0": "cmd",
    "0-4": "",
    "0-1": "string",
    "0-3": "Should be **cas** lowercase.",
    "1-0": "cacheName",
    "1-1": "string",
    "1-2": "Yes",
    "1-3": "Cache name. If not provided, default cache will be used.",
    "1-4": "partionedCache",
    "5-0": "destId",
    "5-1": "string",
    "5-3": "Node ID for which the metrics are to be returned.",
    "5-4": "8daab5ea-af83-4d91-99b6-77ed2ca06647",
    "5-2": "Yes",
    "2-0": "key",
    "3-0": "val",
    "4-0": "val2",
    "2-1": "string",
    "3-1": "string",
    "4-1": "string",
    "2-2": "Key to store in cache.",
    "3-2": "Value associated with the given key.",
    "4-2": "Expected value.",
    "2-3": "name",
    "3-3": "Jack",
    "4-3": "Bob"
  },
  "cols": 5,
  "rows": 6
}
[/block]
##Response example
[block:code]
{
  "codes": [
    {
      "code": "{\n  \"affinityNodeId\": \"1bcbac4b-3517-43ee-98d0-874b103ecf30\",\n  \"error\": \"\",\n  \"response\": true,\n  \"successStatus\": 0\n}",
      "language": "json"
    }
  ]
}
[/block]

[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "description",
    "h-3": "example",
    "0-0": "response",
    "0-1": "boolean",
    "0-2": "True if replace happened, false otherwise.",
    "0-3": "true"
  },
  "cols": 4,
  "rows": 1
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Prepend"
}
[/block]
**Prepend** command prepends a line for value which is associated with key.
[block:code]
{
  "codes": [
    {
      "code": "http://host:port/ignite?cmd=prepend&key=prependKey&val=prefix_&cacheName=partionedCache&destId=8daab5ea-af83-4d91-99b6-77ed2ca06647",
      "language": "curl"
    }
  ]
}
[/block]
##Request parameters
[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "optional",
    "h-3": "decription",
    "h-4": "example",
    "0-0": "cmd",
    "0-4": "",
    "0-1": "string",
    "0-3": "Should be **prepend** lowercase.",
    "1-0": "cacheName",
    "1-1": "string",
    "1-2": "Yes",
    "1-3": "Cache name. If not provided, default cache will be used.",
    "1-4": "partionedCache",
    "4-0": "destId",
    "4-1": "string",
    "4-3": "Node ID for which the metrics are to be returned.",
    "4-4": "8daab5ea-af83-4d91-99b6-77ed2ca06647",
    "4-2": "Yes",
    "2-0": "key",
    "3-0": "val",
    "2-1": "string",
    "3-1": "string",
    "2-2": "Key to store in cache.",
    "3-2": "Value to be prepended to the current value.",
    "2-3": "name",
    "3-3": "Name_"
  },
  "cols": 5,
  "rows": 5
}
[/block]
##Response example
[block:code]
{
  "codes": [
    {
      "code": "{\n  \"affinityNodeId\": \"1bcbac4b-3517-43ee-98d0-874b103ecf30\",\n  \"error\": \"\",\n  \"response\": true,\n  \"successStatus\": 0\n}",
      "language": "json"
    }
  ]
}
[/block]

[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "description",
    "h-3": "example",
    "0-0": "response",
    "0-1": "boolean",
    "0-2": "True if replace happened, false otherwise.",
    "0-3": "true"
  },
  "cols": 4,
  "rows": 1
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Append"
}
[/block]
**Append** command appends a line for value which is associated with key.
[block:code]
{
  "codes": [
    {
      "code": "http://host:port/ignite?cmd=append&key=appendKey&val=_suffix&cacheName=partionedCache&destId=8daab5ea-af83-4d91-99b6-77ed2ca06647",
      "language": "curl"
    }
  ]
}
[/block]
##Request parameters
[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "optional",
    "h-3": "decription",
    "h-4": "example",
    "0-0": "cmd",
    "0-4": "",
    "0-1": "string",
    "0-3": "Should be **append** lowercase.",
    "1-0": "cacheName",
    "1-1": "string",
    "1-2": "Yes",
    "1-3": "Cache name. If not provided, default cache will be used.",
    "1-4": "partionedCache",
    "4-0": "destId",
    "4-1": "string",
    "4-3": "Node ID for which the metrics are to be returned.",
    "4-4": "8daab5ea-af83-4d91-99b6-77ed2ca06647",
    "4-2": "Yes",
    "2-0": "key",
    "3-0": "val",
    "2-1": "string",
    "3-1": "string",
    "2-2": "Key to store in cache.",
    "3-2": "Value to be appended to the current value.",
    "2-3": "name",
    "3-3": "Jack"
  },
  "cols": 5,
  "rows": 5
}
[/block]
##Response example
[block:code]
{
  "codes": [
    {
      "code": "{\n  \"affinityNodeId\": \"1bcbac4b-3517-43ee-98d0-874b103ecf30\",\n  \"error\": \"\",\n  \"response\": true,\n  \"successStatus\": 0\n}",
      "language": "json"
    }
  ]
}
[/block]

[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "description",
    "h-3": "example",
    "0-0": "response",
    "0-1": "boolean",
    "0-2": "True if replace happened, false otherwise.",
    "0-3": "true"
  },
  "cols": 4,
  "rows": 1
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Replace"
}
[/block]
**Replace** command stores a given key-value pair in cache only if there is a previous mapping for it.
[block:code]
{
  "codes": [
    {
      "code": "http://host:port/ignite?cmd=rep&key=repKey&val=newValue&cacheName=partionedCache&destId=8daab5ea-af83-4d91-99b6-77ed2ca06647",
      "language": "curl"
    }
  ]
}
[/block]
##Request parameters
[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "optional",
    "h-3": "decription",
    "h-4": "example",
    "0-0": "cmd",
    "0-4": "",
    "0-1": "string",
    "0-3": "Should be **rmvall** lowercase.",
    "1-0": "cacheName",
    "1-1": "string",
    "1-2": "Yes",
    "1-3": "Cache name. If not provided, default cache will be used.",
    "1-4": "partionedCache",
    "4-0": "destId",
    "4-1": "string",
    "4-3": "Node ID for which the metrics are to be returned.",
    "4-4": "8daab5ea-af83-4d91-99b6-77ed2ca06647",
    "4-2": "Yes",
    "2-0": "key",
    "3-0": "val",
    "2-1": "string",
    "3-1": "string",
    "2-2": "Key to store in cache.",
    "3-2": "Value associated with the given key.",
    "2-3": "name",
    "3-3": "Jack"
  },
  "cols": 5,
  "rows": 5
}
[/block]
##Response example
[block:code]
{
  "codes": [
    {
      "code": "{\n  \"affinityNodeId\": \"1bcbac4b-3517-43ee-98d0-874b103ecf30\",\n  \"error\": \"\",\n  \"response\": true,\n  \"successStatus\": 0\n}",
      "language": "json"
    }
  ]
}
[/block]

[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "description",
    "h-3": "example",
    "0-0": "response",
    "0-1": "boolean",
    "0-2": "True if replace happened, false otherwise.",
    "0-3": "true"
  },
  "cols": 4,
  "rows": 1
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Remove all"
}
[/block]
**Remove all** command removes given key mappings from cache.
[block:code]
{
  "codes": [
    {
      "code": "http://host:port/ignite?cmd=rmvall&k1=rmKey1&k2=rmKey2&k3=rmKey3&cacheName=partionedCache&destId=8daab5ea-af83-4d91-99b6-77ed2ca06647",
      "language": "curl"
    }
  ]
}
[/block]
##Request parameters
[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "optional",
    "h-3": "decription",
    "h-4": "example",
    "0-0": "cmd",
    "0-4": "",
    "0-1": "string",
    "0-3": "Should be **rmvall** lowercase.",
    "1-0": "cacheName",
    "1-1": "string",
    "1-2": "Yes",
    "1-3": "Cache name. If not provided, default cache will be used.",
    "1-4": "partionedCache",
    "3-0": "destId",
    "3-1": "string",
    "3-3": "Node ID for which the metrics are to be returned.",
    "3-4": "8daab5ea-af83-4d91-99b6-77ed2ca06647",
    "3-2": "Yes",
    "2-0": "k1...kN",
    "2-1": "string",
    "2-2": "Keys whose mappings are to be removed from cache.",
    "2-3": "name"
  },
  "cols": 5,
  "rows": 4
}
[/block]
##Response example
[block:code]
{
  "codes": [
    {
      "code": "{\n  \"affinityNodeId\": \"1bcbac4b-3517-43ee-98d0-874b103ecf30\",\n  \"error\": \"\",\n  \"response\": true,\n  \"successStatus\": 0\n}",
      "language": "json"
    }
  ]
}
[/block]

[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "description",
    "h-3": "example",
    "0-0": "response",
    "0-1": "boolean",
    "0-2": "True if replace happened, false otherwise.",
    "0-3": "true"
  },
  "cols": 4,
  "rows": 1
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Remove"
}
[/block]
**Remove** command removes the given key mapping from cache.
[block:code]
{
  "codes": [
    {
      "code": "http://host:port/ignite?cmd=rmv&key=rmvKey&cacheName=partionedCache&destId=8daab5ea-af83-4d91-99b6-77ed2ca06647",
      "language": "curl"
    }
  ]
}
[/block]
##Request parameters
[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "optional",
    "h-3": "decription",
    "h-4": "example",
    "0-0": "cmd",
    "0-4": "",
    "0-1": "string",
    "0-3": "Should be **rmv** lowercase.",
    "1-0": "cacheName",
    "1-1": "string",
    "1-2": "Yes",
    "1-3": "Cache name. If not provided, default cache will be used.",
    "1-4": "partionedCache",
    "3-0": "destId",
    "3-1": "string",
    "3-3": "Node ID for which the metrics are to be returned.",
    "3-4": "8daab5ea-af83-4d91-99b6-77ed2ca06647",
    "3-2": "Yes",
    "2-0": "key",
    "2-1": "string",
    "2-2": "Key - for which the mapping is to be removed from cache.",
    "2-3": "name"
  },
  "cols": 5,
  "rows": 4
}
[/block]
##Response example
[block:code]
{
  "codes": [
    {
      "code": "{\n  \"affinityNodeId\": \"1bcbac4b-3517-43ee-98d0-874b103ecf30\",\n  \"error\": \"\",\n  \"response\": true,\n  \"successStatus\": 0\n}",
      "language": "json"
    }
  ]
}
[/block]

[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "description",
    "h-3": "example",
    "0-0": "response",
    "0-1": "boolean",
    "0-2": "True if replace happened, false otherwise.",
    "0-3": "true"
  },
  "cols": 4,
  "rows": 1
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Add"
}
[/block]
**Add** command stores a given key-value pair in cache only if there isn't a previous mapping for it.
[block:code]
{
  "codes": [
    {
      "code": "http://host:port/ignite?cmd=add&key=newKey&val=newValue&cacheName=partionedCache&destId=8daab5ea-af83-4d91-99b6-77ed2ca06647",
      "language": "curl"
    }
  ]
}
[/block]
##Request parameters
[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "optional",
    "h-3": "decription",
    "h-4": "example",
    "0-0": "cmd",
    "0-4": "",
    "0-1": "string",
    "0-3": "Should be **add** lowercase.",
    "1-0": "cacheName",
    "1-1": "string",
    "1-2": "Yes",
    "1-3": "Cache name. If not provided, default cache will be used.",
    "1-4": "partionedCache",
    "4-0": "destId",
    "4-1": "string",
    "4-3": "Node ID for which the metrics are to be returned.",
    "4-4": "8daab5ea-af83-4d91-99b6-77ed2ca06647",
    "4-2": "Yes",
    "2-0": "key",
    "2-1": "string",
    "2-3": "Key to be associated with the value.",
    "2-4": "name",
    "3-0": "val",
    "3-1": "string",
    "3-3": "Value to be associated with the key.",
    "3-4": "Jack"
  },
  "cols": 5,
  "rows": 5
}
[/block]
##Response example
[block:code]
{
  "codes": [
    {
      "code": "{\n  \"affinityNodeId\": \"1bcbac4b-3517-43ee-98d0-874b103ecf30\",\n  \"error\": \"\",\n  \"response\": true,\n  \"successStatus\": 0\n}",
      "language": "json"
    }
  ]
}
[/block]

[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "description",
    "h-3": "example",
    "0-0": "response",
    "0-1": "boolean",
    "0-2": "True if value was stored in cache, false otherwise.",
    "0-3": "true"
  },
  "cols": 4,
  "rows": 1
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Put all"
}
[/block]
**Put all** command stores the given key-value pairs in cache.
[block:code]
{
  "codes": [
    {
      "code": "http://host:port/ignite?cmd=putall&k1=putKey1&k2=putKey2&k3=putKey3&v1=value1&v2=value2&v3=value3&cacheName=partionedCache&destId=8daab5ea-af83-4d91-99b6-77ed2ca06647",
      "language": "curl"
    }
  ]
}
[/block]
##Request parameters
[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "optional",
    "h-3": "decription",
    "h-4": "example",
    "0-0": "cmd",
    "0-4": "",
    "0-1": "string",
    "0-3": "Should be **putall** lowercase.",
    "1-0": "cacheName",
    "1-1": "string",
    "1-2": "Yes",
    "1-3": "Cache name. If not provided, default cache will be used.",
    "1-4": "partionedCache",
    "4-0": "destId",
    "4-1": "string",
    "4-3": "Node ID for which the metrics are to be returned.",
    "4-4": "8daab5ea-af83-4d91-99b6-77ed2ca06647",
    "4-2": "Yes",
    "2-0": "k1...kN",
    "2-1": "string",
    "2-3": "Keys to be associated with values.",
    "2-4": "name",
    "3-0": "v1...vN",
    "3-1": "string",
    "3-3": "Values to be associated with keys.",
    "3-4": "Jack"
  },
  "cols": 5,
  "rows": 5
}
[/block]
##Response example
[block:code]
{
  "codes": [
    {
      "code": "{\n  \"affinityNodeId\": \"1bcbac4b-3517-43ee-98d0-874b103ecf30\",\n  \"error\": \"\",\n  \"response\": true,\n  \"successStatus\": 0\n}",
      "language": "json"
    }
  ]
}
[/block]

[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "description",
    "h-3": "example",
    "0-0": "response",
    "0-1": "boolean",
    "0-2": "True if values was stored in cache, false otherwise.",
    "0-3": "true"
  },
  "cols": 4,
  "rows": 1
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Put"
}
[/block]
**Put** command stores the given key-value pair in cache.
[block:code]
{
  "codes": [
    {
      "code": "http://host:port/ignite?cmd=put&key=newKey&val=newValue&cacheName=partionedCache&destId=8daab5ea-af83-4d91-99b6-77ed2ca06647",
      "language": "curl"
    }
  ]
}
[/block]
##Request parameters
[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "optional",
    "h-3": "decription",
    "h-4": "example",
    "0-0": "cmd",
    "0-4": "",
    "0-1": "string",
    "0-3": "Should be **put** lowercase.",
    "1-0": "cacheName",
    "1-1": "string",
    "1-2": "Yes",
    "1-3": "Cache name. If not provided, default cache will be used.",
    "1-4": "partionedCache",
    "4-0": "destId",
    "4-1": "string",
    "4-3": "Node ID for which the metrics are to be returned.",
    "4-4": "8daab5ea-af83-4d91-99b6-77ed2ca06647",
    "4-2": "Yes",
    "2-0": "key",
    "2-1": "string",
    "2-3": "Key to be associated with values.",
    "2-4": "name",
    "3-0": "val",
    "3-1": "string",
    "3-3": "Value to be associated with keys.",
    "3-4": "Jack"
  },
  "cols": 5,
  "rows": 5
}
[/block]
##Response example
[block:code]
{
  "codes": [
    {
      "code": "{\n  \"affinityNodeId\": \"1bcbac4b-3517-43ee-98d0-874b103ecf30\",\n  \"error\": \"\",\n  \"response\": true,\n  \"successStatus\": 0\n}",
      "language": "json"
    }
  ]
}
[/block]

[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "description",
    "h-3": "example",
    "0-0": "response",
    "0-1": "boolean",
    "0-2": "True if value was stored in cache, false otherwise.",
    "0-3": "true"
  },
  "cols": 4,
  "rows": 1
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Get all"
}
[/block]
**Get all** command retrieves values mapped to the specified keys from cache.
[block:code]
{
  "codes": [
    {
      "code": "http://host:port/ignite?cmd=getall&k1=getKey1&k2=getKey2&k3=getKey3&cacheName=partionedCache&destId=8daab5ea-af83-4d91-99b6-77ed2ca06647",
      "language": "curl"
    }
  ]
}
[/block]
##Request parameters
[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "optional",
    "h-3": "decription",
    "h-4": "example",
    "0-0": "cmd",
    "0-4": "",
    "0-1": "string",
    "0-3": "Should be **getall** lowercase.",
    "1-0": "cacheName",
    "1-1": "string",
    "1-2": "Yes",
    "1-3": "Cache name. If not provided, default cache will be used.",
    "1-4": "partionedCache",
    "3-0": "destId",
    "3-1": "string",
    "3-3": "Node ID for which the metrics are to be returned.",
    "3-4": "8daab5ea-af83-4d91-99b6-77ed2ca06647",
    "3-2": "Yes",
    "2-0": "k1...kN",
    "2-1": "string",
    "2-3": "Keys whose associated values are to be returned.",
    "2-4": "key1, key2, ..., keyN"
  },
  "cols": 5,
  "rows": 4
}
[/block]
##Response example
[block:code]
{
  "codes": [
    {
      "code": "{\n  \"affinityNodeId\": \"\",\n  \"error\": \"\",\n  \"response\": {\n    \"key1\": \"value1\",\n    \"key2\": \"value2\"\n  },\n  \"successStatus\": 0\n}",
      "language": "json"
    }
  ]
}
[/block]

[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "description",
    "h-3": "example",
    "0-0": "response",
    "0-1": "jsonObject",
    "0-2": "The map of key-value pairs.",
    "0-3": "{\"key1\": \"value1\",\"key2\": \"value2\"}"
  },
  "cols": 4,
  "rows": 1
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Get"
}
[/block]
**Get** command retrieves value mapped to the specified key from cache.
[block:code]
{
  "codes": [
    {
      "code": "http://host:port/ignite?cmd=get&key=getKey&cacheName=partionedCache&destId=8daab5ea-af83-4d91-99b6-77ed2ca06647",
      "language": "curl"
    }
  ]
}
[/block]
##Request parameters
[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "optional",
    "h-3": "decription",
    "h-4": "example",
    "0-0": "cmd",
    "0-4": "",
    "0-1": "string",
    "0-3": "Should be **get** lowercase.",
    "1-0": "cacheName",
    "1-1": "string",
    "1-2": "Yes",
    "1-3": "Cache name. If not provided, default cache will be used.",
    "1-4": "partionedCache",
    "3-0": "destId",
    "3-1": "string",
    "3-3": "Node ID for which the metrics are to be returned.",
    "3-4": "8daab5ea-af83-4d91-99b6-77ed2ca06647",
    "3-2": "Yes",
    "2-0": "key",
    "2-1": "string",
    "2-3": "Key whose associated value is to be returned",
    "2-4": "testKey"
  },
  "cols": 5,
  "rows": 4
}
[/block]
##Response example
[block:code]
{
  "codes": [
    {
      "code": "{\n  \"affinityNodeId\": \"2bd7b049-3fa0-4c44-9a6d-b5c7a597ce37\",\n  \"error\": \"\",\n  \"response\": \"value\",\n  \"successStatus\": 0\n}",
      "language": "json"
    }
  ]
}
[/block]

[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "description",
    "h-3": "example",
    "0-0": "response",
    "0-1": "jsonObject",
    "0-2": "Value for the given key.",
    "0-3": "{\"name\": \"bob\"}"
  },
  "cols": 4,
  "rows": 1
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Node"
}
[/block]
**Node** command gets information about a node.
[block:code]
{
  "codes": [
    {
      "code": "http://host:port/ignite?cmd=node&attr=true&mtr=true&id=c981d2a1-878b-4c67-96f6-70f93a4cd241",
      "language": "curl"
    }
  ]
}
[/block]
##Request parameters
[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "optional",
    "h-3": "decription",
    "h-4": "example",
    "0-0": "cmd",
    "0-4": "",
    "0-1": "string",
    "0-3": "Should be **node** lowercase.",
    "1-0": "mtr",
    "1-1": "boolean",
    "1-2": "Yes",
    "1-3": "Response will include metrics, if this parameter has value true.",
    "1-4": "true",
    "4-0": "id",
    "4-1": "string",
    "4-3": "This parameter is optional, if ip parameter is passed. Response will be returned for node which has the node ID.",
    "4-4": "8daab5ea-af83-4d91-99b6-77ed2ca06647",
    "4-2": "",
    "2-0": "attr",
    "2-1": "boolean",
    "2-3": "Response will include attributes, if this parameter has value true.",
    "2-4": "true",
    "3-0": "ip",
    "3-1": "string",
    "3-3": "This parameter is optional, if id parameter is passed. Response will be returned for node which has the IP.",
    "3-4": "192.168.0.1",
    "2-2": "Yes"
  },
  "cols": 5,
  "rows": 5
}
[/block]
##Response example
[block:code]
{
  "codes": [
    {
      "code": "{\n  \"error\": \"\",\n  \"response\": {\n    \"attributes\": null,\n    \"caches\": {},\n    \"consistentId\": \"127.0.0.1:47500\",\n    \"defaultCacheMode\": \"REPLICATED\",\n    \"metrics\": null,\n    \"nodeId\": \"2d0d6510-6fed-4fa3-b813-20f83ac4a1a9\",\n    \"replicaCount\": 128,\n    \"tcpAddresses\": [\"127.0.0.1\"],\n    \"tcpHostNames\": [\"\"],\n    \"tcpPort\": 11211\n  },\n  \"successStatus\": 0\n}",
      "language": "json"
    }
  ]
}
[/block]

[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "description",
    "h-3": "example",
    "0-0": "response",
    "0-1": "jsonObject",
    "0-2": "Information about only one node.",
    "0-3": "{\n\"attributes\": null,\n\"caches\": {},\n\"consistentId\": \"127.0.0.1:47500\",\n\"defaultCacheMode\": \"REPLICATED\",\n\"metrics\": null,\n\"nodeId\": \"2d0d6510-6fed-4fa3-b813-20f83ac4a1a9\",\n\"replicaCount\": 128,\n\"tcpAddresses\": [\"127.0.0.1\"],\n\"tcpHostNames\": [\"\"],\n\"tcpPort\": 11211\n}"
  },
  "cols": 4,
  "rows": 1
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Topology"
}
[/block]
**Topology** command gets information about grid topology.
[block:code]
{
  "codes": [
    {
      "code": "http://host:port/ignite?cmd=top&attr=true&mtr=true&id=c981d2a1-878b-4c67-96f6-70f93a4cd241",
      "language": "curl"
    }
  ]
}
[/block]
##Request parameters
[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "optional",
    "h-3": "decription",
    "h-4": "example",
    "0-0": "cmd",
    "0-4": "",
    "0-1": "string",
    "0-3": "Should be **top** lowercase.",
    "1-0": "mtr",
    "1-1": "boolean",
    "1-2": "Yes",
    "1-3": "Response will include metrics, if this parameter has value true.",
    "1-4": "true",
    "4-0": "id",
    "4-1": "string",
    "4-3": "This parameter is optional, if ip parameter is passed. Response will be returned for node which has the node ID.",
    "4-4": "8daab5ea-af83-4d91-99b6-77ed2ca06647",
    "4-2": "Yes",
    "2-0": "attr",
    "2-1": "boolean",
    "2-3": "Response will include attributes, if this parameter has value true.",
    "2-4": "true",
    "3-0": "ip",
    "3-1": "string",
    "3-3": "This parameter is optional, if id parameter is passed. Response will be returned for node which has the IP.",
    "3-4": "192.168.0.1",
    "2-2": "Yes",
    "3-2": "Yes"
  },
  "cols": 5,
  "rows": 5
}
[/block]
##Response example
[block:code]
{
  "codes": [
    {
      "code": "{\n  \"error\": \"\",\n  \"response\": [\n    {\n      \"attributes\": {\n        ...\n      },\n      \"caches\": {},\n      \"consistentId\": \"127.0.0.1:47500\",\n      \"defaultCacheMode\": \"REPLICATED\",\n      \"metrics\": {\n        ...\n      },\n      \"nodeId\": \"96baebd6-dedc-4a68-84fd-f804ee1ed995\",\n      \"replicaCount\": 128,\n      \"tcpAddresses\": [\"127.0.0.1\"],\n      \"tcpHostNames\": [\"\"],\n      \"tcpPort\": 11211\n   },\n   {\n     \"attributes\": {\n       ...\n     },\n     \"caches\": {},\n      \"consistentId\": \"127.0.0.1:47501\",\n      \"defaultCacheMode\": \"REPLICATED\",\n      \"metrics\": {\n        ...\n      },\n      \"nodeId\": \"2bd7b049-3fa0-4c44-9a6d-b5c7a597ce37\",\n      \"replicaCount\": 128,\n      \"tcpAddresses\": [\"127.0.0.1\"],\n      \"tcpHostNames\": [\"\"],\n      \"tcpPort\": 11212\n   }\n  ],\n  \"successStatus\": 0\n}",
      "language": "json"
    }
  ]
}
[/block]

[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "description",
    "h-3": "example",
    "0-0": "response",
    "0-1": "jsonObject",
    "0-2": "Information about grid topology.",
    "0-3": "[\n{\n\"attributes\": {\n...\n},\n\"caches\": {},\n\"consistentId\": \"127.0.0.1:47500\",\n\"defaultCacheMode\": \"REPLICATED\",\n\"metrics\": {\n...\n},\n\"nodeId\": \"96baebd6-dedc-4a68-84fd-f804ee1ed995\",\n...\n\"tcpPort\": 11211\n},\n{\n\"attributes\": {\n...\n},\n\"caches\": {},\n\"consistentId\": \"127.0.0.1:47501\",\n\"defaultCacheMode\": \"REPLICATED\",\n\"metrics\": {\n...\n},\n\"nodeId\": \"2bd7b049-3fa0-4c44-9a6d-b5c7a597ce37\",\n...\n\"tcpPort\": 11212\n}\n]"
  },
  "cols": 4,
  "rows": 1
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Execute"
}
[/block]
**Execute** command executes given task on grid.
[block:code]
{
  "codes": [
    {
      "code": "http://host:port/ignite?cmd=exe&name=taskName&p1=param1&p2=param2&async=true",
      "language": "curl"
    }
  ]
}
[/block]
##Request parameters
[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "optional",
    "h-3": "decription",
    "h-4": "example",
    "0-0": "cmd",
    "0-4": "",
    "0-1": "string",
    "0-3": "Should be **exe** lowercase.",
    "1-0": "name",
    "1-1": "string",
    "1-2": "",
    "1-3": "Name of the task to execute.",
    "1-4": "summ",
    "2-0": "p1...pN",
    "2-1": "string",
    "2-3": "Argument of task execution.",
    "2-4": "arg1...argN",
    "3-0": "async",
    "3-1": "boolean",
    "3-3": "The flag determines whether the task is performed asynchronously.",
    "3-4": "true",
    "2-2": "Yes",
    "3-2": "Yes"
  },
  "cols": 5,
  "rows": 4
}
[/block]
##Response example
[block:code]
{
  "codes": [
    {
      "code": "{\n  \"error\": \"\",\n  \"response\": {\n    \"error\": \"\",\n    \"finished\": true,\n    \"id\": \"~ee2d1688-2605-4613-8a57-6615a8cbcd1b\",\n    \"result\": 4\n  },\n  \"successStatus\": 0\n}",
      "language": "json"
    }
  ]
}
[/block]

[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "description",
    "h-3": "example",
    "0-0": "response",
    "0-1": "jsonObject",
    "0-2": "JSON object contains message about error, unique identifier of task, result of computation and status of computation.",
    "0-3": "{\n\"error\": \"\",\n\"finished\": true,\n\"id\": \"~ee2d1688-2605-4613-8a57-6615a8cbcd1b\",\n\"result\": 4\n}"
  },
  "cols": 4,
  "rows": 1
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Result"
}
[/block]
**Result** command returns computation result for the given task.
[block:code]
{
  "codes": [
    {
      "code": "http://host:port/ignite?cmd=res&id=8daab5ea-af83-4d91-99b6-77ed2ca06647",
      "language": "curl"
    }
  ]
}
[/block]
##Request parameters
[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "optional",
    "h-3": "decription",
    "h-4": "example",
    "0-0": "cmd",
    "0-4": "",
    "0-1": "string",
    "0-3": "Should be **res** lowercase.",
    "1-0": "id",
    "1-1": "string",
    "1-2": "",
    "1-3": "ID of the task whose result is to be returned.",
    "1-4": "69ad0c48941-4689aae0-6b0e-4d52-8758-ce8fe26f497d~4689aae0-6b0e-4d52-8758-ce8fe26f497d"
  },
  "cols": 5,
  "rows": 2
}
[/block]
##Response example
[block:code]
{
  "codes": [
    {
      "code": "{\n  \"error\": \"\",\n  \"response\": {\n    \"error\": \"\",\n    \"finished\": true,\n    \"id\": \"69ad0c48941-4689aae0-6b0e-4d52-8758-ce8fe26f497d~4689aae0-6b0e-4d52-8758-ce8fe26f497d\",\n    \"result\": 4\n  },\n  \"successStatus\": 0\n}",
      "language": "json"
    }
  ]
}
[/block]

[block:parameters]
{
  "data": {
    "h-0": "name",
    "h-1": "type",
    "h-2": "description",
    "h-3": "example",
    "0-0": "response",
    "0-1": "jsonObject",
    "0-2": "JSON object contains message about error, ID of task, result of computation and status of computation.",
    "0-3": "{\n    \"error\": \"\",\n    \"finished\": true,\n    \"id\": \"~ee2d1688-2605-4613-8a57-6615a8cbcd1b\",\n    \"result\": 4\n}"
  },
  "cols": 4,
  "rows": 1
}
[/block]