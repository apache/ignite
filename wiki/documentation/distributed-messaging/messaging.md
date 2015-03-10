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

Ignite distributed messaging allows for topic based cluster-wide communication between all nodes. Messages with a specified message topic can be distributed to all or sub-group of nodes that have subscribed to that topic. 

Ignite messaging is based on publish-subscribe paradigm where publishers and subscribers  are connected together by a common topic. When one of the nodes sends a message A for topic T, it is published on all nodes that have subscribed to T.
[block:callout]
{
  "type": "info",
  "body": "Any new node joining the cluster automatically gets subscribed to all the topics that other nodes in the cluster (or [cluster group](/docs/cluster-groups)) are subscribed to."
}
[/block]
Distributed Messaging functionality in Ignite is provided via `IgniteMessaging` interface. You can get an instance of `IgniteMessaging`, like so:
[block:code]
{
  "codes": [
    {
      "code": "Ignite ignite = Ignition.ignite();\n\n// Messaging instance over this cluster.\nIgniteMessaging msg = ignite.message();\n\n// Messaging instance over given cluster group (in this case, remote nodes).\nIgniteMessaging rmtMsg = ignite.message(ignite.cluster().forRemotes());",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Publish Messages"
}
[/block]
Send methods help sending/publishing messages with a specified message topic to all nodes. Messages can be sent in *ordered* or *unordered* manner.
##Ordered Messages
`sendOrdered(...)` method can be used if you want to receive messages in the order they were sent. A timeout parameter is passed to specify how long a message will stay in the queue to wait for messages that are supposed to be sent before this message. If the timeout expires, then all the messages that have not yet arrived for a given topic on that node will be ignored.
##Unordered Messages
`send(...)` methods do not guarantee message ordering. This means that, when you sequentially send message A and message B, you are not guaranteed that the target node first receives A and then B.
[block:api-header]
{
  "type": "basic",
  "title": "Subscribe for Messages"
}
[/block]
Listen methods help to listen/subscribe for messages. When these methods are called, a listener with specified message topic is registered on  all (or sub-group of ) nodes to listen for new messages. With listen methods, a predicate is passed that returns a boolean value which tells the listener to continue or stop listening for new messages. 
##Local Listen
`localListen(...)` method registers a message listener with specified topic only on the local node and listens for messages from any node in *this* cluster group.
##Remote Listen
`remoteListen(...)` method registers message listeners with specified topic on all nodes in *this* cluster group and listens for messages from any node in *this* cluster group . 


[block:api-header]
{
  "type": "basic",
  "title": "Example"
}
[/block]
Following example shows message exchange between remote nodes.
[block:code]
{
  "codes": [
    {
      "code": "Ignite ignite = Ignition.ignite();\n \nIgniteMessaging rmtMsg = ignite.message(ignite.cluster().forRemotes());\n \n// Add listener for unordered messages on all remote nodes.\nrmtMsg.remoteListen(\"MyOrderedTopic\", (nodeId, msg) -> {\n    System.out.println(\"Received ordered message [msg=\" + msg + \", from=\" + nodeId + ']');\n \n    return true; // Return true to continue listening.\n});\n \n// Send ordered messages to remote nodes.\nfor (int i = 0; i < 10; i++)\n    rmtMsg.sendOrdered(\"MyOrderedTopic\", Integer.toString(i));",
      "language": "java",
      "name": "Ordered Messaging"
    },
    {
      "code": " Ignite ignite = Ignition.ignite();\n \nIgniteMessaging rmtMsg = ignite.message(ignite.cluster().forRemotes());\n \n// Add listener for unordered messages on all remote nodes.\nrmtMsg.remoteListen(\"MyUnOrderedTopic\", (nodeId, msg) -> {\n    System.out.println(\"Received unordered message [msg=\" + msg + \", from=\" + nodeId + ']');\n \n    return true; // Return true to continue listening.\n});\n \n// Send unordered messages to remote nodes.\nfor (int i = 0; i < 10; i++)\n    rmtMsg.send(\"MyUnOrderedTopic\", Integer.toString(i));",
      "language": "java",
      "name": "Unordered Messaging"
    },
    {
      "code": "Ignite ignite = Ignition.ignite();\n\n// Get cluster group of remote nodes.\nClusterGroup rmtPrj = ignite.cluster().forRemotes();\n\n// Get messaging instance over remote nodes.\nIgniteMessaging msg = ignite.message(rmtPrj);\n\n// Add message listener for specified topic on all remote nodes.\nmsg.remoteListen(\"myOrderedTopic\", new IgniteBiPredicate<UUID, String>() {\n    @Override public boolean apply(UUID nodeId, String msg) {\n        System.out.println(\"Received ordered message [msg=\" + msg + \", from=\" + nodeId + ']');\n\n        return true; // Return true to continue listening.\n    }\n});\n\n// Send ordered messages to all remote nodes.\nfor (int i = 0; i < 10; i++)\n    msg.sendOrdered(\"myOrderedTopic\", Integer.toString(i), 0);",
      "language": "java",
      "name": "java7 ordered"
    }
  ]
}
[/block]