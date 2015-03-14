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

`CommunicationSpi` provides basic plumbing to send and receive grid messages and is utilized for all distributed grid operations, such as task execution, monitoring data exchange, distributed event querying and others. Ignite provides `TcpCommunicationSpi` as the default implementation of `CommunicationSpi`, that uses the TCP/IP to communicate with other nodes. 

To enable communication with other nodes, `TcpCommunicationSpi` adds `TcpCommuncationSpi.ATTR_ADDRS` and `TcpCommuncationSpi.ATTR_PORT` local node attributes. At startup, this SPI tries to start listening to local port specified by `TcpCommuncationSpi.setLocalPort(int)` method. If local port is occupied, then SPI will automatically increment the port number until it can successfully bind for listening. `TcpCommuncationSpi.setLocalPortRange(int)` configuration parameter controls maximum number of ports that SPI will try before it fails. 
[block:callout]
{
  "type": "info",
  "body": "Port range comes very handy when starting multiple grid nodes on the same machine or even in the same VM. In this case all nodes can be brought up without a single change in configuration.",
  "title": "Local Port Range"
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Configuration"
}
[/block]
Following configuration parameters can be optionally configured on `TcpCommuncationSpi`:
[block:parameters]
{
  "data": {
    "h-0": "Setter Method",
    "h-1": "Description",
    "h-2": "Default",
    "0-0": "`setLocalAddress(String)\t`",
    "0-1": "Sets local host address for socket binding.",
    "0-2": "Any available local IP address.",
    "1-0": "`setLocalPort(int)`",
    "2-0": "`setLocalPortRange(int)`",
    "3-0": "`setTcpNoDelay(boolean)`",
    "4-0": "`setConnectTimeout(long)`",
    "5-0": "`setIdleConnectionTimeout(long)`",
    "6-0": "`setBufferSizeRatio(double)`",
    "7-0": "`setMinimumBufferedMessageCount(int)`",
    "8-0": "`setDualSocketConnection(boolean)`",
    "9-0": "`setSpiPortResolver(GridSpiPortResolver)`",
    "10-0": "`setConnectionBufferSize(int)`",
    "11-0": "`setSelectorsCount(int)`",
    "12-0": "`setConnectionBufferFlushFrequency(long)`",
    "13-0": "`setDirectBuffer(boolean)`",
    "14-0": "`setDirectSendBuffer(boolean)`",
    "15-0": "`setAsyncSend(boolean)`",
    "16-0": "`setSharedMemoryPort(int)`",
    "17-0": "`setSocketReceiveBuffer(int)`",
    "18-0": "`setSocketSendBuffer(int)`",
    "1-1": "Sets local port for socket binding.",
    "1-2": "47100",
    "2-1": "Controls maximum number of local ports tried if all previously tried ports are occupied.",
    "2-2": "100",
    "3-1": "Sets value for `TCP_NODELAY` socket option. Each socket accepted or created will be using provided value.\nThis should be set to true (default) for reducing request/response time during communication over TCP protocol. In most cases we do not recommend to change this option.",
    "3-2": "true",
    "4-1": "Sets connect timeout used when establishing connection with remote nodes.",
    "4-2": "1000",
    "5-1": "Sets maximum idle connection timeout upon which a connection to client will be closed.",
    "5-2": "30000",
    "6-1": "Sets the buffer size ratio for this SPI. As messages are sent, the buffer size is adjusted using this ratio.",
    "6-2": "0.8 or `IGNITE_COMMUNICATION_BUF_RESIZE_RATIO` system property value, if set.",
    "7-1": "Sets the minimum number of messages for this SPI, that are buffered prior to sending.",
    "7-2": "512 or `IGNITE_MIN_BUFFERED_COMMUNICATION_MSG_CNT` system property value, if set.",
    "8-1": "Sets flag indicating whether dual-socket connection between nodes should be enforced. If set to true, two separate connections will be established between communicating nodes: one for outgoing messages, and one for incoming. When set to false, single TCP connection will be used for both directions.\nThis flag is useful on some operating systems, when TCP_NODELAY flag is disabled and messages take too long to get delivered.",
    "8-2": "false",
    "9-1": "Sets port resolver for internal-to-external port mapping. In some cases network routers are configured to perform port mapping between external and internal networks and the same mapping must be available to SPIs in GridGain that perform communication over IP protocols.",
    "9-2": "null",
    "10-1": "This parameter is used only when `setAsyncSend(boolean)` is set to false. \n\nSets connection buffer size for synchronous connections. Increase buffer size if using synchronous send and sending large amount of small sized messages. However, most of the time this should be set to 0 (default).",
    "10-2": "0",
    "11-1": "Sets the count of selectors to be used in TCP server.",
    "11-2": "Default count of selectors equals to the expression result - \nMath.min(4, Runtime.getRuntime() .availableProcessors())",
    "12-1": "This parameter is used only when `setAsyncSend(boolean)` is set to false. \n\nSets connection buffer flush frequency in milliseconds. This parameter makes sense only for synchronous send when connection buffer size is not 0. Buffer will be flushed once within specified period if there is no enough messages to make it flush automatically.",
    "12-2": "100",
    "13-1": "Switches between using NIO direct and NIO heap allocation buffers. Although direct buffers perform better, in some cases (especially on Windows) they may cause JVM crashes. If that happens in your environment, set this property to false.",
    "13-2": "true",
    "14-1": "Switches between using NIO direct and NIO heap allocation buffers usage for message sending in asynchronous mode.",
    "14-2": "false",
    "15-1": "Switches between synchronous and asynchronous message sending.\nThis should be set to true (default) if grid nodes send large amount of data over network from multiple threads, however this maybe environment and application specific and we recommend to benchmark the application in both modes.",
    "15-2": "true",
    "16-1": "Sets port which will be used by `IpcSharedMemoryServerEndpoint`. \nNodes started on the same host will communicate over IPC shared memory (only for Linux and MacOS hosts). Set this to -1 to disable IPC shared memory communication.",
    "16-2": "48100",
    "17-1": "Sets receive buffer size for sockets created or accepted by this SPI. If not provided, default is 0 which leaves buffer unchanged after socket creation (i.e. uses Operating System default value).",
    "17-2": "0",
    "18-1": "Sets send buffer size for sockets created or accepted by this SPI. If not provided, default is 0 which leaves the buffer unchanged after socket creation (i.e. uses Operating System default value).",
    "18-2": "0"
  },
  "cols": 3,
  "rows": 19
}
[/block]
##Example 
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.configuration.IgniteConfiguration\">\n  ...\n  <property name=\"communicationSpi\">\n    <bean class=\"org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi\">\n      <!-- Override local port. -->\n      <property name=\"localPort\" value=\"4321\"/>\n    </bean>\n  </property>\n  ...\n</bean>",
      "language": "xml"
    },
    {
      "code": "TcpCommunicationSpi commSpi = new TcpCommunicationSpi();\n \n// Override local port.\ncommSpi.setLocalPort(4321);\n \nIgniteConfiguration cfg = new IgniteConfiguration();\n \n// Override default communication SPI.\ncfg.setCommunicationSpi(commSpi);\n \n// Start grid.\nIgnition.start(cfg);",
      "language": "java"
    }
  ]
}
[/block]