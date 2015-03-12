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

In Ignite, nodes can discover each other by using `DiscoverySpi`. Ignite provides `TcpDiscoverySpi` as a default implementation of `DiscoverySpi` that uses TCP/IP for node discovery. Discovery SPI can be configured for Multicast and Static IP based node discovery.
[block:api-header]
{
  "type": "basic",
  "title": "Multicast Based Discovery"
}
[/block]
`TcpDiscoveryMulticastIpFinder` uses Multicast to discover other nodes in the grid and is the default IP finder. You should not have to specify it unless you plan to override default settings. Here is an example of how to configure this finder via Spring XML file or programmatically from Java:
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.configuration.IgniteConfiguration\">\n  ...\n  <property name=\"discoverySpi\">\n    <bean class=\"org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi\">\n      <property name=\"ipFinder\">\n        <bean class=\"org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder\">\n          <property name=\"multicastGroup\" value=\"228.10.10.157\"/>\n        </bean>\n      </property>\n    </bean>\n  </property>\n</bean>",
      "language": "xml"
    },
    {
      "code": "TcpDiscoverySpi spi = new TcpDiscoverySpi();\n \nTcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();\n \nipFinder.setMulticastGroup(\"228.10.10.157\");\n \nspi.setIpFinder(ipFinder);\n \nIgniteConfiguration cfg = new IgniteConfiguration();\n \n// Override default discovery SPI.\ncfg.setDiscoverySpi(spi);\n \n// Start Ignite node.\nIgnition.start(cfg);",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Static IP Based Discovery"
}
[/block]
For cases when Multicast is disabled, `TcpDiscoveryVmIpFinder` should be used with pre-configured list of IP addresses. You are only required to provide at least one IP address, but usually it is advisable to provide 2 or 3 addresses of the grid nodes that you plan to start first for redundancy. Once a connection to any of the provided IP addresses is established, Ignite will automatically discover all other grid nodes.
[block:callout]
{
  "type": "success",
  "body": "You do not need to specify IP addresses for all Ignite nodes, only for a couple of nodes you plan to start first."
}
[/block]

Here is an example of how to configure this finder via Spring XML file or programmatically from Java:
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.configuration.IgniteConfiguration\">\n  ...\n  <property name=\"discoverySpi\">\n    <bean class=\"org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi\">\n      <property name=\"ipFinder\">\n        <bean class=\"org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder\">\n          <property name=\"addresses\">\n            <list>\n              <value>1.2.3.4</value>\n              \n              <!-- \n                  IP Address and optional port range.\n                  You can also optionally specify an individual port.\n              -->\n              <value>1.2.3.5:47500..47509</value>\n            </list>\n          </property>\n        </bean>\n      </property>\n    </bean>\n  </property>\n</bean>",
      "language": "xml"
    },
    {
      "code": "TcpDiscoverySpi spi = new TcpDiscoverySpi();\n \nTcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();\n \n// Set initial IP addresses.\n// Note that you can optionally specify a port or a port range.\nipFinder.setAddresses(Arrays.asList(\"1.2.3.4\", \"1.2.3.5:47500..47509\"));\n \nspi.setIpFinder(ipFinder);\n \nIgniteConfiguration cfg = new IgniteConfiguration();\n \n// Override default discovery SPI.\ncfg.setDiscoverySpi(spi);\n \n// Start Ignite node.\nIgnition.start(cfg);",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Multicast and Static IP Based Discovery"
}
[/block]
You can use both, Multicast and Static IP based discovery together. In this case, in addition to addresses received via multicast, if any, `TcpDiscoveryMulticastIpFinder` can also work with pre-configured list of static IP addresses, just like Static IP-Based Discovery described above. Here is an example of how to configure Multicast IP finder with static IP addresses:
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.configuration.IgniteConfiguration\">\n  ...\n  <property name=\"discoverySpi\">\n    <bean class=\"org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi\">\n      <property name=\"ipFinder\">\n        <bean class=\"org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder\">\n          <property name=\"multicastGroup\" value=\"228.10.10.157\"/>\n           \n          <!-- list of static IP addresses-->\n          <property name=\"addresses\">\n            <list>\n              <value>1.2.3.4</value>\n              \n              <!-- \n                  IP Address and optional port range.\n                  You can also optionally specify an individual port.\n              -->\n              <value>1.2.3.5:47500..47509</value>\n            </list>\n          </property>\n        </bean>\n      </property>\n    </bean>\n  </property>\n</bean>",
      "language": "xml"
    },
    {
      "code": "TcpDiscoverySpi spi = new TcpDiscoverySpi();\n \nTcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();\n \n// Set Multicast group.\nipFinder.setMulticastGroup(\"228.10.10.157\");\n\n// Set initial IP addresses.\n// Note that you can optionally specify a port or a port range.\nipFinder.setAddresses(Arrays.asList(\"1.2.3.4\", \"1.2.3.5:47500..47509\"));\n \nspi.setIpFinder(ipFinder);\n \nIgniteConfiguration cfg = new IgniteConfiguration();\n \n// Override default discovery SPI.\ncfg.setDiscoverySpi(spi);\n \n// Start Ignite node.\nIgnition.start(cfg);",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Amazon S3 Based Discovery"
}
[/block]
Refer to [AWS Configuration](doc:aws-config) documentation.
[block:api-header]
{
  "type": "basic",
  "title": "JDBC Based Discovery"
}
[/block]
You can have your database be a common shared storage of initial IP addresses. In this nodes will write their IP addresses to a database on startup. This is done via `TcpDiscoveryJdbcIpFinder`.
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.configuration.IgniteConfiguration\">\n  ...\n  <property name=\"discoverySpi\">\n    <bean class=\"org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi\">\n      <property name=\"ipFinder\">\n        <bean class=\"org.apache.ignite.spi.discovery.tcp.ipfinder.jdbc.TcpDiscoveryJdbcIpFinder\">\n          <property name=\"dataSource\" ref=\"ds\"/>\n        </bean>\n      </property>\n    </bean>\n  </property>\n</bean>\n\n<!-- Configured data source instance. -->\n<bean id=\"ds\" class=\"some.Datasource\">\n  ...\n</bean>",
      "language": "xml"
    },
    {
      "code": "TcpDiscoverySpi spi = new TcpDiscoverySpi();\n\n// Configure your DataSource.\nDataSource someDs = MySampleDataSource(...);\n\nTcpDiscoveryJdbcIpFinder ipFinder = new TcpDiscoveryJdbcIpFinder();\n\nipFinder.setDataSource(someDs);\n\nspi.setIpFinder(ipFinder);\n\nIgniteConfiguration cfg = new IgniteConfiguration();\n \n// Override default discovery SPI.\ncfg.setDiscoverySpi(spi);\n \n// Start Ignite node.\nIgnition.start(cfg);",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Configuration"
}
[/block]
Following configuration parameters can be optionally configured on `TcpDiscoverySpi`.
[block:parameters]
{
  "data": {
    "0-0": "`setIpFinder(TcpDiscoveryIpFinder)`",
    "0-1": "IP finder that is used to share info about nodes IP addresses.",
    "0-2": "`TcpDiscoveryMulticastIpFinder`\n\nProvided implementations can be used:\n`TcpDiscoverySharedFsIpFinder`\n`TcpDiscoveryS3IpFinder`\n`TcpDiscoveryJdbcIpFinder`\n`TcpDiscoveryVmIpFinder`",
    "h-0": "Setter Method",
    "h-1": "Description",
    "h-2": "Default",
    "h-3": "Default",
    "0-3": "",
    "1-0": "`setLocalAddress(String)`",
    "1-1": "Sets local host IP address that discovery SPI uses.",
    "1-3": "",
    "1-2": "If not provided, by default a first found non-loopback address will be used. If there is no non-loopback address available, then `java.net.InetAddress.getLocalHost()` will be used.",
    "2-0": "`setLocalPort(int)`",
    "2-1": "Port the SPI listens to.",
    "2-2": "47500",
    "2-3": "",
    "3-0": "`setLocalPortRange(int)`",
    "3-1": "Local port range. \nLocal node will try to bind on first available port starting from local port up until local port + local port range.",
    "3-2": "100",
    "3-3": "100",
    "4-0": "`setHeartbeatFrequency(long)`",
    "4-1": "Delay in milliseconds between heartbeat issuing of heartbeat messages. \nSPI sends messages in configurable time interval to other nodes to notify them about its state.",
    "4-3": "2000",
    "4-2": "2000",
    "5-0": "`setMaxMissedHeartbeats(int)`",
    "5-1": "Number of heartbeat requests that could be missed before local node initiates status check.",
    "5-3": "1",
    "5-2": "1",
    "6-0": "`setReconnectCount(int)`",
    "6-1": "Number of times node tries to (re)establish connection to another node.",
    "6-3": "2",
    "6-2": "2",
    "7-0": "`setNetworkTimeout(long)`",
    "7-1": "Sets maximum network timeout in milliseconds to use for network operations.",
    "7-2": "5000",
    "7-3": "5000",
    "8-0": "`setSocketTimeout(long)`",
    "8-1": "Sets socket operations timeout. This timeout is used to limit connection time and write-to-socket time.",
    "8-2": "2000",
    "8-3": "2000",
    "9-0": "`setAckTimeout(long)`",
    "9-1": "Sets timeout for receiving acknowledgement for sent message. \nIf acknowledgement is not received within this timeout, sending is considered as failed and SPI tries to repeat message sending.",
    "9-2": "2000",
    "9-3": "2000",
    "10-0": "`setJoinTimeout(long)`",
    "10-1": "Sets join timeout. If non-shared IP finder is used and node fails to connect to any address from IP finder, node keeps trying to join within this timeout. If all addresses are still unresponsive, exception is thrown and node startup fails. \n0 means wait forever.",
    "10-2": "0",
    "10-3": "0",
    "11-0": "`setThreadPriority(int)`",
    "11-1": "Thread priority for threads started by SPI.",
    "11-2": "0",
    "11-3": "0",
    "12-0": "`setStatisticsPrintFrequency(int)`",
    "12-1": "Statistics print frequency in milliseconds. \n0 indicates that no print is required. If value is greater than 0 and log is not quiet then stats are printed out with INFO level once a period. This may be very helpful for tracing topology problems.",
    "12-2": "true",
    "12-3": "true",
    "13-0": ""
  },
  "cols": 3,
  "rows": 13
}
[/block]