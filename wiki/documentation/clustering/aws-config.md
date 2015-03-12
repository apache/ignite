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

Node discovery on AWS cloud is usually proven to be more challenging. Amazon EC2, just like most of the other virtual environments, has the following limitations:
* Multicast is disabled.
* TCP addresses change every time a new image is started.

Although you can use TCP-based discovery in the absence of the Multicast, you still have to deal with constantly changing IP addresses and constantly updating the configuration. This creates a major inconvenience and makes configurations based on static IPs virtually unusable in such environments.
[block:api-header]
{
  "type": "basic",
  "title": "Amazon S3 Based Discovery"
}
[/block]
To mitigate constantly changing IP addresses problem, Ignite supports automatic node discovery by utilizing S3 store via `TcpDiscoveryS3IpFinder`. On startup nodes register their IP addresses with Amazon S3 store. This way other nodes can try to connect to any of the IP addresses stored in S3 and initiate automatic grid node discovery.
[block:callout]
{
  "type": "success",
  "body": "Such approach allows to create your configuration once and reuse it for all EC2 instances."
}
[/block]


Here is an example of how to configure Amazon S3 IP finder:
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.configuration.IgniteConfiguration\">\n  ...\n  <property name=\"discoverySpi\">\n    <bean class=\"org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi\">\n      <property name=\"ipFinder\">\n        <bean class=\"org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinder\">\n          <property name=\"awsCredentials\" ref=\"aws.creds\"/>\n          <property name=\"bucketName\" value=\"YOUR_BUCKET_NAME\"/>\n        </bean>\n      </property>\n    </bean>\n  </property>\n</bean>\n\n<!-- AWS credentials. Provide your access key ID and secret access key. -->\n<bean id=\"aws.creds\" class=\"com.amazonaws.auth.BasicAWSCredentials\">\n  <constructor-arg value=\"YOUR_ACCESS_KEY_ID\" />\n  <constructor-arg value=\"YOUR_SECRET_ACCESS_KEY\" />\n</bean>",
      "language": "xml"
    },
    {
      "code": "TcpDiscoverySpi spi = new TcpDiscoverySpi();\n\nBasicAWSCredentials creds = new BasicAWSCredentials(\"yourAccessKey\", \"yourSecreteKey\");\n\nTcpDiscoveryS3IpFinder ipFinder = new TcpDiscoveryS3IpFinder();\n\nipFinder.setAwsCredentials(creds);\n\nspi.setIpFinder(ipFinder);\n\nIgniteConfiguration cfg = new IgniteConfiguration();\n \n// Override default discovery SPI.\ncfg.setDiscoverySpi(spi);\n \n// Start Ignite node.\nIgnition.start(cfg);",
      "language": "java"
    }
  ]
}
[/block]

[block:callout]
{
  "type": "success",
  "body": "Refer to [Cluster Configuration](doc:cluster-config) for more information on various cluster configuration properties."
}
[/block]