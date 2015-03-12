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
  "title": "General Configuration"
}
[/block]

[block:parameters]
{
  "data": {
    "h-0": "Parameter name",
    "h-3": "Default value",
    "h-2": "Optional",
    "h-1": "Description",
    "0-0": "**setSecretKey(String)**",
    "0-1": "Defines secret key used for client authentication. When provided, client request must contain HTTP header **X-Signature** with Base64 encoded SHA1 hash of the string \"[1];[2]\", where [1] is timestamp in milliseconds and [2] is the secret key.",
    "0-3": "**null**",
    "0-2": "Yes",
    "1-0": "**setPortRange(int)**",
    "1-1": "Port range for Jetty server. In case port provided in Jetty configuration or **IGNITE_JETTY_PORT** system property is already in use, Ignite will iteratively increment port by 1 and try binding once again until provided port range is exceeded.",
    "1-3": "**100**",
    "1-2": "Yes",
    "2-0": "**setJettyPath(String)**",
    "2-1": "Path to Jetty configuration file. Should be either absolute or relative to **IGNITE_HOME**. If not provided then GridGain will start Jetty server with simple HTTP connector. This connector will use **IGNITE_JETTY_HOST** and **IGNITE_JETTY_PORT** system properties as host and port respectively. In case **IGNITE_JETTY_HOST** is not provided, localhost will be used as default. In case **IGNITE_JETTY_PORT** is not provided, port 8080 will be used as default.",
    "2-3": "**null**",
    "2-2": "Yes"
  },
  "cols": 4,
  "rows": 3
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Sample Jetty XML configuration"
}
[/block]

[block:code]
{
  "codes": [
    {
      "code": "<?xml version=\"1.0\"?>\n<!DOCTYPE Configure PUBLIC \"-//Jetty//Configure//EN\" \"http://www.eclipse.org/jetty/configure.dtd\">\n<Configure id=\"Server\" class=\"org.eclipse.jetty.server.Server\">\n    <Arg name=\"threadPool\">\n        <!-- Default queued blocking thread pool -->\n        <New class=\"org.eclipse.jetty.util.thread.QueuedThreadPool\">\n            <Set name=\"minThreads\">20</Set>\n            <Set name=\"maxThreads\">200</Set>\n        </New>\n    </Arg>\n    <New id=\"httpCfg\" class=\"org.eclipse.jetty.server.HttpConfiguration\">\n        <Set name=\"secureScheme\">https</Set>\n        <Set name=\"securePort\">8443</Set>\n        <Set name=\"sendServerVersion\">true</Set>\n        <Set name=\"sendDateHeader\">true</Set>\n    </New>\n    <Call name=\"addConnector\">\n        <Arg>\n            <New class=\"org.eclipse.jetty.server.ServerConnector\">\n                <Arg name=\"server\"><Ref refid=\"Server\"/></Arg>\n                <Arg name=\"factories\">\n                    <Array type=\"org.eclipse.jetty.server.ConnectionFactory\">\n                        <Item>\n                            <New class=\"org.eclipse.jetty.server.HttpConnectionFactory\">\n                                <Ref refid=\"httpCfg\"/>\n                            </New>\n                        </Item>\n                    </Array>\n                </Arg>\n                <Set name=\"host\">\n                  <SystemProperty name=\"IGNITE_JETTY_HOST\" default=\"localhost\"/>\n              \t</Set>\n                <Set name=\"port\">\n                  <SystemProperty name=\"IGNITE_JETTY_PORT\" default=\"8080\"/>\n              \t</Set>\n                <Set name=\"idleTimeout\">30000</Set>\n                <Set name=\"reuseAddress\">true</Set>\n            </New>\n        </Arg>\n    </Call>\n    <Set name=\"handler\">\n        <New id=\"Handlers\" class=\"org.eclipse.jetty.server.handler.HandlerCollection\">\n            <Set name=\"handlers\">\n                <Array type=\"org.eclipse.jetty.server.Handler\">\n                    <Item>\n                        <New id=\"Contexts\" class=\"org.eclipse.jetty.server.handler.ContextHandlerCollection\"/>\n                    </Item>\n                </Array>\n            </Set>\n        </New>\n    </Set>\n    <Set name=\"stopAtShutdown\">false</Set>\n</Configure>",
      "language": "xml",
      "name": ""
    }
  ]
}
[/block]