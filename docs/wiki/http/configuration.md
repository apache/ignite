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
    "0-0": "**setRestSecretKey(String)**",
    "0-1": "Defines secret key used for client authentication. When provided, client request must contain HTTP header **X-Signature** with Base64 encoded SHA1 hash of the string \"[1];[2]\", where [1] is timestamp in milliseconds and [2] is the secret key.",
    "0-3": "**null**",
    "0-2": "Yes",
    "1-0": "**setRestPortRange(int)**",
    "1-1": "Port range for Jetty server. In case port provided in Jetty configuration or **IGNITE_JETTY_PORT** system property is already in use, Ignite will iteratively increment port by 1 and try binding once again until provided port range is exceeded.",
    "1-3": "**100**",
    "1-2": "Yes",
    "3-0": "**setRestEnabled(Boolean)**",
    "3-1": "Whether REST client is enabled or not.",
    "3-3": "**true**",
    "3-2": "Yes",
    "4-0": "**setRestAccessibleFolders(String...)**",
    "4-1": "Array of folders that are accessible for log reading commands. When remote client requests a log file, file path is checked against this list. If requested file is not located in any sub-folder of these folders, request is not processed. By default array consists of a single **IGNITE_HOME** folder. If **IGNITE_HOME** could not be detected and property is not specified, no restrictions applied.",
    "4-2": "Yes",
    "4-3": "**IGNITE_HOME**",
    "2-0": "**setRestJettyPath(String)**",
    "2-1": "Path to Jetty configuration file. Should be either absolute or relative to **IGNITE_HOME**. If not provided then GridGain will start Jetty server with simple HTTP connector. This connector will use **IGNITE_JETTY_HOST** and **IGNITE_JETTY_PORT** system properties as host and port respectively. In case **IGNITE_JETTY_HOST** is not provided, localhost will be used as default. In case **IGNITE_JETTY_PORT** is not provided, port 8080 will be used as default.",
    "2-3": "**null**",
    "2-2": "Yes"
  },
  "cols": 4,
  "rows": 5
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