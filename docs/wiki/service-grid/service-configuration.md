In addition to deploying managed services by calling any of the provided `IgniteServices.deploy(...)` methods, you can also automatically deploy services on startup by setting `serviceConfiguration` property of IgniteConfiguration:
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.IgniteConfiguration\">\n    ...  \n    <!-- Distributed Service configuration. -->\n    <property name=\"serviceConfiguration\">\n        <list>\n            <bean class=\"org.apache.ignite.services.ServiceConfiguration\">\n                <property name=\"name\" value=\"MyClusterSingletonSvc\"/>\n                <property name=\"maxPerNodeCount\" value=\"1\"/>\n                <property name=\"totalCount\" value=\"1\"/>\n                <property name=\"service\">\n                  <ref bean=\"myServiceImpl\"/>\n                </property>\n            </bean>\n        </list>\n    </property>\n</bean>\n \n<bean id=\"myServiceImpl\" class=\"foo.bar.MyServiceImpl\">\n  ...\n</bean>",
      "language": "xml"
    },
    {
      "code": "ServiceConfiguration svcCfg1 = new ServiceConfiguration();\n \n// Cluster-wide singleton configuration.\nsvcCfg1.setName(\"MyClusterSingletonSvc\");\nsvcCfg1.setMaxPerNodeCount(1);\nsvcCfg1.setTotalCount(1);\nsvcCfg1.setService(new MyClusterSingletonImpl());\n \nServiceConfiguration svcCfg2 = new ServiceConfiguration();\n \n// Per-node singleton configuration.\nsvcCfg2.setName(\"MyNodeSingletonSvc\");\nsvcCfg2.setMaxPerNodeCount(1);\nsvcCfg2.setService(new MyNodeSingletonImpl());\n\nIgniteConfiguration igniteCfg = new IgniteConfiguration();\n \nigniteCfg.setServiceConfiguration(svcCfg1, svcCfg2);\n...\n\n// Start Ignite node.\nIgnition.start(gridCfg);",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Deploying After Startup"
}
[/block]
You can configure and deploy services after the startup of Ignite nodes. Besides multiple convenience methods that allow deployment of various [cluster singletons](doc:cluster-singletons), you can also create and deploy service with custom configuration.
[block:code]
{
  "codes": [
    {
      "code": "ServiceConfiguration cfg = new ServiceConfiguration();\n \ncfg.setName(\"myService\");\ncfg.setService(new MyService());\n\n// Maximum of 4 service instances within cluster.\ncfg.setTotalCount(4);\n\n// Maximum of 2 service instances per each Ignite node.\ncfg.setMaxPerNodeCount(2);\n \nignite.services().deploy(cfg);",
      "language": "java"
    }
  ]
}
[/block]