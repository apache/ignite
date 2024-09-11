
Apache Ignite REST-HTTP Module
------------------------------

Apache Ignite REST-HTTP module provides Jetty-based server which can be used to execute tasks and/or cache commands
in grid using REST approach via HTTP protocol.

To enable REST-HTTP module when starting a standalone node, move 'optional/ignite-rest-http' folder to
'libs' folder before running 'ignite.{sh|bat}' script. The content of the module folder will
be added to classpath in this case.

Importing REST-HTTP Module In Maven Project
-------------------------------------------

If you are using Maven to manage dependencies of your project, you can add REST-HTTP module
dependency like this (replace '${ignite.version}' with actual Ignite version you are
interested in):

<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                        http://maven.apache.org/xsd/maven-4.0.0.xsd">
    ...
    <dependencies>
        ...
        <dependency>
            <groupId>org.apache.ignite</groupId>
            <artifactId>ignite-rest-http</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>

ChangeLog
--------------------------------
- 通过在webapps下面部署应用，额外部署了一个es webapp，可以提供扩展服务
- 一个jvm只有一个jetty server，通过contextPath定位rest服务
- 如果没有配置jettyPath则启动默认端口的Rest服务器;
=======
Apache Ignite REST-HTTP Module
------------------------------

Apache Ignite REST-HTTP module provides Jetty-based server which can be used to execute tasks and/or cache commands
in grid using REST approach via HTTP protocol.

To enable REST-HTTP module when starting a standalone node, move 'optional/ignite-rest-http' and 'optional/ignite-json'
folders to 'libs' folder before running 'ignite.{sh|bat}' script. The content of the module folder will
be added to classpath in this case.

The module depends on third-party libraries that use the slf4j facade for logging.
You can set up an underlying logging framework yourself.

Importing REST-HTTP Module In Maven Project
-------------------------------------------

If you are using Maven to manage dependencies of your project, you can add REST-HTTP module
dependency like this (replace '${ignite.version}' with actual Ignite version you are
interested in):

<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                        http://maven.apache.org/xsd/maven-4.0.0.xsd">
    ...
    <dependencies>
        ...
        <dependency>
            <groupId>org.apache.ignite</groupId>
            <artifactId>ignite-rest-http</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>

