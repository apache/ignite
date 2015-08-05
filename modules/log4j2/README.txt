Apache Ignite Log4J2 Module
--------------------------

Apache Ignite Log4J2 module provides IgniteLogger implementation based on Apache Log4J2.

To enable Log4J2 module when starting a standalone node, move 'optional/ignite-log4j2' folder to
'libs' folder before running 'ignite.{sh|bat}' script. The content of the module folder will
be added to classpath in this case. Also it is needed to configure ignite logger with Log4J2Logger
and provide log4j2 configuration.

To enable ignite logging with default configuration use:

<bean class="org.apache.ignite.configuration.IgniteConfiguration">
    ...
    <property name="gridLogger">
        <bean class="org.apache.ignite.logger.log4j2.Log4J2Logger">
            <constructor-arg type="java.lang.String" value="config/ignite-log4j2.xml"/>
        </bean>
    </property>
    ...
</bean>

Importing Log4J2 Module In Maven Project
---------------------------------------

If you are using Maven to manage dependencies of your project, you can add Log4J2 module
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
            <artifactId>ignite-log4j2</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
