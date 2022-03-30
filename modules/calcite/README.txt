Apache Ignite Calcite Module
--------------------------

Apache Ignite Calcite module provides experimental Apache Calcite based query engine.

To enable Calcite based engine explicit `CalciteQueryEngineConfiguration` instance should be added to
`SqlConfiguration.QueryEnginesConfiguration` property (see `SqlConfiguration.setQueryEnginesConfiguration()`) or ./examples/config/example-sql.xml.
Config example:

<bean id="queries.cfg" class="org.apache.ignite.configuration.IgniteConfiguration">
    <property name="clientConnectorConfiguration">
        <bean class="org.apache.ignite.configuration.ClientConnectorConfiguration">
            <property name="host" value="127.0.0.1"/>
            <property name="port" value="10800"/>
            <property name="portRange" value="10"/>
        </bean>
    </property>
    ...
    <property name="sqlConfiguration">
        <bean class="org.apache.ignite.configuration.SqlConfiguration">
            <property name="queryEnginesConfiguration">
                <list>
                    <bean class="org.apache.ignite.indexing.IndexingQueryEngineConfiguration">
                        <property name="default" value="true"/>
                    </bean>
                    <bean class="org.apache.ignite.calcite.CalciteQueryEngineConfiguration">
                        <property name="default" value="false"/>
                    </bean>
                </list>
            </property>
        </bean>
    </property>
    ...
</bean>


Also, Calcite module libraries should be in a classpath.

When starting a standalone node, move 'optional/ignite-calcite' folder to 'libs' folder before running
'ignite.{sh|bat}' script. The content of the module folder will be added to classpath in this case.

Note: At now some logic from ignite-indexing module is reused, this means ignite-indexing module also
has to be present at classpath.

If more than one engine configured throught "queryEnginesConfiguration" it`s possible to use exact engine:
1. JDBC:
  By passing "queryEngine" param.
  Connection url via JDBC with Calcite engine: jdbc:ignite:thin://127.0.0.1:10800?queryEngine=calcite
  and with H2 engine: jdbc:ignite:thin://127.0.0.1:10800?queryEngine=h2

2. ODBC, connection string samples:
  [IGNITE_H2]
  DRIVER=Apache Ignite
  SERVER=127.0.0.1
  PORT=11110
  SCHEMA=PUBLIC
  QUERY_ENGINE=H2

  [IGNITE_CALCITE]
  DRIVER=Apache Ignite
  SERVER=127.0.0.1
  PORT=11110
  SCHEMA=PUBLIC
  QUERY_ENGINE=CALCITE

  More extended info can be found here https://ignite.apache.org/docs/latest//SQL/ODBC/connection-string-dsn

3. From code or 3-rd party tools:
  By using hints:
    SELECT /*+ QUERY_ENGINE('h2') */ fld FROM table;
    or
    SELECT /*+ QUERY_ENGINE('calcite') */ fld FROM table;

Importing Calcite Module In Maven Project
---------------------------------------

If you are using Maven to manage dependencies of your project, you can add Calcite module
dependency like this (replace '${ignite.version}' with actual Apache Ignite version you are
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
            <artifactId>ignite-calcite</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
