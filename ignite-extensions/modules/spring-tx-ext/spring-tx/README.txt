Apache Ignite Spring Transactions Module
---------------------------

Apache Ignite Spring Transactions extension provides an integration with Spring Transactions framework.

To get started with Apache Ignite Spring Transactions, create instance of Apache Ignite Spring Transactions Manager as bean in your Spring application.

There are two implementations of Apache Ignite Spring Transactions Manager - org.apache.ignite.transactions.spring.SpringTransactionManager and org.apache.ignite.transactions.spring.IgniteClientSpringTransactionManager, that provide ability to use the Ignite thick or thin client to connect to the Ignite cluster and manage Ignite transactions, respectively.

Importing Spring Transactions extension In Maven Project
----------------------------------------

If you are using Maven to manage dependencies of your project, you can add Spring Transactions extension dependency like this (replace '${ignite-spring-tx-ext.version}', '${spring.version}' and '${ignite.version}'  with actual version of Ignite Spring Transactions extension, Spring Transactions and Ignite you are interested in, respectively):

<!-- Please note that for Ignite versions earlier than 2.11, the ignite-spring-tx-ext dependency must be added to classpath before ignite-spring, due to duplication of Spring Transactions integration classes. If you are using Maven to manage dependencies, it just needs to place ignite-spring-tx-ext before ignite-spring dependency in your pom file. --!>

<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                        http://maven.apache.org/xsd/maven-4.0.0.xsd">
    ...
    <dependencies>
        ...

        <dependency>
            <groupId>org.apache.ignite</groupId>
            <artifactId>ignite-spring-tx-ext</artifactId>
            <version>${ignite-spring-tx-ext.version}</version>
        </dependency>

         <dependency>
            <groupId>org.apache.ignite</groupId>
            <artifactId>ignite-spring</artifactId>
            <version>${ignite.version}</version>
        </dependency>

        <dependency>
            <groupId>org.apache.ignite</groupId>
            <artifactId>ignite-core</artifactId>
            <version>${ignite.version}</version>
        </dependency>

        <dependency>
            <groupId>org.springframework</groupId>
            <artifactId>spring-tx</artifactId>
            <version>${spring.version}</version>
        </dependency>

        ...
    </dependencies>
    ...
</project>
