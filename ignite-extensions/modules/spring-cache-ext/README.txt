Apache Ignite Spring Cache Module
---------------------------

Apache Ignite Spring Cache extension provides an integration with Spring Cache framework.

There are two implementations of Apache Ignite Spring Cache Manager - org.apache.ignite.cache.spring.SpringCacheManager and
org.apache.ignite.cache.spring.IgniteClientSpringCacheManager, that provide ability to use the Ignite thick or thin client to
connect to the Ignite cluster and manage Ignite caches, respectively. Note, that org.apache.ignite.cache.spring.IgniteClientSpringCacheManager
can be used only with Ignite since 2.11.0 version.

Importing Spring Cache extension In Maven Project
----------------------------------------

If you are using Maven to manage dependencies of your project, you can add Spring Cache extension dependency like this (replace '${ignite-spring-cache-ext.version}' and '${ignite.version}' with actual version of Ignite Spring Cache extension and Ignite you are interested in, respectively):

 <!-- Please note that for Ignite versions earlier than 2.11, the ignite-spring-cache-ext dependency must be added to classpath before ignite-spring, due to duplication of Spring Cache integration classes. If you are using Maven to manage dependencies, it just needs to place ignite-spring-cache-ext before ignite-spring dependency in your pom file. --!>

<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                        http://maven.apache.org/xsd/maven-4.0.0.xsd">
    ...
    <dependencies>
        ...
        <dependency>
            <groupId>org.apache.ignite</groupId>
            <artifactId>ignite-spring-cache-ext</artifactId>
            <version>${ignite-spring-cache-ext.version}</version>
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
        ...
    </dependencies>
    ...
</project>
