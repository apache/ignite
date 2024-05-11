Apache Ignite Client Spring Boot Autoconfigure Module
----------------------------------------

Apache Ignite Client Spring Boot Autoconfigure module provides autoconfiguration capabilities for Spring-boot based applications.

Features
---------------------------
This module provides the ability to integrate `IgniteClient` into you spring-boot application with zero(or minimal) configuration.

After you add this module as a dependency to your spring-boot application
`IgniteClient` will be configured and injected into `BeanFactory`.

Algorithm to configure `Ignite` is the following:
  1. If `ClientConfiguration` bean exists in the `BeanFactory` it will be used.
  2. If `ClientConfiguration` bean doesn't exist following rules are applied:
    2.1. Default `ClientConfiguration` configuration created.
    2.2. If `IgniteClientConfigurer` bean exists in `BeanFactory` it will be used to customize `ClientConfiguration`.
    2.3  Application properties applied to `ClientConfiguration`. Prefix for the properties is `ignite-client`.

Importing Spring Boot Autoconfigure Module In Maven Project
----------------------------------------

If you are using Maven to manage dependencies of your project, you can add Spring module
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
            <artifactId>spring-boot-ignite-client-autoconfigure-ext</artifactId>
            <version>${spring-boot-ignite-client-autoconfigure-ext.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>

