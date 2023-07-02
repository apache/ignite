Apache Ignite Spring Boot Autoconfigure Module
----------------------------------------

Apache Ignite Spring Boot Autoconfigure module provides autoconfiguration capabilities for Spring-boot based applications.

Features
---------------------------
This module provides the ability to integrate `Ignite` into you spring-boot application with zero(or minimal) configuration.

After you add this module as a dependency to your spring-boot application
`Ignite` node will be configured and injected into `BeanFactory`.

Algorithm to configure `Ignite` is the following:
  1. If `IgniteConfiguration` bean exists in the `BeanFactory` it will be used.
  2. If `IgniteConfiguration` bean doesn't exist following rules are applied:
    2.1. Default `Ignite` configuration created.
    2.2. If `IgniteConfigurer` bean exists in `BeanFactory` it will be used to customize `IgniteConfiguration`.
         If a user wants to set custom SPI instances or similar hardcoded values
         one should do it with `IgniteConfigurer` implementation.
    2.3  Application properties applied to `IgniteConfiguration`. Prefix for the properties is `ignite`.

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
            <artifactId>spring-boot-ignite-autoconfigure-ext</artifactId>
            <version>${spring-boot-ignite-autoconfigure-ext.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>

