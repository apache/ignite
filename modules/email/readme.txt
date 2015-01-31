Apache Ignite Email Module
--------------------------

Apache Ignite email module enables Apache Ignite to send emails in critical situations such as license
expiration or fatal system errors (this should be also configured via 'GridConfiguration.setSmtpXXX(..)'
configuration properties).

To enable email module when starting a standalone node, move 'optional/ignite-email' folder to
'libs' folder before running 'ignite.{sh|bat}' script. The content of the module folder will
be added to classpath in this case.

Importing email Module In Maven Project
---------------------------------------

If you are using Maven to manage dependencies of your project, you can add email module
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
            <artifactId>ignite-email</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
