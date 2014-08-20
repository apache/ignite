GridGain Email Module
---------------------

GridGain email module enables GridGain to send emails in critical situations such as license
expiration or fatal system errors (this should be also configured via 'GridConfiguration.setSmtpXXX(..)'
configuration properties).

To enable email module when starting a standalone node, move 'optional/gridgain-email' folder to
'libs' folder before running 'ggstart.{sh|bat}' script. The content of the module folder will
be added to classpath in this case.

Importing email Module In Maven Project
---------------------------------------

If you are using Maven to manage dependencies of your project, you can add email module
dependency like this (replace '${gridgain.version}' with actual GridGain version you are
interested in):

<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                        http://maven.apache.org/xsd/maven-4.0.0.xsd">
    ...
    <dependencies>
        ...
        <dependency>
            <groupId>org.gridgain</groupId>
            <artifactId>gridgain-email</artifactId>
            <version>${gridgain.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
