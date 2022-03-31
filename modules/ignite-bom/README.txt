Apache Ignite BOM (Bill of Materials) Module
---------------------------------------

The Apache Ignite BOM (Bill of Materials) provides a special POM file that groups dependency versions that are known
to be valid and tested to work together. This will reduce the developers pain of having to test the compatibility of
different versions and reduce the chances to have version mismatches.

In the example below, note that the ignite-core and ignite-indexing dependencies doesn’t need a version number.
Maven will resolve it from the list of dependencies in the BOM file.

Using Ignite BOM In Maven Project
---------------------------------------

<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>org.apache.ignite</groupId>
            <artifactId>ignite-bom</artifactId>
            <version>${ignite.version}</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>

<dependencies>
    <dependency>
        <groupId>org.apache.ignite</groupId>
        <artifactId>ignite-core</artifactId>
    </dependency>
    <dependency>
        <groupId>org.apache.ignite</groupId>
        <artifactId>ignite-indexing</artifactId>
    </dependency>
</dependencies>
