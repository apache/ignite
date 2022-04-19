Apache Ignite Direct IO Module
------------------------------

Apache Ignite Direct IO is plugin, which provides page store with ability to write and read cache partitions
in O_DIRECT mode.


OS gets the data and stores it in a file buffer cache (Page Cache).

<!-- To edit picture please use
https://docs.google.com/drawings/d/19xXbaWC2F2EBcd7F0T9wJ7wZO3DDR0PFR0tBqqIXjUI/edit?usp=sharing
-->
<img src="https://docs.google.com/drawings/d/e/2PACX-1vQBR0OoKFeQ1AOMyDK9QoQEBLDs4kbs7EY6Ed48HnRjlM0J1Ao3g_glD7AR3KZRtUcAVL6hQut6IPVw/pub?w=638&amp;h=499">

Similarly, for every write operation,
the OS first writes the data in a cache and then transfers to the disk. To eliminate this process you can enable
Direct I/O in which case the data is read and written directly from/to the disk bypassing the file buffer cache.

Direct I/O plugin in Ignite is used for the checkpointing process where the dirty pages in RAM are written to the disk.

Importing Direct I/O Pluging In Maven Project
-------------------------------------

If you are using Maven to manage dependencies of your project, you can add Direct IO Module
dependency like this (replace '${ignite.version}' with actual Ignite version you are
interested in):

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                        http://maven.apache.org/xsd/maven-4.0.0.xsd">
    ...
    <dependencies>
        ...
        <dependency>
            <groupId>org.apache.ignite</groupId>
            <artifactId>ignite-direct-io</artifactId>
            <version>${ignite.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
```

Importing Direct I/O Pluging In Gradle Project
-------------------------------------
For gradle you can add compile dependency, where igniteVersion is actual Ignite version:

```groovy
compile group: 'org.apache.ignite', name: 'ignite-direct-io', version: igniteVersion
```

Additional setup is not required. Once plugin is available in classpath, it will be used for Durable Memory IO.

-------------------------------------
See more information in Apache Ignite documentation:
[How to enable Direct IO](https://apacheignite.readme.io/docs/durable-memory-tuning#section-enabling-direct-i-o)

and description of internal desing can be found in Wiki:
[Under the hood: Direct IO](https://cwiki.apache.org/confluence/display/IGNITE/Ignite+Persistent+Store+-+under+the+hood#IgnitePersistentStore-underthehood-DirectI/O)
