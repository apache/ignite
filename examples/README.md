# Apache Ignite Examples

This module contains examples of how to run [Apache Ignite](ignite.apache.org) and [Apache Ignite](ignite.apache.org) with 3rd party components.

Instructions on how to start examples can be found in [README.txt](README.txt).

How to start examples in the developer's environment, please see [DEVNOTES.txt](DEVNOTES.txt).

## Running examples on JDK 9/10/11
Ignite uses proprietary SDK APIs that are not available by default. See also [How to run Ignite on JDK 9,10 and 11](https://apacheignite.readme.io/docs/getting-started#section-running-ignite-with-java-9-10-11)

To set up local IDE to easier access to examples, it is possible to add following options as default for all applications

``--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED
   --add-exports=java.base/sun.nio.ch=ALL-UNNAMED
   --add-exports=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED
   --add-exports=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED
   --add-exports=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED
   --add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED
   --illegal-access=permit``

For example, for IntelliJ IDEA it is possible to use Application Templates.

Use 'Run' -> 'Edit Configuration' menu.

<img src="https://docs.google.com/drawings/d/e/2PACX-1vQFgjhrPsLPUmic8CA_s1YpjVwA2vQITxNsLrAKOecZxIQEZSb1Ps2XKh0QEn8z9vtYiUofnGek_cag/pub?w=960&h=720"/>

## Contributing to Examples
*Notice* When updating classpath of examples and in case any modifications required in [pom.xml](pom.xml)
please make sure that corresponding changes were applied to
 * [pom-standalone.xml](pom-standalone.xml),
 * [pom-standalone-lgpl.xml](pom-standalone-lgpl.xml).
 
 These pom files are finalized during release and placed to examples folder with these examples code.
