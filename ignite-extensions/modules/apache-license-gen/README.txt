Apache Ignite License Generator Module
------------------------------

Apache Ignite License Generator module is a custom maven resource plugin.
It generates /license/{module name}-licenses.txt file contains list of module's non transitive dependencies.
Apache Ignite binary distribution contains all non transitive dependencies of it's modules.
Set of modules included to binary distribution may vary as well as their dependencies list, versions and licenses.
Automatic generation of /license/{module name}-licenses.txt file guarantee that binary distribution gives user
actual information about licenses used by Apache Ignite's modules.

Note that in case dependency provided under Apache License 2.0 only in will not appear inside generated file.

To use Apache Ignite License Generator Module in your project please add following to pom.xml:

<plugin><!-- generates dependencies licenses -->
     <groupId>org.apache.maven.plugins</groupId>
     <artifactId>maven-remote-resources-plugin</artifactId>
     <executions>
         <execution>
             <id>ignite-dependencies</id>
             <goals>
                 <goal>process</goal>
             </goals>
             <configuration>
                 <resourceBundles>
                     <resourceBundle>org.apache.ignite:ignite-apache-license-gen:${project.version}</resourceBundle>
                 </resourceBundles>
                 <excludeTransitive>true</excludeTransitive>
             </configuration>
         </execution>
     </executions>
 </plugin>

