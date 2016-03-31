Apache Ignite OSGi Pax Logging Fragment Module
----------------------------------------------

This module is an OSGi fragment that exposes the following packages from the Pax Logging API bundle:

  - org.apache.log4j.varia
  - org.apache.log4j.xml

These packages are required when installing the ignite-log4j bundle, and are not exposed by default
by the Pax Logging API - the logging framework used by Apache Karaf.

This fragment exposes them.
