Apache Ignite Schema Import Utility
===================================

Ignite ships with CacheJdbcPojoStore, which is out-of-the-box JDBC
implementation of the IgniteCacheStore interface, and automatically
handles all the write-through and read-through logic.

Ignite also ships with its own database schema mapping wizard which provides automatic
support for integrating with persistence stores. This utility automatically connects to
the underlying database and generates all the required XML OR-mapping configuration
and Java domain model POJOs.

For information on how to get started with Apache Ignite Schema Import Utility please visit:

    http://apacheignite.readme.io/v1.0/docs/automatic-persistence
