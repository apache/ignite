Apache Ignite Schema Import Utility
===================================

Ignite ships with CacheJdbcPojoStore, which is out-of-the-box JDBC
implementation of the IgniteCacheStore interface, and automatically
handles all the write-through and read-through logic.

Ignite also ships with its own database schema mapping wizard which provides automatic
support for integrating with persistence stores. This utility automatically connects to
the underlying database and generates all the required XML OR-mapping configuration
and Java domain model POJOs.

Schema Import Utility Demo
==========================
1. Start H2 database: "examples/schema-import/bin/h2-server.sh"
   H2 server will start and H2 Console will be opened in your default browser.

2. Connect to H2 database with following settings:

   a. Select "Generic H2 (Server)" settings.

   b. IMPORTANT: enter JDBC URL "jdbc:h2:tcp://localhost/~/schema-import/demo"

   c. Click "Connect".

3. Paste content of "examples/schema-import/bin/db-init.sql" into H2 Console and execute.

4. Start Schema Import utility: "IGNITE_HOME/bin/ignite-schema-import.sh examples/schema-import/bin/schema-import.properties"
   Schema Utility will start with predefined settings for this demo.
   Click "Next", "Generate" and answer "Yes" to all override warnings.

5. Import "examples/schema-import/pom.xml" in your Java IDE.

6. Run "Demo.java" example.

For more information on how to get started with Apache Ignite Schema Import Utility please visit:

    http://apacheignite.readme.io/docs/automatic-persistence
