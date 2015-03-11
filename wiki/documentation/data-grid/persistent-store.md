<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

JCache specification comes with APIs for [javax.cache.inegration.CacheLoader](https://ignite.incubator.apache.org/jcache/1.0.0/javadoc/javax/cache/integration/CacheLoader.html) and [javax.cache.inegration.CacheWriter](https://ignite.incubator.apache.org/jcache/1.0.0/javadoc/javax/cache/integration/CacheWriter.html) which are used for **write-through** and **read-through** to and from an underlying persistent storage respectively (e.g. an RDBMS database like Oracle or MySQL, or NoSQL database like MongoDB or Couchbase).

While Ignite allows you to configure the `CacheLoader` and `CacheWriter` separately, it is very awkward to implement a transactional store within 2 separate classes, as multiple `load` and `put` operations have to share the same connection within the same transaction. To mitigate that, Ignite provides `org.apache.ignite.cache.store.CacheStore` interface which extends both, `CacheLoader` and `CacheWriter`. 
[block:callout]
{
  "type": "info",
  "title": "Transactions",
  "body": "`CacheStore` is fully transactional and automatically merges into the ongoing cache transaction."
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "CacheStore"
}
[/block]
`CacheStore` interface in Ignite is used to write and load data to and from the underlying data store. In addition to standard JCache loading and storing methods, it also introduces end-of-transaction demarcation and ability to bulk load a cache from the underlying data store.

## loadCache()
`CacheStore.loadCache()` method allows for cache loading even without passing all the keys that need to be loaded. It is generally used for hot-loading the cache on startup, but can be also called at any point after the cache has been started.

`IgniteCache.loadCache()` method will delegate to `CacheStore.loadCache()` method on every cluster member that is running the cache. To invoke loading only on the local cluster node, use `IgniteCache.localLoadCache()` method.
[block:callout]
{
  "type": "info",
  "body": "In case of partitioned caches, keys that are not mapped to this node, either as primary or backups, will be automatically discarded by the cache."
}
[/block]
## load(), write(), delete()
Methods `load()`, `write()`, and `delete()` on the `CacheStore` are called whenever methods `get()`, `put()`, and `remove()` are called correspondingly on the `IgniteCache` interface. These methods are used to enable **read-through** and **write-through** behavior when working with individual cache entries.

## loadAll(), writeAll(), deleteAll()
Methods `loadAll()`, `writeAll()`, and `deleteAll()` on the `CacheStore` are called whenever methods `getAll()`, `putAll()`, and `removeAll()` are called correspondingly on the `IgniteCache` interface. These methods are used to enable **read-through** and **write-through** behavior when working with multiple cache entries and should generally be implemented using batch operations to provide better performance.
[block:callout]
{
  "type": "info",
  "title": "",
  "body": "`CacheStoreAdapter` provides default implementation for `loadAll()`, `writeAll()`, and `deleteAll()` methods which simply iterates through all keys one by one."
}
[/block]
## sessionEnd()
Ignite has a concept of store session which may span more than one cache store operation. Sessions are especially useful when working with transactions.

In case of `ATOMIC` caches, method `sessionEnd()` is called after completion of each `CacheStore` method. In case of `TRANSACTIONAL` caches, `sessionEnd()` is called at the end of each transaction, which allows to either commit or rollback multiple operations on the underlying persistent store.
[block:callout]
{
  "type": "info",
  "body": "`CacheStoreAdapater` provides default empty implementation of `sessionEnd()` method."
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "CacheStoreSession"
}
[/block]
The main purpose of cache store session is to hold the context between multiple store invocations whenever `CacheStore` is used in a cache transaction. For example, if using JDBC, you can store the ongoing database connection via `CacheStoreSession.attach()` method. You can then commit this connection in the `CacheStore#sessionEnd(boolean)` method.

`CacheStoreSession` can be injected into your cache store implementation via `@GridCacheStoreSessionResource` annotation.
[block:api-header]
{
  "type": "basic",
  "title": "CacheStore Example"
}
[/block]
Below are a couple of different possible cache store implementations. Note that transactional implementation works with and without transactions.
[block:code]
{
  "codes": [
    {
      "code": "public class CacheJdbcPersonStore extends CacheStoreAdapter<Long, Person> {\n  // This mehtod is called whenever \"get(...)\" methods are called on IgniteCache.\n  @Override public Person load(Long key) {\n    try (Connection conn = connection()) {\n      try (PreparedStatement st = conn.prepareStatement(\"select * from PERSONS where id=?\")) {\n        st.setLong(1, key);\n\n        ResultSet rs = st.executeQuery();\n\n        return rs.next() ? new Person(rs.getLong(1), rs.getString(2), rs.getString(3)) : null;\n      }\n    }\n    catch (SQLException e) {\n      throw new CacheLoaderException(\"Failed to load: \" + key, e);\n    }\n  }\n\n  // This mehtod is called whenever \"put(...)\" methods are called on IgniteCache.\n  @Override public void write(Cache.Entry<Long, Person> entry) {\n    try (Connection conn = connection()) {\n      // Syntax of MERGE statement is database specific and should be adopted for your database.\n      // If your database does not support MERGE statement then use sequentially update, insert statements.\n      try (PreparedStatement st = conn.prepareStatement(\n        \"merge into PERSONS (id, firstName, lastName) key (id) VALUES (?, ?, ?)\")) {\n        for (Cache.Entry<Long, Person> entry : entries) {\n          Person val = entry.getValue();\n          \n          st.setLong(1, entry.getKey());\n          st.setString(2, val.getFirstName());\n          st.setString(3, val.getLastName());\n          \n          st.executeUpdate();\n        }\n      }\n    }\n    catch (SQLException e) {\n      throw new CacheWriterException(\"Failed to write [key=\" + key + \", val=\" + val + ']', e);\n    }\n  }\n\n  // This mehtod is called whenever \"remove(...)\" methods are called on IgniteCache.\n  @Override public void delete(Object key) {\n    try (Connection conn = connection()) {\n      try (PreparedStatement st = conn.prepareStatement(\"delete from PERSONS where id=?\")) {\n        st.setLong(1, (Long)key);\n\n        st.executeUpdate();\n      }\n    }\n    catch (SQLException e) {\n      throw new CacheWriterException(\"Failed to delete: \" + key, e);\n    }\n  }\n\n  // This mehtod is called whenever \"loadCache()\" and \"localLoadCache()\"\n  // methods are called on IgniteCache. It is used for bulk-loading the cache.\n  // If you don't need to bulk-load the cache, skip this method.\n  @Override public void loadCache(IgniteBiInClosure<Long, Person> clo, Object... args) {\n    if (args == null || args.length == 0 || args[0] == null)\n      throw new CacheLoaderException(\"Expected entry count parameter is not provided.\");\n\n    final int entryCnt = (Integer)args[0];\n\n    try (Connection conn = connection()) {\n      try (PreparedStatement st = conn.prepareStatement(\"select * from PERSONS\")) {\n        try (ResultSet rs = st.executeQuery()) {\n          int cnt = 0;\n\n          while (cnt < entryCnt && rs.next()) {\n            Person person = new Person(rs.getLong(1), rs.getString(2), rs.getString(3));\n\n            clo.apply(person.getId(), person);\n\n            cnt++;\n          }\n        }\n      }\n    }\n    catch (SQLException e) {\n      throw new CacheLoaderException(\"Failed to load values from cache store.\", e);\n    }\n  }\n\n  // Open JDBC connection.\n  private Connection connection() throws SQLException  {\n    // Open connection to your RDBMS systems (Oracle, MySQL, Postgres, DB2, Microsoft SQL, etc.)\n    // In this example we use H2 Database for simplification.\n    Connection conn = DriverManager.getConnection(\"jdbc:h2:mem:example;DB_CLOSE_DELAY=-1\");\n\n    conn.setAutoCommit(true);\n\n    return conn;\n  }\n}",
      "language": "java",
      "name": "jdbc non-transactional"
    },
    {
      "code": "public class CacheJdbcPersonStore extends CacheStoreAdapter<Long, Person> {\n  /** Auto-injected store session. */\n  @CacheStoreSessionResource\n  private CacheStoreSession ses;\n\n  // Complete transaction or simply close connection if there is no transaction.\n  @Override public void sessionEnd(boolean commit) {\n    try (Connection conn = ses.getAttached()) {\n      if (conn != null && ses.isWithinTransaction()) {\n        if (commit)\n          conn.commit();\n        else\n          conn.rollback();\n      }\n    }\n    catch (SQLException e) {\n      throw new CacheWriterException(\"Failed to end store session.\", e);\n    }\n  }\n\n  // This mehtod is called whenever \"get(...)\" methods are called on IgniteCache.\n  @Override public Person load(Long key) {\n    try (Connection conn = connection()) {\n      try (PreparedStatement st = conn.prepareStatement(\"select * from PERSONS where id=?\")) {\n        st.setLong(1, key);\n\n        ResultSet rs = st.executeQuery();\n\n        return rs.next() ? new Person(rs.getLong(1), rs.getString(2), rs.getString(3)) : null;\n      }\n    }\n    catch (SQLException e) {\n      throw new CacheLoaderException(\"Failed to load: \" + key, e);\n    }\n  }\n\n  // This mehtod is called whenever \"put(...)\" methods are called on IgniteCache.\n  @Override public void write(Cache.Entry<Long, Person> entry) {\n    try (Connection conn = connection()) {\n      // Syntax of MERGE statement is database specific and should be adopted for your database.\n      // If your database does not support MERGE statement then use sequentially update, insert statements.\n      try (PreparedStatement st = conn.prepareStatement(\n        \"merge into PERSONS (id, firstName, lastName) key (id) VALUES (?, ?, ?)\")) {\n        for (Cache.Entry<Long, Person> entry : entries) {\n          Person val = entry.getValue();\n          \n          st.setLong(1, entry.getKey());\n          st.setString(2, val.getFirstName());\n          st.setString(3, val.getLastName());\n          \n          st.executeUpdate();\n        }\n      }\n    }        \n    catch (SQLException e) {\n      throw new CacheWriterException(\"Failed to write [key=\" + key + \", val=\" + val + ']', e);\n    }\n  }\n\n  // This mehtod is called whenever \"remove(...)\" methods are called on IgniteCache.\n  @Override public void delete(Object key) {\n    try (Connection conn = connection()) {\n      try (PreparedStatement st = conn.prepareStatement(\"delete from PERSONS where id=?\")) {\n        st.setLong(1, (Long)key);\n\n        st.executeUpdate();\n      }\n    }\n    catch (SQLException e) {\n      throw new CacheWriterException(\"Failed to delete: \" + key, e);\n    }\n  }\n\n  // This mehtod is called whenever \"loadCache()\" and \"localLoadCache()\"\n  // methods are called on IgniteCache. It is used for bulk-loading the cache.\n  // If you don't need to bulk-load the cache, skip this method.\n  @Override public void loadCache(IgniteBiInClosure<Long, Person> clo, Object... args) {\n    if (args == null || args.length == 0 || args[0] == null)\n      throw new CacheLoaderException(\"Expected entry count parameter is not provided.\");\n\n    final int entryCnt = (Integer)args[0];\n\n    try (Connection conn = connection()) {\n      try (PreparedStatement st = conn.prepareStatement(\"select * from PERSONS\")) {\n        try (ResultSet rs = st.executeQuery()) {\n          int cnt = 0;\n\n          while (cnt < entryCnt && rs.next()) {\n            Person person = new Person(rs.getLong(1), rs.getString(2), rs.getString(3));\n\n            clo.apply(person.getId(), person);\n\n            cnt++;\n          }\n        }\n      }\n    }\n    catch (SQLException e) {\n      throw new CacheLoaderException(\"Failed to load values from cache store.\", e);\n    }\n  }\n\n  // Opens JDBC connection and attaches it to the ongoing\n  // session if within a transaction.\n  private Connection connection() throws SQLException  {\n    if (ses.isWithinTransaction()) {\n      Connection conn = ses.getAttached();\n\n      if (conn == null) {\n        conn = openConnection(false);\n\n        // Store connection in the session, so it can be accessed\n        // for other operations within the same transaction.\n        ses.attach(conn);\n      }\n\n      return conn;\n    }\n    // Transaction can be null in case of simple load or put operation.\n    else\n      return openConnection(true);\n  }\n\n  // Opens JDBC connection.\n  private Connection openConnection(boolean autocommit) throws SQLException {\n    // Open connection to your RDBMS systems (Oracle, MySQL, Postgres, DB2, Microsoft SQL, etc.)\n    // In this example we use H2 Database for simplification.\n    Connection conn = DriverManager.getConnection(\"jdbc:h2:mem:example;DB_CLOSE_DELAY=-1\");\n\n    conn.setAutoCommit(autocommit);\n\n    return conn;\n  }\n}",
      "language": "java",
      "name": "jdbc transactional"
    },
    {
      "code": "public class CacheJdbcPersonStore extends CacheStore<Long, Person> {\n  // Skip single operations and open connection methods.\n  // You can copy them from jdbc non-transactional or jdbc transactional examples.\n  ...\n  \n  // This mehtod is called whenever \"getAll(...)\" methods are called on IgniteCache.\n  @Override public Map<K, V> loadAll(Iterable<Long> keys) {\n    try (Connection conn = connection()) {\n      try (PreparedStatement st = conn.prepareStatement(\n        \"select firstName, lastName from PERSONS where id=?\")) {\n        Map<K, V> loaded = new HashMap<>();\n        \n        for (Long key : keys) {\n          st.setLong(1, key);\n          \n          try(ResultSet rs = st.executeQuery()) {\n            if (rs.next())\n              loaded.put(key, new Person(key, rs.getString(1), rs.getString(2));\n          }\n        }\n\n        return loaded;\n      }\n    }\n    catch (SQLException e) {\n      throw new CacheLoaderException(\"Failed to loadAll: \" + keys, e);\n    }\n  }\n  \n  // This mehtod is called whenever \"putAll(...)\" methods are called on IgniteCache.\n  @Override public void writeAll(Collection<Cache.Entry<Long, Person>> entries) {\n    try (Connection conn = connection()) {\n      // Syntax of MERGE statement is database specific and should be adopted for your database.\n      // If your database does not support MERGE statement then use sequentially update, insert statements.\n      try (PreparedStatement st = conn.prepareStatement(\n        \"merge into PERSONS (id, firstName, lastName) key (id) VALUES (?, ?, ?)\")) {\n        for (Cache.Entry<Long, Person> entry : entries) {\n          Person val = entry.getValue();\n          \n          st.setLong(1, entry.getKey());\n          st.setString(2, val.getFirstName());\n          st.setString(3, val.getLastName());\n          \n          st.addBatch();\n        }\n        \n\t\t\t\tst.executeBatch();\n      }\n    }\n    catch (SQLException e) {\n      throw new CacheWriterException(\"Failed to writeAll: \" + entries, e);\n    }\n  }\n  \n  // This mehtod is called whenever \"removeAll(...)\" methods are called on IgniteCache.\n  @Override public void deleteAll(Collection<Long> keys) {\n    try (Connection conn = connection()) {\n      try (PreparedStatement st = conn.prepareStatement(\"delete from PERSONS where id=?\")) {\n        for (Long key : keys) {\n          st.setLong(1, key);\n          \n          st.addBatch();\n        }\n        \n\t\t\t\tst.executeBatch();\n      }\n    }\n    catch (SQLException e) {\n      throw new CacheWriterException(\"Failed to deleteAll: \" + keys, e);\n    }\n  }\n}",
      "language": "java",
      "name": "jdbc bulk operations"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Configuration"
}
[/block]
`CacheStore` interface can be set on `IgniteConfiguration` via a `Factory` in much the same way like `CacheLoader` and `CacheWriter` are being set.
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.configuration.IgniteConfiguration\">\n  ...\n    <property name=\"cacheConfiguration\">\n      <list>\n        <bean class=\"org.apache.ignite.configuration.CacheConfiguration\">\n          ...\n          <property name=\"cacheStoreFactory\">\n            <bean class=\"javax.cache.configuration.FactoryBuilder$SingletonFactory\">\n              <constructor-arg>\n                <bean class=\"foo.bar.MyPersonStore\">\n    \t\t\t\t\t\t\t...\n    \t\t\t\t\t\t</bean>\n    \t\t\t\t\t</constructor-arg>\n    \t\t\t\t</bean>\n\t    \t\t</property>\n    \t\t\t...\n    \t\t</bean>\n    \t</list>\n    </property>\n  ...\n</bean>",
      "language": "xml"
    },
    {
      "code": "IgniteConfiguration cfg = new IgniteConfiguration();\n\nCacheConfiguration<Long, Person> cacheCfg = new CacheConfiguration<>();\n\nCacheStore<Long, Person> store;\n\nstore = new MyPersonStore();\n\ncacheCfg.setCacheStoreFactory(new FactoryBuilder.SingletonFactory<>(store));\ncacheCfg.setReadThrough(true);\ncacheCfg.setWriteThrough(true);\n\ncfg.setCacheConfiguration(cacheCfg);\n\n// Start Ignite node.\nIgnition.start(cfg);",
      "language": "java"
    }
  ]
}
[/block]