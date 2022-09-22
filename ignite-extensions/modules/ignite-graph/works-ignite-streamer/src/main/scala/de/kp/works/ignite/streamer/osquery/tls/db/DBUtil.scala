package de.kp.works.ignite.streamer.osquery.tls.db
/*
 * Copyright (c) 20129 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import org.apache.ignite.binary.BinaryObject
import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.cache.{CacheMode, QueryEntity}
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.{Ignite, IgniteCache}

import java.security.MessageDigest
import java.util

object DBUtil {

  /**
   * The actual Osquery database consists tables to manage
   * `nodes`, `queries`, `tasks` and `configurations`.
   *
   * `tasks` specifies the relation between a certain node
   * and query.
   */
  def buildTables(ignite:Ignite, namespace:String):Unit = {

    buildNodes(ignite, namespace)
    buildQueries(ignite, namespace)

    buildTasks(ignite, namespace)
    buildConfigurations(ignite, namespace)

  }

  /** SQL QUERY OPERATIONS **/

  /**
   * A basic method to apply a [SqlFieldsQuery]
   * to a certain Apache Ignite cache
   */
  def readSql(ignite:Ignite, cacheName:String, readSql:String): util.List[java.util.List[_]] = {

    val query = new SqlFieldsQuery(readSql)

    val cache = ignite.cache[String, BinaryObject](cacheName)
    cache.query(query).getAll

  }

  /** CONFIGURATION OPERATIONS **/

  def createOrUpdateConfiguration(ignite:Ignite, namespace:String, values:Seq[Any]):Unit = {

    val cacheName = namespace + "_configurations"
    val cache = ignite.cache[String, BinaryObject](cacheName)
    /*
     * It is expected that the values are in the same order
     * as the defined columns:
     *
     * 0: uuid string
     * 1: timestamp long
     * 2: node string
     * 3: config string
     */
    val columnNames = Array("uuid", "timestamp", "node", "config")
    /*
     * Build cache value
     */
    val builder = ignite.binary().builder(cacheName)
    columnNames.zip(values).foreach{ case(name, value) => builder.setField(name, value)}

    val cacheValue = builder.build()
    /*
     * The cache key is built from the content
     * to enable the detection of duplicates.
     */
    val serialized = values.map(_.toString).mkString("#")
    val cacheKey = new String(MessageDigest.getInstance("MD5")
      .digest(serialized.getBytes("UTF-8")))

    cache.put(cacheKey, cacheValue)

  }
  /** NODE OPERATIONS **/

  def createOrUpdateNode(ignite:Ignite, namespace:String, values:Seq[Any]):Unit = {

    val cacheName = namespace + "_nodes"
    val cache = ignite.cache[String, BinaryObject](cacheName)
    /*
     * It is expected that the values are in the same order
     * as the defined columns:
     *
     * 0: uuid string
     * 1: timestamp long
     * 2: active boolean
     * 3: enrolled boolean
     * 4: secret string
     * 5: key string
     * 6: host string
     * 7: checkin long
     * 8: address string
     */
    val columnNames = Array("uuid", "timestamp", "active", "enrolled", "secret", "key", "host", "checkin", "address")
    /*
     * Build cache value
     */
    val builder = ignite.binary().builder(cacheName)
    columnNames.zip(values).foreach{ case(name, value) => builder.setField(name, value)}

    val cacheValue = builder.build()
    /*
     * The cache key is built from the content
     * to enable the detection of duplicates.
     */
    val serialized = values.map(_.toString).mkString("#")
    val cacheKey = new String(MessageDigest.getInstance("MD5")
      .digest(serialized.getBytes("UTF-8")))

    cache.put(cacheKey, cacheValue)

  }

  /** QUERY OPERATIONS **/

  def createOrUpdateQuery(ignite:Ignite, namespace:String, values:Seq[Any]):Unit = {

    val cacheName = namespace + "_queries"
    val cache = ignite.cache[String, BinaryObject](cacheName)
    /*
     * It is expected that the values are in the same order
     * as the defined columns:
     *
     * 0: uuid string
     * 1: timestamp long
     * 2: description string
     * 3: sql string
     * 4: notbefore long
     */
    val columnNames = Array("uuid", "timestamp", "description", "sql", "notbefore")
    /*
     * Build cache value
     */
    val builder = ignite.binary().builder(cacheName)
    columnNames.zip(values).foreach{ case(name, value) => builder.setField(name, value)}

    val cacheValue = builder.build()
    /*
     * The cache key is built from the content
     * to enable the detection of duplicates.
     */
    val serialized = values.map(_.toString).mkString("#")
    val cacheKey = new String(MessageDigest.getInstance("MD5")
      .digest(serialized.getBytes("UTF-8")))

    cache.put(cacheKey, cacheValue)

  }

  /** TASK OPERATIONS **/

  def createOrUpdateTask(ignite:Ignite, namespace:String, values:Seq[Any]):Unit = {

    val cacheName = namespace + "_tasks"
    val cache = ignite.cache[String, BinaryObject](cacheName)
    /*
     * It is expected that the values are in the same order
     * as the defined columns:
     *
     * 0: uuid string
     * 1: timestamp long
     * 2: node string
     * 3: query string
     * 4: status string
     */
    val columnNames = Array("uuid", "timestamp", "node", "query", "status")
    /*
     * Build cache value
     */
    val builder = ignite.binary().builder(cacheName)
    columnNames.zip(values).foreach{ case(name, value) => builder.setField(name, value)}

    val cacheValue = builder.build()
    /*
     * The cache key is built from the content
     * to enable the detection of duplicates.
     */
    val serialized = values.map(_.toString).mkString("#")
    val cacheKey = new String(MessageDigest.getInstance("MD5")
      .digest(serialized.getBytes("UTF-8")))

    cache.put(cacheKey, cacheValue)

  }

  def buildNodes(ignite:Ignite, namespace:String):Unit = {

    val table = "nodes"
    val columns = new util.LinkedHashMap[String, String]

    columns.put("uuid",      "java.lang.String")
    columns.put("timestamp", "java.lang.Long")
    columns.put("active",    "java.lang.Boolean")
    columns.put("enrolled",  "java.lang.Boolean")
    columns.put("secret",    "java.lang.String")
    columns.put("key",       "java.lang.String")
    columns.put("host",      "java.lang.String")
    columns.put("checkin",   "java.lang.Long")
    columns.put("address",   "java.lang.String")

    createCacheIfNotExists(ignite, table, namespace, columns)

  }

  def buildQueries(ignite:Ignite, namespace:String):Unit = {

    val table = "queries"
    val columns = new util.LinkedHashMap[String, String]

    columns.put("uuid",        "java.lang.String")
    columns.put("timestamp",   "java.lang.Long")
    columns.put("description", "java.lang.String")
    columns.put("sql",         "java.lang.String")
    columns.put("notbefore",   "java.lang.Long")

    createCacheIfNotExists(ignite, table, namespace, columns)

  }

  def buildTasks(ignite:Ignite, namespace:String):Unit = {

    val table = "tasks"
    val columns = new util.LinkedHashMap[String, String]

    columns.put("uuid",        "java.lang.String")
    columns.put("timestamp",   "java.lang.Long")
    columns.put("node",        "java.lang.String")
    columns.put("query",       "java.lang.String")
    columns.put("status",      "java.lang.String")

    createCacheIfNotExists(ignite, table, namespace, columns)

  }

  def buildConfigurations(ignite:Ignite, namespace:String):Unit = {

    val table = "nodes"
    val columns = new util.LinkedHashMap[String, String]

    columns.put("uuid",        "java.lang.String")
    columns.put("timestamp",   "java.lang.Long")
    columns.put("node",        "java.lang.String")
    columns.put("config",      "java.lang.String")

    createCacheIfNotExists(ignite, table, namespace, columns)

  }

  def createCacheIfNotExists(
    ignite:Ignite,
    table: String,
    namespace:String,
    columns:util.LinkedHashMap[String, String]): Unit = {

    val cacheName = namespace + "_" + table

    val exists: Boolean = ignite.cacheNames.contains(cacheName)
    if (!exists) {
      createCache(ignite, cacheName, columns)
    }
  }

  def createCache(
    ignite:Ignite,
    cacheName: String,
    columns:util.LinkedHashMap[String, String]): IgniteCache[String, BinaryObject] = {
      createCache(ignite, cacheName, columns, CacheMode.REPLICATED)
  }

  def createCache(
     ignite:Ignite,
     cacheName: String,
     columns:util.LinkedHashMap[String, String],
     cacheMode: CacheMode): IgniteCache[String, BinaryObject] = {

    val cfg = createCacheCfg(cacheName, columns, cacheMode)
    ignite.createCache(cfg)

  }
  /**
   * This method creates an Ignite cache configuration
   * for a database tables that does not exist
   */
  def createCacheCfg(
    cacheName: String,
    columns:util.LinkedHashMap[String, String],
    cacheMode: CacheMode): CacheConfiguration[String, BinaryObject] = {
    /*
     * Defining query entities is the Apache Ignite
     * mechanism to dynamically define a queryable
     * 'class'
     */
    val qe = buildQueryEntity(cacheName, columns)
    val qes = new util.ArrayList[QueryEntity]
    qes.add(qe)
    /*
     * Specify Apache Ignite cache configuration; it is
     * important to leverage 'BinaryObject' as well as
     * 'setStoreKeepBinary'
     */
    val cfg = new CacheConfiguration[String, BinaryObject]
    cfg.setName(cacheName)

    cfg.setStoreKeepBinary(false)
    cfg.setIndexedTypes(classOf[String], classOf[BinaryObject])

    cfg.setCacheMode(cacheMode)
    cfg.setQueryEntities(qes)

    cfg
  }

  def buildQueryEntity(table: String, columns:util.LinkedHashMap[String, String]): QueryEntity = {

    val qe = new QueryEntity
    /*
     * The key type of the Apache Ignite cache is set to
     * [String], i.e. an independent identity management is
     * used here
     */
    qe.setKeyType("java.lang.String")
    /*
     * The 'table' is used as table name in select statement
     * as well as the name of 'ValueType'
     */
    qe.setValueType(table)
    /*
     * Define fields for the Apache Ignite cache that is
     * used as one of the data backends.
     */
    qe.setFields(columns)
    qe

  }

}
