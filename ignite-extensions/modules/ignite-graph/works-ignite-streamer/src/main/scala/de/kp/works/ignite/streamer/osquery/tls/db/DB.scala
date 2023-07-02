package de.kp.works.ignite.streamer.osquery.tls.db
/*
 * Copyright (c) 2021 Dr. Krusche & Partner PartG. All rights reserved.
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

import com.google.gson._
import de.kp.works.ignite.conf.WorksConf
import org.apache.ignite.Ignite

import java.util
import scala.collection.JavaConversions._

/**
 * Osquery uses Apache Ignite as internal database
 * to manage configurations and nodes.
 */
object DB {

  private var instance:Option[DB] = None
  private var tables:Option[Boolean] = None

  def getInstance(ignite:Ignite):DB = {

    if (ignite == null) {
      val now = new java.util.Date().toString
      throw new Exception(s"[ERROR] $now - No Ignite client available.")
    }
    if (instance.isEmpty)
      instance = Some(new DB(ignite))
    /*
     * Build all Apache Ignite caches needed to manage
     * configurations, nodes, queries, and tasks
     */
    if (tables.isEmpty) {

      instance.get.buildTables()
      tables = Some(true)

    }

    instance.get

  }
}

class DB(ignite:Ignite) {

  private val namespace = WorksConf.getNSCfg(WorksConf.OSQUERY_CONF)

  /** NODE **/

  def createOrUpdateNode(values:Seq[Any]):Unit = {
    DBUtil.createOrUpdateNode(ignite, namespace, values)
  }

  /** QUERY **/

  def createOrUpdateQuery(values: Seq[String]):Unit = {
    DBUtil.createOrUpdateQuery(ignite, namespace, values)
  }

  /** TASK **/

  def createOrUpdateTask(values:Seq[String]):Unit = {
    DBUtil.createOrUpdateTask(ignite, namespace, values)
  }

  def createOrUpdateTask(task:OsqueryQueryTask):Unit = {
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
    val values = Seq(
      task.uuid,
      task.timestamp,
      task.node,
      task.query,
      task.status
    ).map(_.toString)

    createOrUpdateTask(values)

  }

  /** CONFIGURATION **/

  def createOrUpdateConfiguration(values:Seq[String]):Unit = {
    DBUtil.createOrUpdateConfiguration(ignite, namespace, values)
  }

  /** READ OPERATIONS - CONFIGURATION **/

  def readConfigByNode(node:String):String = {

    try {
      /*
       * Build SQL query for the [configurations] cache
       */
      val cacheName = s"${namespace}_configurations"
      val fields = Array(
        "_key",
        "uuid",
        "timestamp",
        "node",
        "config"
      ).mkString(",")
      /*
       * This method expects that the configuration cache
       * is created and filled with node specific configurations
       */
      val sql = s"select $fields from $cacheName where node = '$node'"
      val sqlResult:util.List[java.util.List[_]] = DBUtil.readSql(ignite, cacheName, sql)

      val configs = sqlResult.map(config => {
        /*
         * 0: _key string
         * 1: uuid string
         * 2: timestamp long
         * 3: node string
         * 4: config string
         */
        config.get(4).asInstanceOf[String]
      })

      configs.head

    } catch {
      case _:Throwable => null
    }

  }

  /** READ OPERATIONS - NODE **/

  /**
   * This method retrieves a specific node instance
   * that is identified by its shared `key`
   */
  def readNodeByKey(key:String):OsqueryNode = {

    try {
      /*
       * Build SQL query for the [nodes] cache
       */
      val cacheName = s"${namespace}_nodes"
      val fields = Array(
        "_key",
        "uuid",
        "timestamp",
        "active",
        "enrolled",
        "secret",
        "key",
        "host",
        "checkin",
        "address"
      ).mkString(",")

      val sql = s"select $fields from $cacheName where key = '$key'"
      val sqlResult:util.List[java.util.List[_]] = DBUtil.readSql(ignite, cacheName, sql)

      val nodes = sqlResult2Nodes(sqlResult)
      nodes.head

    } catch {
      case _:Throwable => null
    }

  }
  /**
   * This method retrieves a specific node instance
   * that is identified by its shared `secret`
   */
  def readNodeBySecret(secret:String):OsqueryNode = {

    try {
      /*
       * Build SQL query for the [nodes] cache
       */
      val cacheName = s"${namespace}_nodes"
      val fields = Array(
        "_key",
        "uuid",
        "timestamp",
        "active",
        "enrolled",
        "secret",
        "key",
        "host",
        "checkin",
        "address"
      ).mkString(",")

      val sql = s"select $fields from $cacheName where key = '$secret'"
      val sqlResult:util.List[java.util.List[_]] = DBUtil.readSql(ignite, cacheName, sql)

      val nodes = sqlResult2Nodes(sqlResult)
      nodes.head

    } catch {
      case _:Throwable => null
    }

  }
  /**
   * This method determines whether a certain node
   * is still alive
   *
   * interval: pre-defined time interval to describe
   * accepted inactivity
   */
  def readNodeHealth(uuid:String, interval:Long):String = {

    try {
      /*
       * Build SQL query for the [nodes] cache
       */
      val cacheName = s"${namespace}_nodes"
      val fields = Array(
        "_key",
        "uuid",
        "timestamp",
        "active",
        "enrolled",
        "secret",
        "key",
        "host",
        "checkin",
        "address"
      ).mkString(",")

      val sql = s"select $fields from $cacheName where uuid = '$uuid'"
      val sqlResult:util.List[java.util.List[_]] = DBUtil.readSql(ignite, cacheName, sql)

      val checkins = sqlResult.map(node => {
        /*
         * 0: _key string
         * 1: uuid string
         * 2: timestamp long
         * 3: active boolean
         * 4: enrolled boolean
         * 5: secret string
         * 6: key string
         * 7: host string
         * 8: checkin long
         * 9: address string
         */
        node.get(8).asInstanceOf[Long]
      })

      val checkin = checkins.head

      val delta = System.currentTimeMillis - checkin
      if (delta > interval) "danger" else ""

    } catch {
      case _:Throwable => null
    }

  }

  private def sqlResult2Nodes(sqlResult:util.List[java.util.List[_]]):Seq[OsqueryNode] = {

    sqlResult.map(node => {
      /*
       * Note, the first position holds the cache key _key
       */
      val uuid = node.get(1).asInstanceOf[String]
      val timestamp = node.get(2).asInstanceOf[Long]

      val active = node.get(3).asInstanceOf[Boolean]
      val enrolled = node.get(4).asInstanceOf[Boolean]

      val secret = node.get(5).asInstanceOf[String]
      val key = node.get(6).asInstanceOf[String]

      val host = node.get(7).asInstanceOf[String]
      val checkin = node.get(8).asInstanceOf[Long]

      val address = node.get(9).asInstanceOf[String]

      OsqueryNode(
        uuid           = uuid,
        timestamp      = timestamp,
        active         = active,
        enrolled       = enrolled,
        secret         = secret,
        hostIdentifier = host,
        lastCheckIn    = checkin,
        lastAddress    = address,
        nodeKey        = key)

    })

  }

  /** READ OPERATIONS - QUERY **/

  /**
   * Retrieve all distributed queries assigned to a particular
   * node in the NEW state. This function will change the state
   * of the distributed query to PENDING, however will not commit
   * the change.
   *
   * It is the responsibility of the caller to commit or rollback
   * on the current database session.
    */
  def getNewQueries(uuid:String):JsonObject = {
    /*
     * STEP #1: Retrieve all distributed query tasks, that refer
     * to the provided node identifier and have status `NEW`, and
     * select all associated queries where 'notBefore < now`
     *
     * QueryTask.node == node AND QueryTask.status = 'NEW' AND Query.notBefore < now
     */
    val now = System.currentTimeMillis
    /*
     * The result is a `Map`that assigns the task `uuid` and
     * the `sql` field of the query
     */
    val result = readNewQueries(uuid, now)

    val queries = new JsonObject()
    result.foreach { case(uuid, sql) =>
      /*
       * The tasks, that refer to the database retrieval result have
       * to be updated; this should be done after the osquery node
       * received the queries, which, however, is not possible
       */
      var task = readTaskById(uuid)
      task = task.copy(status = "PENDING", timestamp = now)

      createOrUpdateTask(task)
      /*
       * Assign (task uuid, sql) to the output
       */
      queries.addProperty(uuid, sql.asInstanceOf[String])
    }

    queries
  }

  /**
   * This method retrieves queries that have been assigned to
   * a certain node but not deployed to the respective osquery
   * daemon
   */
  private def readNewQueries(uuid:String, timestamp:Long):Map[String,String] = {
    /*
     * NOTE: We do not expect to very large caches for `queries`
     * and `tasks` and therefore do not use Apache Ignite's join
     * mechanism.
     */
    try {
      /*
       * STEP #1: Retrieve all queries not before `timestamp`
       * and transform into a lookup dataset for the respective
       * SQL statements (`sql`)
       */
      val queries = {
        /*
          * Build SQL query for the [queries] cache
          */
        val cacheName = s"${namespace}_queries"
        val fields = Array(
          "_key",
          "uuid",
          "timestamp",
          "description",
          "sql",
          "notbefore"
        ).mkString(",")

        val sql = s"select $fields from $cacheName where notbefore < $timestamp"
        val sqlResult:util.List[java.util.List[_]] = DBUtil.readSql(ignite, cacheName, sql)

        sqlResult2Queries(sqlResult)
          /*
           * The queries dataset can be restricted
           * to `uuid` and `sql`
           */
          .map(query => (query.uuid, query.sql))
       }

      val quuids = queries.map{case(uuid, _) => s"'$uuid''"}.mkString(",")
      /*
       * STEP #2: Retrieve all `tasks` that refer to node `uuid`
       * and status `NEW` and are related to the retrieved queries
       */
      val tasks = {
        /*
          * Build SQL query for the [task] cache
          */
        val cacheName = s"${namespace}_tasks"
        val fields = Array(
          "_key",
          "uuid",
          "timestamp",
          "node",
          "query",
          "status"
        ).mkString(",")

        val sql = s"select $fields from $cacheName where node = '$uuid' and status = 'NEW' and query in ($quuids)"
        val sqlResult:util.List[java.util.List[_]] = DBUtil.readSql(ignite, cacheName, sql)

        sqlResult2Tasks(sqlResult)
          /*
           * The tasks dataset can be restricted
           * to `uuid` and `query`
           */
          .map(task => (task.uuid, task.query))
      }
      /*
       * STEP #3: Replace the query placeholder by the
       * referenced SQL statement
       */
      val sqlLookup = queries.toMap
      tasks.map{case(uuid, query) => (uuid, sqlLookup(query))}.toMap

    } catch {
      case _:Throwable => null
    }

  }

  private def sqlResult2Queries(sqlResult:util.List[java.util.List[_]]):Seq[OsqueryQuery] = {

    sqlResult.map(query => {
      /*
       * Note, the first position holds the cache key _key
       */
      val uuid = query.get(1).asInstanceOf[String]
      val timestamp = query.get(2).asInstanceOf[Long]

      val description = query.get(3).asInstanceOf[String]
      val sql = query.get(4).asInstanceOf[String]

      val notbefore = query.get(5).asInstanceOf[Long]

      OsqueryQuery(
        uuid        = uuid,
        timestamp   = timestamp,
        description = description,
        sql         = sql,
        notbefore   = notbefore)
    })

  }

  /** READ OPERATIONS - TASK **/

  def readTaskById(uuid:String):OsqueryQueryTask = {

    try {
      /*
       * Build SQL query for the [task] cache
       */
      val cacheName = s"${namespace}_tasks"
      val fields = Array(
        "_key",
        "uuid",
        "timestamp",
        "node",
        "query",
        "status"
      ).mkString(",")

      val sql = s"select $fields from $cacheName where uuid = '$uuid'"
      val sqlResult:util.List[java.util.List[_]] = DBUtil.readSql(ignite, cacheName, sql)

      val tasks = sqlResult2Tasks(sqlResult)
      tasks.head

    } catch {
      case _:Throwable => null
    }

  }

  private def sqlResult2Tasks(sqlResult:util.List[java.util.List[_]]):Seq[OsqueryQueryTask] = {

    sqlResult.map(task => {
      /*
       * Note, the first position holds the cache key _key
       */
      val uuid = task.get(1).asInstanceOf[String]
      val timestamp = task.get(2).asInstanceOf[Long]

      val node = task.get(3).asInstanceOf[String]
      val query = task.get(4).asInstanceOf[String]

      val status = task.get(5).asInstanceOf[String]

      OsqueryQueryTask(
        uuid      = uuid,
        timestamp = timestamp,
        node      = node,
        query     = query,
        status    = status)

    })

  }

  def buildTables():Unit = {
    DBUtil.buildTables(ignite, namespace)
  }

}
