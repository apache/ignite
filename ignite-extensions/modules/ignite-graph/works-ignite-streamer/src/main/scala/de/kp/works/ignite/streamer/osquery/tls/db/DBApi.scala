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

import com.google.gson.{JsonObject, JsonParser}
import org.apache.ignite.Ignite

case class OsqueryNode(
   /*
    * The unique identifier of this node
    */
   uuid:String,
   /*
    * The timestamp, this node has been
    * created
    */
   timestamp:Long,
   /*
    * Flag to specify whether the respective
    * osquery agent is `active`
    */
   active:Boolean,
   /*
    * Flag to specify whether the respective
    * node is enrolled already
    */
   enrolled:Boolean,
   /*
    * The secret shared between the TLS server
    * and this osquery node
    */
   secret:String,
   /*
    * The identifier of the remote osquery host
    */
   hostIdentifier:String,
   /*
    * The timestamp of the last change of this
    * node
    */
   lastCheckIn:Long,
   /*
    * The IP address of the remote osquery daemon
    * that most recently changed this node
    */
   lastAddress:String,
   /*
    * Unique identifer of this node, shared with the
    * remote osquery daemon
    */
   nodeKey:String)

/**
 * This class describes a registered distributed query;
 * it can be assigned to multiple nodes.
 */
case class OsqueryQuery(
   /*
    * The unique identifier of this distributed
    * query
    */
   uuid:String,
   /*
    * The timestamp, this distributed query has been
    * created
    */
   timestamp:Long,
   /*
    * The description of this distributed query;
    * note, this column is a database column and
    * is nullable
    */
   description:String,
   /*
    * The SQL statement, that specifies this query.
    * This database column is not nullable
    */
   sql:String,
   /*
    * This is a timestamp to define the earliest use
    * of this distributed query (schedule)
    */
   notbefore:Long)

case class OsqueryQueryTask(
   /*
    * The unique identifier of this distributed
    * query task
    */
   uuid:String,
   /*
    * The timestamp, this distributed query task has been
    * created
    */
   timestamp:Long,
   /*
    * The reference to the unique identifier of the
    * specific node
    */
   node:String,
   /*
    * The reference to the unique identifier of the
    * specific distributed query
    */
   query:String,
   /*
    * The status of a certain distributed query with respect
    * to a specific node. Permitted values are NEW, COMPLETE,
    * FAILED, PENDING
    */
   status:String)

/**
 * This object represents the interval interface to the
 * pre-defined and pre-configured nodes
 */
class DBApi(ignite:Ignite) {
 /*
  * This method retrieves the registered configuration that refers
  * to a certain remote osquery node:
  *
  * {
    * "options": {
  * 		"host_identifier": "hostname",
  *			"schedule_splay_percent": 10
    * },
  *	 "schedule": {
  *			"macos_kextstat": {
  *  			"query": "SELECT * FROM kernel_extensions;",
  *  			"interval": 10
  *		 	},
  * 		"foobar": {
  *  			"query": "SELECT foo, bar, pid FROM foobar_table;",
  *  			"interval": 600
  * 			}
    *	 },
    * "packs": {
  * 		"internal_stuff": {
  *  			"discovery": [
  *    		"SELECT pid FROM processes WHERE name = 'ldap';"
  *  		],
  *  		"platform": "linux",
  *  		"version": "1.5.2",
  *  		"queries": {
  *    		"active_directory": {
  *      		"query": "SELECT * FROM ad_config;",
  *      		"interval": "1200",
  *      		"description": "Check each user's active directory cached settings."
  *    		}
  *  		}
  * },
  * }
  */

  private val db = DB.getInstance(ignite)
  /**
   * Retrieve configuration for a certain
   * remote osquery node
   */
  def nodeConfig(uuid:String):JsonObject = {

    val config:String = db.readConfigByNode(uuid)

    if (config == null || config.isEmpty)
     return null

    try {

     val jConfig = JsonParser.parseString(config)
     jConfig.getAsJsonObject

    } catch {
     case _:Throwable => null
    }

  }

  /**
   * This method queries the node database table and retrieves
   * a node instance that refers to the provided node key
   */
  def nodeByKey(key:String):OsqueryNode =
    db.readNodeByKey(key)
  /**
   * The `secret` is provided by a certain osquery node, which must
   * be deployed in a previous step. The `secret` is used to identity
   * a specific node and provide a pre-computed `nodeKey`.
   *
   * This method supports enrollment requests
   */
  def nodeBySecret(secret:String):OsqueryNode =
    db.readNodeBySecret(secret)
  /**
   * Retrieve all queries of status `NEW` assigned
   * to this node; this request is delegated to the
   * RedisDB
   */
  def nodeNewQueries(uuid:String):JsonObject =
    db.getNewQueries(uuid)

   def nodeUpdate(node:OsqueryNode):Unit = {
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
     val values = Seq(
      node.uuid,
      node.timestamp,
      node.active,
      node.enrolled,
      node.secret,
      node.nodeKey,
      node.hostIdentifier,
      node.lastCheckIn,
      node.lastAddress)

     db.createOrUpdateNode(values)

   }
  /**
   * The unique identifier of the distributed query
   * task is sent to the remote node and used to
   * identify this task
   *
   * uuid: unique identifier of a certain
   * query task
   *
   * This method is used by the `WriteActor`
   */
  def taskById(uuid:String):OsqueryQueryTask = db.readTaskById(uuid)

  def updateTask(task:OsqueryQueryTask):Unit = {
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

    db.createOrUpdateTask(values)

  }

}
