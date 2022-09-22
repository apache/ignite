package de.kp.works.ignite.streamer.osquery.fleet.table
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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

import com.google.gson.{JsonElement, JsonObject}
import de.kp.works.ignite.conf.WorksConf
import de.kp.works.ignite.json.JsonUtil
import de.kp.works.ignite.streamer.osquery.OsqueryConstants
import de.kp.works.ignite.streamer.osquery.schema.Schema
import de.kp.works.ignite.transform.fleet.FleetSchema
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.text.SimpleDateFormat
import scala.collection.JavaConversions._

object FleetUtil {

  private val fleetCfg = WorksConf.getCfg(WorksConf.FLEETDM_CONF)
  private val fleetKey = fleetCfg.getString("primaryKey")

  private val primaryKey = StructField(fleetKey, StringType, nullable = false)

  private val X_WORKS_ACTION    = "x_works_action"
  private val X_WORKS_HOSTNAME  = "x_works_hostname"
  private val X_WORKS_NAME      = "x-works_name"
  private val X_WORKS_TIMESTAMP = "x_works_timestamp"

  def fromResult(logs: Seq[JsonElement]): Seq[(String, StructType, Seq[Row])] = {

    logs
      /*
       * A certain log result refers to a predefined query (configuration or pack).
       * Each query addresses a certain Osquery table (name) and a set of configured
       * columns. This implementation expects that a specific table always occurs
       * with the same (predefined) columns (see schema).
       *
       * A sequence of logs, streamed from the filesystem, can contain query or table
       * results that refer to different tables. Therefore, the result is grouped by
       * the table name.
       *
       * Finally, the table name is also used as the name of the respective Apache
       * Ignite cache.
       */
      .map(log => fromResult(log.getAsJsonObject))
      /*
       * Group the transformed results with respect to the query or table name
       */
      .groupBy { case (name, _, _) => name }
      .map { case (name, values) =>
        val schema = values.head._2
        val rows = values.flatMap(_._3)

        (name, schema, rows)
      }
      .toSeq

  }

  /**
   * This method accepts that each log object provided
   * is different with respect to its data format
   */
  def fromResult(oldObj: JsonObject): (String, StructType, Seq[Row]) = {
    /*
     * STEP #1: Normalize log events with formats,
     * event, batch and snapshot into a single JSON
     * format representation
     */
    val commonObj = new JsonObject
    /*
     * Extract `name` (of the query) and normalize `calendarTime`:
     *
     * "calendarTime": "Tue Sep 30 17:37:30 2014"
     *
     * - Weekday as locale’s abbreviated name
     *
     * - Month as locale’s abbreviated name
     *
     * - Day of the month as a zero-padded decimal number
     *
     * - H:M:S
     *
     * - Year with century as a decimal number
     *
     * UTC
     */
    val name = oldObj.get(OsqueryConstants.NAME).getAsString
    commonObj.addProperty(X_WORKS_NAME, name)

    val calendarTime = oldObj.get(OsqueryConstants.CALENDAR_TIME).getAsString
    val datetime = transformCalTime(calendarTime)

    val timestamp = datetime.getTime
    commonObj.addProperty(X_WORKS_TIMESTAMP, timestamp)

    val hostname = getHostname(oldObj)
    commonObj.addProperty(X_WORKS_HOSTNAME, hostname)
    /*
     * In this case, `event format`, columns is a single
     * object that must be added to the overall output
     *
     * {
     *  "action": "added",
     *  "columns": {
     *    "name": "osqueryd",
     *    "path": "/opt/osquery/bin/osqueryd",
     *    "pid": "97830"
     *  },
     *  "name": "processes",
     *  "hostname": "hostname.local",
     *  "calendarTime": "Tue Sep 30 17:37:30 2014",
     *  "unixTime": "1412123850",
     *  "epoch": "314159265",
     *  "counter": "1",
     *  "numerics": false
     * }
     */
    val rowObjs = if (oldObj.get(OsqueryConstants.COLUMNS) != null) {

      val action = oldObj.get(OsqueryConstants.ACTION).getAsString

      val rowObj = commonObj
      rowObj.addProperty(X_WORKS_ACTION, action)
      /*
       * Extract log event specific format and thereby
       * assume that the columns provided are the result
       * of an offline configuration process.
       *
       * NOTE: the mechanism below does not work for adhoc
       * (distributed) queries.
       */
      val columns = oldObj.get(OsqueryConstants.COLUMNS).getAsJsonObject
      /*
       * Extract the column names in ascending order to
       * enable a correct match with the schema definition
       */
      val colnames = columns.keySet.toSeq.sorted
      colnames.foreach(colName => rowObj.add(colName, columns.get(colName)))
      /*
      val schema = result(colnames)
      val row = JsonUtil.json2Row(rowObject, schema)

      (schema, Seq(row))
      */
      Seq(rowObj)
    }
    /*
     * If a query identifies multiple state changes, the batched format
     * will include all results in a single log line.
     *
     * If you're programmatically parsing lines and loading them into a
     * backend datastore, this is probably the best solution.
     *  {
     *    "diffResults": {
     *       "added": [
     *         {
     *           "name": "osqueryd",
     *           "path": "/opt/osquery/bin/osqueryd",
     *           "pid": "97830"
     *         }
     *       ],
     *       "removed": [
     *         {
     *           "name": "osqueryd",
     *           "path": "/opt/osquery/bin/osqueryd",
     *           "pid": "97650"
     *         }
     *       ]
     *    },
     *    "name": "processes",
     *    "hostname": "hostname.local",
     *    "calendarTime": "Tue Sep 30 17:37:30 2014",
     *    "unixTime": "1412123850",
     *    "epoch": "314159265",
     *    "counter": "1",
     *    "numerics": false
     *  }
     */
    else if (oldObj.get(OsqueryConstants.DIFF_RESULTS) != null) {

      val diffResults = oldObj.get(OsqueryConstants.DIFF_RESULTS).getAsJsonObject
      /*
       * The subsequent transformation assumes that the columns
       * specified in the query result, independent of the data
       * action, is always the same.
       */
      val actions = diffResults.keySet().toSeq.sorted
      actions.flatMap(action => {

        val data = diffResults.get(action).getAsJsonArray
        data.map(columns => {

          val rowObj = commonObj
          rowObj.addProperty(X_WORKS_ACTION, action)

          val colnames = columns.getAsJsonObject.keySet.toSeq.sorted
          colnames.foreach(colName => rowObj.add(colName, columns.getAsJsonObject.get(colName)))

          rowObj

        })

      })

    }
    /*
     * Snapshot queries attempt to mimic the differential event format,
     * instead of emitting "columns", the snapshot data is stored using
     * "snapshot".
     *
     *  {
     *    "action": "snapshot",
     *    "snapshot": [
     *      {
     *        "parent": "0",
     *         "path": "/sbin/launchd",
     *        "pid": "1"
     *      },
     *      {
     *        "parent": "1",
     *        "path": "/usr/sbin/syslogd",
     *        "pid": "51"
     *      },
     *      {
     *        "parent": "1",
     *        "path": "/usr/libexec/UserEventAgent",
     *        "pid": "52"
     *      },
     *      {
     *        "parent": "1",
     *        "path": "/usr/libexec/kextd",
     *        "pid": "54"
     *      }
     *    ],
     *    "name": "process_snapshot",
     *    "hostIdentifier": "hostname.local",
     *    "calendarTime": "Mon May  2 22:27:32 2016 UTC",
     *    "unixTime": "1462228052",
     *    "epoch": "314159265",
     *    "counter": "1",
     *    "numerics": false
     *  }
     */
    else if (oldObj.get(OsqueryConstants.SNAPSHOT) != null) {

      val data = oldObj.get(OsqueryConstants.SNAPSHOT).getAsJsonArray
      /*
       * The subsequent transformation assumes that the columns
       * specified in the query result, independent of the data
       * action, is always the same.
       */
      data.map(columns => {

        val rowObj = commonObj
        rowObj.addProperty(X_WORKS_ACTION, OsqueryConstants.SNAPSHOT)
        /**
         * The column names are sorted in alphabetical
         * ascending order to ensure, that the schema
         * field order is correct for all entries.
         */
        val colnames = columns.getAsJsonObject.keySet.toSeq.sorted
        colnames.foreach(colName => rowObj.add(colName, columns.getAsJsonObject.get(colName)))

        rowObj

      }).toSeq

    }
    else {
      throw new Exception("Query result encountered that cannot be transformed.")
    }
    /*
     * STEP #2: Determine schema that refers to the
     * extract JSON event objects and transform into
     * Apache Spark compliant rows
     */
    val table = rowObjs.head.get(OsqueryConstants.NAME).getAsString
    val schema = result_table(table)

    val rows = rowObjs.map(rowObj => JsonUtil.json2Row(rowObj, schema))
    (table, schema, rows)

  }

  def result_table(table:String): StructType = {
    /*
     * COMMON FIELDS
     *
     * In order to avoid naming conflicts with Osquery based column
     * names, these metadata columns are prefixed by `x_works_`.
     */
    var fields = Array(
      /* The `name` (of the query) */
      StructField(X_WORKS_NAME, StringType, nullable = false),
      /* The timestamp of the event */
      StructField(X_WORKS_TIMESTAMP, LongType, nullable = false),
      /* The hostname of the event */
      StructField(X_WORKS_HOSTNAME, StringType, nullable = false),
      /* The action of the event */
      StructField(X_WORKS_ACTION, StringType, nullable = false)
    )
    /*
     * REQUEST SPECIFIC FIELDS
     *
     * The remaining schema fields, that describe the columns
     * associated with a result log event are retrieved via
     * method invocation
     */
    val methods = FleetSchema.getClass.getMethods

    val method = methods.filter(m => m.getName == table).head
    val schema = method.invoke(FleetSchema).asInstanceOf[StructType]

    fields = Array(primaryKey) ++ fields ++ schema.fields
    StructType(fields)

  }

  /**
   * Osquery creates status logs of its own execution, for log levels
   * INFO, WARNING and ERROR. Note, this implementation expects a single
   * log file that contains status messages of all levels.
   */
  def fromStatus(logs: Seq[JsonElement]): Seq[(String, StructType, Seq[Row])] = {
    logs
      .map(log => fromStatus(log.getAsJsonObject))
      /*
       * Group the transformed results with respect
       * to the table name
       */
      .groupBy { case (name, _, _) => name }
      .map { case (name, values) =>
        val schema = values.head._2
        val rows = values.flatMap(_._3)
        (name, schema, rows)
      }
      .toSeq

  }

  def fromStatus(oldObject: JsonObject): (String, StructType, Seq[Row]) = {

    val rowObject = new JsonObject
    rowObject.addProperty(OsqueryConstants.MESSAGE, oldObject.toString)

    val schema = status_table()
    val row = JsonUtil.json2Row(rowObject, schema)

    ("osquery_status", schema, Seq(row))

  }

  def status_table(): StructType = {
    val fields = Array(primaryKey) ++ FleetSchema.osquery_status().fields
    StructType(fields)
  }

  /** HELPER METHODS * */

  private def getHostname(oldObject: JsonObject): String = {
    /*
     * The host name is represented by different
     * keys within the different result logs
     */
    val keys = Array(
      OsqueryConstants.HOST,
      OsqueryConstants.HOST_IDENTIFIER,
      OsqueryConstants.HOSTNAME)

    try {

      val key = oldObject.keySet()
        .filter(k => keys.contains(k))
        .head

      oldObject.get(key)
        .getAsString

    } catch {
      case _: Throwable => ""
    }
  }

  private def transformCalTime(s: String): java.util.Date = {

    try {

      /* Osquery use UTC to describe datetime */
      val pattern = if (s.endsWith("UTC")) {
        "EEE MMM dd hh:mm:ss yyyy z"
      }
      else
        "EEE MMM dd hh:mm:ss yyyy"

      val format = new SimpleDateFormat(pattern, java.util.Locale.US)
      format.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))

      format.parse(s)

    } catch {
      case _: Throwable => null
    }

  }

}
