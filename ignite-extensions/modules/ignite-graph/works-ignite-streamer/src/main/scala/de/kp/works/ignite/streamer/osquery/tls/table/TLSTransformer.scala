package de.kp.works.ignite.streamer.osquery.tls.table
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

import de.kp.works.ignite.conf.WorksConf
import de.kp.works.ignite.json.JsonUtil
import de.kp.works.ignite.streamer.osquery.tls.{BaseTransformer, TLSEvent}
import de.kp.works.ignite.transform.tls.{TLSSchema, TLSTables}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object TLSTransformer extends BaseTransformer {

  private val tlsCfg = WorksConf.getCfg(WorksConf.OSQUERY_CONF)
  private val tlsKey = tlsCfg.getString("primaryKey")

  private val primaryKey = StructField(tlsKey, StringType, nullable = false)

  private val X_WORKS_ACTION    = "x_works_action"
  private val X_WORKS_HOSTNAME  = "x_works_hostname"
  private val X_WORKS_NAME      = "x-works_name"
  private val X_WORKS_TIMESTAMP = "x_works_timestamp"

  def transform(events: Seq[TLSEvent]): Seq[(String, StructType, Seq[Row])] = {

    try {
      /*
       * STEP #1: Collect TLS log events with respect
       * to their eventType (which refers to the name
       * of the osquery table)
       */
      val data = groupEvents(events)
      /*
       * STEP #2: Transform logs for each table individually
       */
      data.map{case(table, batch) => {

        if (TLSTables.fromTable(table) == null)
          throw new Exception(s"Unknown Osquery table detected.")

        val schema =
          if (table == TLSTables.OSQUERY_STATUS.toString)
            status_table()
          else
            result_table(table)

        val rows = batch.map(batchElem => {
          val batchObj = batchElem.getAsJsonObject
          JsonUtil.json2Row(batchObj, schema)
        })

        (table, schema, rows)

      }}

    } catch {
      case _: Throwable => Seq.empty[(String, StructType, Seq[Row])]
    }

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
    val methods = TLSSchema.getClass.getMethods

    val method = methods.filter(m => m.getName == table).head
    val schema = method.invoke(TLSSchema).asInstanceOf[StructType]

    fields = Array(primaryKey) ++ fields ++ schema.fields
    StructType(fields)

  }

  def status_table(): StructType = {
    val fields = Array(primaryKey) ++ TLSSchema.osquery_status().fields
    StructType(fields)
  }

}
