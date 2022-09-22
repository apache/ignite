package de.kp.works.ignite.streamer.beat.table

/**
 * Copyright (c) 2019 - 2022 Dr. Krusche & Partner PartG. All rights reserved.
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

import de.kp.works.ignite.IgniteConnect
import de.kp.works.ignite.conf.WorksConf
import de.kp.works.ignite.core.Session
import de.kp.works.ignite.sse.SseEvent
import de.kp.works.ignite.writer.TableWriter
import org.apache.spark.sql.SaveMode

class BeatTableWriter(connect:IgniteConnect) extends TableWriter(connect) {

  private val beatCfg = WorksConf.getCfg(WorksConf.BEAT_CONF)

  private val primaryKey = beatCfg.getString("primaryKey")
  private val tableParameters = beatCfg.getString("tableParameters")

  def write(sseEvents: Seq[SseEvent]):Unit = {

    try {
      /*
       * STEP #1: Retrieve Spark session and make sure
       * that an Apache Ignite node is started
       */
      val session = Session.getSession
      /*
       * STEP #2: Transform the Beat SSE events into an
       * Apache Spark compliant format
       */
      val transformed = BeatTransformer.transform(sseEvents)
      /*
       * STEP #3: Write logs that refer to a certain SSE event
       * type to individual Apache Ignite caches
       */
      transformed.foreach{case(table, schema, rows) =>
        if (rows.nonEmpty) {
          val dataframe = session.createDataFrame(session.sparkContext.parallelize(rows), schema)
          /*
           * Beat SSE events ship with ArrayType(LongType|StringType) fields,
           * but Apache Ignite currently does not support this data type.
           *
           * See: https://issues.apache.org/jira/browse/IGNITE-9229
           *
           * Therefore, we intercept the generated dataframe here and serialize
           * all ArrayType fields before writing to Apache Ignite
           */
          save(table, primaryKey, tableParameters, dataframe, SaveMode.Append)
        }
      }

    } catch {
      case _:Throwable => /* Do nothing */
    }

  }

}
