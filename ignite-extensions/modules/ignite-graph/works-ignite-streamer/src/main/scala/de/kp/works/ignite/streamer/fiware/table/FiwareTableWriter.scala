package de.kp.works.ignite.streamer.fiware.table
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

import de.kp.works.ignite.IgniteConnect
import de.kp.works.ignite.conf.WorksConf
import de.kp.works.ignite.core.Session
import de.kp.works.ignite.streamer.fiware.FiwareEvent
import de.kp.works.ignite.writer.TableWriter
import org.apache.spark.sql.SaveMode

class FiwareTableWriter(connect:IgniteConnect) extends TableWriter(connect) {

  private val fiwareCfg = WorksConf.getCfg(WorksConf.FIWARE_CONF)

  private val primaryKey = fiwareCfg.getString("primaryKey")
  private val tableParameters = fiwareCfg.getString("tableParameters")

  def write(events:Seq[FiwareEvent]):Unit = {

    try {
      /*
       * STEP #1: Retrieve Spark session and make sure
       * that an Apache Ignite node is started
       */
      val session = Session.getSession
      /*
       * STEP #2: Transform the Fiware notifications into an
       * Apache Spark compliant format
       */
      val (schema, rows) = FiwareTransformer.transform(events)
      /*
       * STEP #3: Write notifications to individual Apache Ignite
       * cache
       */
      val dataframe = session.createDataFrame(session.sparkContext.parallelize(rows), schema)

      val table = "fiware_events"
      save(table, primaryKey, tableParameters, dataframe, SaveMode.Append)

    } catch {
      case _:Throwable => /* Do nothing */
    }

  }

}
