package de.kp.works.ignite.streamer.fiware.table

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

import com.google.gson.JsonArray
import de.kp.works.ignite.core.Session
import de.kp.works.ignite.reader.TableReader
import org.apache.ignite.Ignite

class FiwareTableReader(ignite:Ignite) extends TableReader(ignite) {

  private val table = "fiware_events"

  def readEntities(params:Map[String,Any]):JsonArray = {

    try {
      /*
       * STEP #1: Retrieve Spark session and make sure
       * that an Apache Ignite node is started
       */
      val session = Session.getSession
      /*
       * STEP #2: Build SQL statement from provided
       * parameters
       */
      val sql = paramsToSql(params)
      /*
       * STEP #3: Read data from Apache Ignite cache
       */
      val dataframe = read(table, sql, session)

      // TODO
      ???

    } catch {
      case _:Throwable => new JsonArray
    }

  }

  private def paramsToSql(params:Map[String,Any]):String = ???
}
