package de.kp.works.ignite.streamer.osquery.tls

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

import com.google.gson.{JsonElement, JsonParser}
import de.kp.works.ignite.file.FileEvent
import de.kp.works.ignite.transform.fleet.{FleetFormatUtil, FleetFormats}

trait BaseTransformer {

  def groupEvents(events: Seq[TLSEvent]): Seq[(String, Seq[JsonElement])] = {

    events
      .map(event => {
        /*
         * The event type has the following
         * format
         *     tls/table
         */
        val eventType = event.eventType
        val table = eventType.split("\\/")(1)

        (table, JsonParser.parseString(event.eventData))

      })
      /*
       * Group logs by table and prepare format specific
       * log processing
       */
      .groupBy { case (table, _) => table }
      .map { case (table, logs) => (table, logs.map(_._2)) }
      .toSeq

  }

}
