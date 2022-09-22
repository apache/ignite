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

import com.google.gson.{JsonObject, JsonParser}
import de.kp.works.ignite.sse.SseEvent
import de.kp.works.ignite.streamer.beat.enums.Beats.Beat
import de.kp.works.ignite.streamer.beat.enums.Sensors.Sensor
import de.kp.works.ignite.streamer.beat.enums.{Beats, Sensors}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * The [BeatTransformer] is implemented to transform
 * Beat SSE events of different event types.
 */
object BeatTransformer {

  def transform(sseEvents: Seq[SseEvent]): Seq[(String, StructType, Seq[Row])] = {
    /*
     * SSE events that originate from Works or Sensor
     * Beats have the same internal data format:
     *
     * {
     *    type: "..."
     *    event: "..."
     * }
     *
     * The Ignite streaming support for *Beats supports
     * multiple and different event types.
     */

    try {
      /*
       * STEP #1: Reorganize the provided SSE events
       * with respect to their content-specific event
       * type
       */
      val input = sseEvents.map(sseEvent => {
        /*
         * Transform SSE data into a JSON object
         * and extract the respective fields
         */
        val json = JsonParser
          .parseString(sseEvent.data)
          .getAsJsonObject

        val eventType = json.get("type").getAsString
        val eventObj = JsonParser
          .parseString(json.get("event").getAsString)
          .getAsJsonObject

        (eventType, eventObj)
      })
      .groupBy{case(eventType, _) => eventType}
      .map{case (eventType, eventGroup) =>
        val eventObjs = eventGroup.map{case(_, eventObj) => eventObj}
        (eventType, eventObjs)
      }
      /*
       * STEP #2: Transform Beat-specific events into
       * Apache Spark SQL compliant [Row]s
       */
      input.map{case(eventType, eventObjs) =>

        val tokens = eventType.split("\\/")

        val (schema, rows) = if (tokens(0).toLowerCase == "beat") {
          transformBeat(Beats.withName(tokens(1)), eventObjs)

        } else if (eventType.toLowerCase.startsWith("sensor")) {
          transformSensor(Sensors.withName(tokens(1)), eventObjs)

        } else
          throw new Exception(s"Unknown event type `$eventType` detected.")
        /*
         * Transform `eventType` into table name
         */
        val table = eventType.replace("\\/", "_")
        (table, schema, rows)

      }.toSeq

    } catch {
      case _: Throwable => Seq.empty[(String, StructType, Seq[Row])]
    }

  }

  private def transformBeat(beat:Beat, eventObjs: Seq[JsonObject]): (StructType, Seq[Row]) = {

    beat match {
      case Beats.FIWARE =>

      case Beats.FLEET =>

      case Beats.OPCUA =>

      case Beats.OPENCTI =>

      case Beats.TLS =>

      case Beats.THINGS =>

      case Beats.ZEEK =>

      case _ =>
        throw new Exception(s"The provided Works Beat is not supported.")

    }

    ???
  }

  private def transformSensor(sensor:Sensor, eventObjs: Seq[JsonObject]): (StructType, Seq[Row]) = {

    sensor match {
      case Sensors.MILESIGHT =>

      case _ =>
        throw new Exception(s"The provided Sensor Beat is not supported.")
    }
    ???
  }

}
