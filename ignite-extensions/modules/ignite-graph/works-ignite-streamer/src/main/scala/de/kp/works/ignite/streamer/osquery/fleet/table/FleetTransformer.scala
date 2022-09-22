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
import de.kp.works.ignite.file.FileEvent
import de.kp.works.ignite.streamer.osquery.fleet.BaseTransformer
import de.kp.works.ignite.transform.fleet.FleetFormats.{RESULT, STATUS}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

object FleetTransformer extends BaseTransformer {

  def transform(events: Seq[FileEvent]): Seq[(String, StructType, Seq[Row])] = {

    try {
      /*
       * STEP #1: Collect Fleet log events with respect
       * to their eventType (which refers to the name
       * of the log file)
       */
      val data = groupEvents(events)
      /*
       * STEP #2: Transform logs for each format individually
       */
      data.flatMap { case (format, logs) =>

        format match {
          case RESULT =>
            /*
             * The result `log` events are described with
             * respect to the query name.
             *
             * Note, this streamer expects that each query
             * name is equivalent to an Osquery table.
             */
            FleetUtil.fromResult(logs)

          case STATUS =>
            FleetUtil.fromStatus(logs)

          case _ => throw new Exception(s"[FleetTransformer] Unknown format `$format.toString` detected.")
        }
      }

    } catch {
      case _: Throwable => Seq.empty[(String, StructType, Seq[Row])]
    }

  }

}
