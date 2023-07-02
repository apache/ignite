package de.kp.works.ignite.streamer.osquery.fleet.graph
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

import de.kp.works.ignite.mutate.IgniteMutation
import de.kp.works.ignite.file.FileEvent
import de.kp.works.ignite.streamer.osquery.fleet.BaseTransformer

object FleetTransformer extends BaseTransformer{
  /**
   * REMINDER: Extracting nodes & edges from event logs
   * requires a suitable configuration of the timing
   * windows and intervals associated with the temporary
   * Ignite cache.
   */
  def transform(events: Seq[FileEvent]): (Seq[IgniteMutation], Seq[IgniteMutation]) = {

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

      ???

    } catch {
      case _: Throwable => (Seq.empty[IgniteMutation], Seq.empty[IgniteMutation])
    }
  }

}
