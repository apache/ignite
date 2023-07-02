package de.kp.works.ignite.streamer.zeek
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
import de.kp.works.ignite.transform.zeek.{ZeekFormatUtil, ZeekFormats}

trait BaseTransformer {

  protected def groupEvents(events: Seq[FileEvent]): Seq[(ZeekFormats.Value, Seq[JsonElement])] = {
    /*
     * Collect Zeek log events with respect to their
     * eventType (which refers to the name of the log
     * file)
     */
    events
      /*
       * Convert `eventType` (file name) into Zeek format
       * and deserialize event data. Restrict to those
       * formats that are support by the current version.
       */
      .map(event =>
        (ZeekFormatUtil.fromFile(event.eventType), JsonParser.parseString(event.eventData))
      )
      .filter { case (format, _) => format != null }
      /*
       * Group logs by format and prepare format specific
       * log processing
       */
      .groupBy { case (format, _) => format }
      .map { case (format, logs) => (format, logs.map(_._2)) }
      .toSeq

  }
}
