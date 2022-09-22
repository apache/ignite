package de.kp.works.ignite.streamer.opencti
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
import de.kp.works.ignite.sse.SseEvent
import de.kp.works.ignite.streamer.opencti.graph.CTIGraphWriter
import de.kp.works.ignite.streamer.opencti.table.CTITableWriter

class CTIWriter(connect:IgniteConnect) {

  private val ctiCfg = WorksConf.getCfg(WorksConf.OPENCTI_CONF)
  private val writeMode = ctiCfg.getString("writeMode")

  def write(events:Seq[SseEvent]):Unit = {

    writeMode match {
      case "graph" =>
        val writer = new CTIGraphWriter(connect)
        writer.write(events)
      case "table" =>
        val writer = new CTITableWriter(connect)
        writer.write(events)
      case _ =>
        throw new Exception(s"The configured writeMode `$writeMode` is not supported.")
    }

  }

}
