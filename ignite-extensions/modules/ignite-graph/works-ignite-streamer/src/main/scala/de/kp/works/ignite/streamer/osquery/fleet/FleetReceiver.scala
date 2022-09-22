package de.kp.works.ignite.streamer.osquery.fleet
/*
 * Copyright (c) 20129 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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
import de.kp.works.ignite.file.{FileEventHandler, FileMonitor}

import java.util.concurrent.Executors

class FleetReceiver(
  fleetFolder: String,
  eventHandler: FileEventHandler,
  numThreads:Int = 1) {

  private val executorService = Executors.newFixedThreadPool(numThreads)

  def start():Unit = {
    /*
     * Wrap connector and output handler in a runnable
     */
    val worker = new Runnable {
      /*
       * File Monitor to listen to log file
       * changes of a FleetDM platform
       */
      private val connector = new FileMonitor(WorksConf.FLEETDM_CONF, fleetFolder, eventHandler)

      override def run(): Unit = {

        val now = new java.util.Date().toString
        println(s"[FleetReceiver] $now - Receiver worker started.")

        connector.start()

      }
    }

    try {

      /* Initiate stream execution */
      executorService.execute(worker)

    } catch {
      case _:Exception => executorService.shutdown()
    }

  }

  def stop():Unit = {

    /* Stop listening to the Zeek log events stream  */
    executorService.shutdown()
    executorService.shutdownNow()

  }

}
