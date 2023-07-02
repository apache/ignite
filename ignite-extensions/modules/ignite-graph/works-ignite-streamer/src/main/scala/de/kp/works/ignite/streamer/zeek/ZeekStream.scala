package de.kp.works.ignite.streamer.zeek

import de.kp.works.ignite.conf.WorksConf
import de.kp.works.ignite.core.BaseStream

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

object ZeekStream extends BaseStream {

  override var channel: String = WorksConf.ZEEK_CONF

  override var programName: String = "ZeekStream"
  override var programDesc: String = "Ignite streaming support for Zeek log events."

  override def launch(args: Array[String]): Unit = {

    /* Command line argument parser */
    val parser = buildParser()

    /* Parse the argument and then run */
    parser.parse(args, CliConfig()).foreach { c =>

      try {

        igniteConnect = Some(buildIgniteConnect(c, channel))
        /*
         * Build streaming context and finally start the
         * service that listens to Zeek log events.
         */
        val zeekEngine = new ZeekEngine(igniteConnect.get)
        igniteStream = zeekEngine.buildStream

        start()

        println("[INFO] -------------------------------------------------")
        println(s"[INFO] $programName started.")
        println("[INFO] -------------------------------------------------")

      } catch {
        case t: Throwable =>
          t.printStackTrace()
          println("[ERROR] --------------------------------------------------")
          println(s"[ERROR] $programName cannot be started: " + t.getMessage)
          println("[ERROR] --------------------------------------------------")
          /*
           * Sleep for 10 seconds so that one may see error messages
           * in Yarn clusters where logs are not stored.
           */
          Thread.sleep(10000)
          sys.exit(1)

      }
    }

  }


}
