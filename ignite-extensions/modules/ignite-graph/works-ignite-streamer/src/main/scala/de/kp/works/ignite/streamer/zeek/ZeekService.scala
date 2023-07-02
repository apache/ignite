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

import de.kp.works.ignite.conf.WorksConf
import de.kp.works.ignite.file.FileEventHandler

class ZeekService {

  private var eventHandler:Option[FileEventHandler] = None
  /**
   * Specify the callback to be used by this service
   * to send Zeek log events to the respective Ignite
   * cache.
   *
   * The current implementation leverages the Zeek
   * Streamer as callback
   */
  def setEventHandler(handler:FileEventHandler):ZeekService = {
    this.eventHandler = Some(handler)
    this
  }

  def start():Unit = {

    if (eventHandler.isEmpty)
      throw new Exception("[ZeekService] No callback specified to send events to.")

    val receiverCfg = WorksConf.getReceiverCfg(WorksConf.ZEEK_CONF)
    val zeekFolder = receiverCfg.getString("zeekFolder")

    val numThreads = receiverCfg.getInt("numThreads")

    val receiver = new ZeekReceiver(zeekFolder, eventHandler.get, numThreads)
    receiver.start()

  }

  def stop():Unit = {}

}
