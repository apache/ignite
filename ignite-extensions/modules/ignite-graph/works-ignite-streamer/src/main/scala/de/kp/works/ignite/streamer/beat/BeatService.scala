package de.kp.works.ignite.streamer.beat

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

import de.kp.works.ignite.conf.WorksConf
import de.kp.works.ignite.sse.{SseEventHandler, SseReceiver}
import de.kp.works.ignite.ssl.SslOptions
/**
 * [BeatService] manages a set of SSE receivers, each
 * for a certain Works & Sensor Beat SSE micro service.
 */
class BeatService {

  private var eventHandler:Option[SseEventHandler] = None
  /**
   * Specify the callback to be used by this service
   * to send Beat SSE events to the respective Ignite
   * cache.
   *
   * The current implementation leverages the Beat
   * Streamer as callback
   */
  def setEventHandler(handler:SseEventHandler):BeatService = {
    this.eventHandler = Some(handler)
    this
  }
  /**
   * This method launches the SSE connection and
   * listens to published SSE
   */
  def start():Unit = {

    if (eventHandler.isEmpty)
      throw new Exception("[BeatService] No callback specified to send events to.")
    /*
     * After having started the Http(s) server,
     * the receivers are started that connect to
     * Beat SSE servers and retrieve SSE events
     *
     * Beat SSE streams (server) --> SseReceiver
     *
     * Each receiver is an SSE client that listens
     * to published events of a certain Works &
     * Sensor Beat.
     */
    val receiversCfg = WorksConf.getReceiversCfg(WorksConf.BEAT_CONF)
    receiversCfg.foreach(receiverCfg => {

      val authToken = {
        val value = receiverCfg.getString("authToken")
        if (value.isEmpty) None else Some(value)
      }

      val endpoint = receiverCfg.getString("endpoint")
      /*
       * Transport security configuration used to
       * establish a Http(s) connection to the server.
       */
      val securityCfg = receiverCfg.getConfig("security")
      val sslOptions = SslOptions.getOptions(securityCfg)
      /*
       * Note, each [SseReceiver] shares the same
       * instance of the event handler (BeatStreamer)
       */
      val numThreads = receiverCfg.getInt("numThreads")
      val receiver = new SseReceiver(
        endpoint,
        eventHandler.get,
        authToken,
        Some(sslOptions),
        numThreads
      )

      receiver.start()

    })

  }

  def stop():Unit = {}

}
