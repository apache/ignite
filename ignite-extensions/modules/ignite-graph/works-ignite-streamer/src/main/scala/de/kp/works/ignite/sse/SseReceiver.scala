package de.kp.works.ignite.sse

/**
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

import de.kp.works.ignite.ssl.SslOptions
import java.util.concurrent.Executors

/**
 * This [SseReceiver] actually supports SSE events from OpenCTI
 * and Works & Sensor Beats. The receiver leverages an SSE client
 * to connect to the exposed SSE endpoint and listens to the SSE
 * event stream.
 *
 * OpenCTI
 * -------
 * OpenCTI is currently using REDIS Stream as its technical layer.
 * Each time data is modified in the OpenCTI database, a specific
 * event is added in the stream.
 *
 * In order to provides a really easy consuming protocol OpenCTI
 * provides an SSE endpoint.
 *
 * Every user with respective access rights can open and access
 *
 * http(s)://[host]:[port]/stream]
 *
 * and open an SSE connection to start receiving live events.
 *
 * Works & Sensor Beats
 * --------------------
 *
 * In order to provides a really easy consuming protocol Works &
 * Sensor Beats provide an SSE endpoint.
 *
 * Every user with respective access rights can open and access
 *
 * http(s)://[host]:[port]/{beat|sensor}/stream]
 *
 * and open an SSE connection to start receiving live events.
 *
 */
class SseReceiver(
   /*
    * The endpoint of the SSE server
    */
   endpoint:String,
   /*
    * The callback used to send events to
    */
   eventHandler: SseEventHandler,
   /*
    * The (optional) authorization token
    * to access the SSE server
    */
   authToken:Option[String] = None,
   /*
    * The optional SSL configuration to
    * access a secure SSE server
    */
   sslOptions:Option[SslOptions] = None,
   /* The number of threads to use for processing */
   numThreads:Int = 1) {

  private val executorService = Executors.newFixedThreadPool(numThreads)

  def start():Unit = {
    /*
     * Wrap connector and output handler in a runnable
     */
    val worker = new Runnable {
      /*
       * Initialize the connector to the SSE server
       */
      private val connector = new SseConnector(endpoint, eventHandler, authToken, sslOptions)

      override def run(): Unit = {

        val now = new java.util.Date().toString
        println(s"[SseReceiver] $now - Receiver worker started.")

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

    /* Stop listening to the SSE events stream  */
    executorService.shutdown()
    executorService.shutdownNow()

  }

}
