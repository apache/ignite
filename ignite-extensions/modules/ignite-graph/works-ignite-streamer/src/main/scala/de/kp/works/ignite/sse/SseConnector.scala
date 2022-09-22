package de.kp.works.ignite.sse

/**
 * Copyright (c) 2020 Dr. Krusche & Partner PartG. All rights reserved.
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
import okhttp3.Response
import okhttp3.sse.{EventSource, EventSourceListener, EventSources}

/**
 * The [SseConnector] connects & listens to the event
 * stream of a certain Works or Sensor Beat via an SSE
 * client and write these events to the provided output
 * handler.
 */
class SseConnector(
  endpoint: String,
  callback: SseEventHandler,
  authToken: Option[String] = None,
  sslOptions: Option[SslOptions] = None) {

  def stop(): Unit = {
    /* Do nothing */
  }

  def start() {

    val sseClient = new SseClient(endpoint, authToken, sslOptions)

    val request = sseClient.getRequest
    val httpClient = sseClient.getHttpClient

    /** SSE **/

    val factory = EventSources.createFactory(httpClient)
    val listener = new EventSourceListener() {

      override def onOpen(eventSource:EventSource, response:Response):Unit = {
        /* Do nothing */
      }

      override def onEvent(eventSource:EventSource, eventId:String, eventType:String, eventData:String):Unit = {
        callback.eventArrived(SseEvent(eventId, eventType, eventData))
      }

      override def onClosed(eventSource:EventSource) {
      }

      override def onFailure(eventSource:EventSource, t:Throwable, response:Response) {
        /* Restart the receiver in case of an error */
        restart(t)
      }

    }

    factory.newEventSource(request, listener)

  }

  def restart(t:Throwable): Unit = {

    val now = new java.util.Date().toString
    println(s"[SseConnector] $now - Restart due to: ${t.getLocalizedMessage}")

    start()

  }
}
