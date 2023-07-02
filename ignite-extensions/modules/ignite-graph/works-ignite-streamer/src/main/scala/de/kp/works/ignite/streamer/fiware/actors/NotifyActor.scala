package de.kp.works.ignite.streamer.fiware.actors

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

import akka.http.scaladsl.model.HttpRequest
import akka.util.ByteString
import com.google.gson.JsonParser
import de.kp.works.ignite.streamer.fiware.{FiwareEvent, FiwareEventHandler, FiwareSubscriptions}

import scala.concurrent.Await
import scala.util.Try
/**
 * The [NotifyActor] is responsible for executing HTTP based
 * notification requests from the Orion Broker. Each notification
 * is checked whether it refers to a registered subscriptions,
 * and, if this is the case, further processing is delegated to
 * the Ignite (Fiware) Streamer
 */
class NotifyActor(callback:FiwareEventHandler) extends BaseActor {
  /**
   * The header positions that hold service and
   * service path information
   */
  val SERVICE_HEADER = 4
  val SERVICE_PATH_HEADER = 5

  override def receive: Receive = {

    case message: HttpRequest =>
      sender ! Response(Try({
        execute(message)
      })
        .recover {
          case e: Exception =>
            throw new Exception(e.getMessage)
        })

  }
  /**
   * Method to transform an Orion Broker notification
   * message into an internal format and to delegate
   * further processing to the provided callback
   */
  private def execute(httpReq: HttpRequest):Unit = {
    /*
     * Convert Http request from Orion Broker
     * into internal notification format
     */
    val notification = toFiwareEvent(httpReq)
    /*
     * Before we continue to delegate the notification
     * to the Ignite streamer, we check whether the
     * notification refers to a registered subscription.
     */
    val json = notification.payload
    /*
     * {
        "data": [
            {
                "id": "Room1",
                "temperature": {
                    "metadata": {},
                    "type": "Float",
                    "value": 28.5
                },
                "type": "Room"
            }
        ],
        "subscriptionId": "57458eb60962ef754e7c0998"
       }
     */
    val sid = json.get("subscriptionId").getAsString
    if (FiwareSubscriptions.isRegistered(sid))
      callback.notificationArrived(notification)

  }
  /**
   * Method to unpack the (incoming) Context Broker notification
   * request and to transform into an internal JSON format for
   * further processing.
   */
  def toFiwareEvent(request:HttpRequest):FiwareEvent = {

    /** HEADERS **/

    val headers = request.headers

    val service = headers(SERVICE_HEADER).value()
    val servicePath = headers(SERVICE_PATH_HEADER).value()

    /** BODY **/

    /* Extract body as String from request entity */

    val source = request.entity.dataBytes
    val future = source.runFold(ByteString(""))(_ ++ _)
    /*
     * We do not expect to retrieve large messages
     * and accept a blocking wait
     */
    val bytes = Await.result(future, timeout.duration)
    val body = bytes.decodeString("UTF-8")

    /* We expect that the Orion Context Broker sends a JSON object */
    val payload = JsonParser.parseString(body).getAsJsonObject
    FiwareEvent(service, servicePath, payload)

  }

}
