package de.kp.works.ignite.streamer.fiware
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
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ContentTypes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import de.kp.works.ignite.conf.WorksConf

import scala.concurrent.Future
/**
 * The FiwareClient is used to connect to the Fiware
 * Context Broker and subscribe to certain events.
 *
 * The Fiware implementation requires a (public)
 * endpoint, the Fiware Context Broker can send its
 * notifications to.
 */
object FiwareClient {
  /*
   * {
   *		"description": "A subscription to get info about Room1",
   *  	"subject": {
   *    "entities": [
   *      {
   *        "id": "Room1",
   *        "type": "Room"
   *      }
   *    ],
   *    "condition": {
   *      "attrs": [
   *        "pressure"
   *      ]
   *    }
   *  },
   *  "notification": {
   *    "http": {
   *      "url": "http://localhost:9080/notifications"
   *    },
   *    "attrs": [
   *      "temperature"
   *    ]
   *  },
   *  "expires": "2040-01-01T14:00:00.00Z",
   *  "throttling": 5
   * }
   *
   */
  def subscribe(subscription:String, service:Option[String], servicePath:Option[String], system:ActorSystem):Future[HttpResponse] = {

    try {

      val entity = subscription.getBytes("UTF-8")
      /*
       * Build request: A subscription is registered with a POST request
       * to /v2/subscriptions
       */
      val brokerUrl = WorksConf.getFiwareBrokerUrl
      val endpoint = s"$brokerUrl/v2/subscriptions"

      /* __MOD__ Content type changed to `application/json` */

     var headers = Seq(RawHeader("Content-Type", "application/json"))
      if (service.nonEmpty)
        headers = headers ++ Seq(RawHeader("fiware-service", service.get))

      if (servicePath.nonEmpty)
        headers = headers ++ Seq(RawHeader("fiware-servicepath", servicePath.get))

      val request = HttpRequest(
        HttpMethods.POST,
        endpoint,
        headers = headers.toList,
        entity = HttpEntity(`application/json`, entity))

      val response: Future[HttpResponse] = {

        if (!FiwareSsl.isFiwareSsl)
         /*
          * The request protocol in the broker url must be
          * specified as 'http://'
          */
          Http(system).singleRequest(request)

        else {
          /*
           * The request protocol in the broker url must be
           * specified as 'https://'. In this case, an SSL
           * security context must be specified
           */
          val context = FiwareSsl.buildBrokerContext
          Http(system).singleRequest(request = request, connectionContext = context)

        }
      }
      response

    } catch {
      case _:Throwable => null
    }

  }

  /*
   * This method validates that the response code of the Orion Context
   * Broker response is 201 (Created) and then the subscription ID is
   * extracted from the provided 'Location' header
   */
  def getSubscriptionId(response:HttpResponse):String = {

    var sid:Option[String] = None

    val statusCode = response._1
    if (statusCode == StatusCodes.Created) {
      /*
       * The Orion Context Broker responds with a 201 Created response
       * code; the subscription identifier is provided through the
       * Location Header
       */
      val headers = response._2

      headers.foreach(header => {
        /* Akka HTTP requires header in lower cases */
        if (header.is("location")) {
          /*
           * Location: /v2/subscriptions/57458eb60962ef754e7c0998
           *
           * Subscription ID: a 24 digit hexadecimal number used
           * for updating and cancelling the subscription. It is
           * used to identify the notifications that refer to this
           * subscription
           */
          sid = Some(header.value().replace("/v2/subscriptions/",""))
        }

      })
    }

    if (sid.isEmpty)
      throw new Exception("Orion Context Broker did not respond with a subscription response.")

    sid.get

  }

}
