package de.kp.works.ignite.streamer.osquery.tls.actor
/*
 * Copyright (c) 2021 Dr. Krusche & Partner PartG. All rights reserved.
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
import akka.actor.Props
import akka.http.scaladsl.model.HttpRequest
import akka.pattern.ask
import akka.routing.{DefaultResizer, RoundRobinPool}
import de.kp.works.ignite.streamer.osquery.tls.actor.ResultActor._
import de.kp.works.ignite.streamer.osquery.tls.actor.StatusActor._
import de.kp.works.ignite.streamer.osquery.OsqueryConstants
import de.kp.works.ignite.streamer.osquery.tls.TLSEventHandler
import de.kp.works.ignite.streamer.osquery.tls.db.DBApi

import scala.concurrent.Await

class LogActor(api:DBApi, handler:TLSEventHandler) extends BaseActor(api) {
  /**
   * We define a common resizer for all children pools
   */
  private val resizer = new DefaultResizer(lower, upper)
  /**
   * `result`and `status` log messages are processed by
   * individual actors
   */
  private val resultActor = system.actorOf(
    RoundRobinPool(instances)
      .withResizer(resizer)
      .props(Props(new ResultActor(api, handler))), "ResultActor")

  private val statusActor = system.actorOf(
    RoundRobinPool(instances)
      .withResizer(resizer)
      .props(Props(new StatusActor(api, handler))), "StatusActor")

  override def execute(request:HttpRequest):String = {
    /*
     * Retrieve node that refers to the provided
     * `node_key`
     */
    val node = getNode(request)
    if (node == null)
      return buildInvalidResponse

    /*
     * We do not expect that the respective payload
     * is empty; therefore, no check is performed
     */
    val payload = getBodyAsJson(request).getAsJsonObject
    val logType = try {
      payload.get(OsqueryConstants.LOG_TYPE).getAsString

    } catch {
      case _:Throwable => null
    }

    if (logType == null) {

      log.error(s"Request contains invalid `log_type`.")
      return buildInvalidResponse

    }

    /* Extract data from request body */
    val data = try {
      payload.get(OsqueryConstants.DATA).getAsJsonArray

    } catch {
      case _:Throwable => null
    }

    if (data == null) {
      log.warning("No data for logging received")

    } else {
      /*
       * `result` and `status` messages are
       * supported as logging information
       */
      logType match {
        case "result" =>
          /*
           * Send message to [ResultActor]
           */
          val future = resultActor ? ResultReq(node = node, data = data)
          val response = Await.result(future, timeout.duration).asInstanceOf[ResultRsp]

          if (!response.success)
            log.error(response.message)
        case "status" =>
          /*
           * Send message to [StatusActor]
           */
          val future = statusActor ? StatusReq(node = node, data = data)
          val response = Await.result(future, timeout.duration).asInstanceOf[StatusRsp]

          if (!response.success)
            log.error(response.message)
        case _ =>
          log.error(s"Request contains invalid `log_type`.")
      }
    }

    /*
     * Update node in Apache Ignite cache and send respond
     * to the osquery agent (node)
     */
    api.nodeUpdate(node)
    buildResponse(nodeInvalid = false)

  }

}
