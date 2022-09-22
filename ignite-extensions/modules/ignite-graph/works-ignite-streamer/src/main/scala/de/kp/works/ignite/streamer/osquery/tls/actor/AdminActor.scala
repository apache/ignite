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

import akka.http.scaladsl.model.HttpRequest
import com.google.gson.JsonObject
import de.kp.works.ignite.streamer.osquery.OsqueryConstants
import de.kp.works.ignite.streamer.osquery.tls.db.DBApi

class AdminActor(api:DBApi) extends BaseActor(api) {

  override def execute(request: HttpRequest): String = {

    try {
      /*
       * STEP #1: Extract JSON body from external
       * admin request
       */
      val json = getBodyAsJson(request)
      if (json == null) {

        log.error("Request did not contain valid JSON.")
        return buildInvalidResponse

      }

      val payload = json.getAsJsonObject
      val requestData = payload
        .get(OsqueryConstants.REQUEST_DATA).getAsJsonObject
      /*
       * STEP #2: Extract the request type from the
       * provided admin request
       */
      val requestType = payload
        .get(OsqueryConstants.REQUEST_TYPE).getAsString

      requestType match {
        case OsqueryConstants.CONFIG_REQUEST => processConfig(requestData)
        case _ => throw new Exception(s"Unknown admin request `$requestType` detected.")
      }

      // TODO
      throw new Exception("not implemented yet.")

    } catch {
      case t:Throwable => buildErrorResponse(t.getLocalizedMessage)
    }

  }
  private def processConfig(config:JsonObject):Unit = {

  }

  private def buildErrorResponse(message:String):String = {

    val response = new JsonObject()
    response.addProperty(OsqueryConstants.REQUEST_INVALID, false)
    response.addProperty(OsqueryConstants.ERROR_MESSAGE, message)

    response.toString

  }

  override def buildInvalidResponse:String = {

    val response = new JsonObject()
    response.addProperty(OsqueryConstants.REQUEST_INVALID, false)

    response.toString
  }

}
