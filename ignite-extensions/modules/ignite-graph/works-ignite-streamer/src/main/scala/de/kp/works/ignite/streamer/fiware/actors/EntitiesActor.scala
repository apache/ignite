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
import de.kp.works.ignite.streamer.fiware.table.FiwareTableReader
import org.apache.ignite.Ignite

import scala.util.Try

class EntitiesActor(ignite:Ignite) extends BaseActor {

  private val reader = new FiwareTableReader(ignite)

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

  private def execute(httpReq: HttpRequest):String = {

    /** HEADERS **/

    val headers = httpReq.headers

    val service = getHeaderValue("fiware-service", headers)
    val servicePath = getHeaderValue("fiware-servicepath", headers)

    /** QUERY PARAMS **/

    val queryParams = httpReq.uri.query().toMap
    /*
     * Comma-separated list of entity types whose data
     * are to be included in the response.
     *
     * Use only one (no comma) when required. If used to
     * resolve ambiguity for the given entityId, make sure
     * the given entityId exists for this entityType.
     */
    val entityTypes = queryParams.getOrElse("type", "").split(",")
    /*
     * Optional. The starting date and time (inclusive) from
     * which the context information is queried.
     *
     * Must be in ISO8601 format (e.g., 2018-01-05T15:44:34)
     */
    val fromDate = queryParams.getOrElse("fromDate", "")
    val startTs = if (fromDate.isEmpty) 0L else toEpochMillis(fromDate)
    /*
     * Optional. The final date and time (inclusive) from
     * which the context information is queried.
     *
     * Must be in ISO8601 format (e.g., 2018-01-05T15:44:34)
     */
    val toDate = queryParams.getOrElse("toDate", "")
    val endTs = if (toDate.isEmpty) 0L else toEpochMillis(toDate)
    /*
     * Optional. Maximum number of results to retrieve in a
     * single response. The default value = 100
     */
    val limit = queryParams.getOrElse("limit", "100").toInt
    /*
     * Optional. Offset to apply to the response results.
     * For example, if the query was to return 10 results
     * and you use an offset of 1, the response will return
     * the last 9 values.
     */
    val offset = queryParams.getOrElse("offset", "0").toInt
    /*
     * Build parameter map for the data reader and assign
     * service and service path if provided
     */
    var params = Map(
      "startTs" -> startTs,
      "endTs"   -> endTs,
      "limit"   -> limit,
      "offset"  -> offset,
      "types"   -> entityTypes)

    if (service != null)
      params = params ++ Map("service" ->service)

    if (servicePath != null)
      params = params ++ Map("servicePath" ->servicePath)

    /** READ ENTITIES & RETURN **/

    val entities = reader.readEntities(params)
    entities.toString

  }
}
