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

class EntityActor(ignite:Ignite) extends BaseActor {

  private val reader = new FiwareTableReader(ignite)

  override def receive: Receive = {

    case message: (String, HttpRequest) =>
      sender ! Response(Try({
        val (entityId, httpReq) = message
        execute(entityId, httpReq)
      })
        .recover {
          case e: Exception =>
            throw new Exception(e.getMessage)
        })

  }

  private def execute(entityId:String, httpReq: HttpRequest):Unit = {

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
     * Optional. Comma-separated list of attribute names whose
     * data are to be included in the response. The attributes
     * are retrieved in the order specified by this parameter.
     *
     * If not specified, all attributes are included in the
     * response in arbitrary order.
     */
    val attrNames = queryParams.getOrElse("attrs", "").split(",")
    /*
     * Optional. The function to apply to the raw data filtered
     * by the query parameters. If not given, the returned data
     * are the same raw inserted data.
     *
     * Available values : count, sum, avg, min, max
     */
    val aggrMethod = queryParams.getOrElse("aggrMethod", "")
    /*
     * Optional. If not defined, the aggregation will apply to all
     * the values contained in the search result. If defined, the
     * aggregation function will instead be applied N times, once
     * for each period, and all those results will be considered
     * for the response.
     *
     * For example, a query asking for the average temperature of
     * an attribute will typically return 1 value. However, with an
     * aggregationPeriod of day, you get the daily average of the
     * temperature instead (more than one value assuming you had
     * measurements across many days within the scope of your search
     * result).
     *
     * aggrPeriod must be accompanied by an aggrMethod, and the
     * aggrMethod will be applied to all the numeric attributes
     * specified in attrs; the rest of the non-numerical attrs
     * will be ignored.
     *
     * By default, the response is grouped by entity_id.
     *
     * Available values : year, month, day, hour, minute, second
     */
    val aggrPeriod = queryParams.getOrElse("aggrPeriod", "")
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
     * Optional. Used to request only the last N values that satisfy
     * the request conditions.
     *
     * minimum: 1
     */
    val lastN = queryParams.getOrElse("lastN", "0").toInt
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
     * The current implementation does not support geospatial
     * query parameters (in contrast to QuantumLeap).
     */

    // TODO
  }

}
