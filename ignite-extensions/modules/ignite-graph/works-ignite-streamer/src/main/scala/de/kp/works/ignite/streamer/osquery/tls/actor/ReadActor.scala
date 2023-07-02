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
import com.google.gson._
import de.kp.works.ignite.streamer.osquery.OsqueryConstants
import de.kp.works.ignite.streamer.osquery.tls.db.DBApi

/**
 * As of version 1.5.3, osquery provides support for "ad-hoc" or distributed queries.
 * The concept of running a query outside of the schedule and having results returned
 * immediately. Distributed queries must be explicitly enabled with a CLI flag or option,
 * and you must explicitly enable and configure the distributed plugin.
 */
class ReadActor(api:DBApi) extends BaseActor(api) {

  override def execute(request:HttpRequest):String = {
    /*
     * Retrieve node that refers to the provided
     * `node_key`
     *
     * {
     *  "node_key": "..." // Optionally blank
     *  }
     */
    val node = getNode(request)
    if (node == null)
      return buildInvalidResponse

    val queries = api.nodeNewQueries(node.uuid)
    if (queries == null) {

      log.error(s"Invalid queries for node `${node.nodeKey}` detected.")
      return buildInvalidResponse

    }
    /*
     * Update node in Apache Ignite Cache, unpack distributed
     * queries and send back to the remote node
     */
    api.nodeUpdate(node)
    /*
     * {
     *  "queries": {
     * 	 "id1": "SELECT * FROM osquery_info;",
     *    "id2": "SELECT * FROM osquery_schedule;",
     *    "id3": "SELECT * FROM does_not_exist;"
     *  },
     *  "node_invalid": false
     * }
     */
    val response = new JsonObject()

    response.add(OsqueryConstants.QUERIES, queries)
    response.addProperty(OsqueryConstants.NODE_INVALID, false)

    response.toString

  }

}
