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
import de.kp.works.ignite.streamer.osquery.OsqueryConstants
import de.kp.works.ignite.streamer.osquery.tls.db.DBApi

/**
 *
 * Osquery agent (osqueryd) --> TLS server
 *
 * Enrollment is an authentication step: Machines running osqueryd
 * processes (nodes) must authenticate to the (remote) TLS server
 * for every config retrieval and log submission request.
 *
 * Enrollment provides an initial secret to the remote server in
 * order to negotiate a private node secret used for future identification.
 */
class EnrollActor(api:DBApi) extends BaseActor(api) {

  override def execute(request:HttpRequest):String = {
    /*
     * STEP #1: Extract secret from remote
     * agent request
     */
    val json = getBodyAsJson(request)
    if (json == null) {

      log.error("Request did not contain valid JSON.")
      return buildInvalidResponse

    }

    val payload = json.getAsJsonObject
    /*
     * Enrollment provides an initial secret to the
     * remote server in order to negotiate a private
     * node secret used for future identification.
     *
     * The validity of a node_key is determined and
     * implemented in the TLS server. The node will
     * request the key during an initial enroll step
     * then post the key during subsequent requests
     * for config or logging.
     */
    val secret = try {
      payload.get(OsqueryConstants.ENROLL_SECRET).getAsString

    } catch {
      case _:Throwable => null
    }

    /*
     * The 'osquery' documentation specifies the
     * enroll_secret as optional. In this project,
     * it is mandatory to increase security
     */
    if (secret == null) {

      log.error("Request is no valid enroll request.")
      return buildInvalidResponse

    }

    /*
     * STEP #2: The nodes that are allowed to communicate
     * with this server are pre-defined in an internal table.
     *
     * The `secret`must be provided by the osquery agent as
     * it is used to uniquely identify and find the respective
     * database entry
     */
    var node = api.nodeBySecret(secret)
    if (node == null) {

      log.error("Invalid enroll secret.")
      return buildInvalidResponse

    }
    /*
     * Publish warning in case of a second enrollment
     * request, but accept the respective request
     */
    if (node.enrolled)
      log.warning("Node is already enrolled.")

    val hostIdentifier = try {
      val value = payload.get(OsqueryConstants.HOST_IDENTIFIER).getAsString
      if (value.isEmpty) null else value

    } catch {
      case _:Throwable => null
    }

    /*
     * The `hostIdentifier` is mandatory; it is registered
     * with the respective node; this parameter can be
     * different from the already registered one, but must
     * be provided
     */
    if (hostIdentifier == null) {

      log.error("Invalid host identifier.")
      return buildInvalidResponse

    }

    if (node.hostIdentifier != hostIdentifier)
      node = node.copy(hostIdentifier = hostIdentifier)

    /*
     * The remote (client) address is also registered with
     * the respective node `lastAddress` field
     */
    val remoteAddress = {

      val addressHeader = request.headers.find(_.name == "Remote-Address")
      if (addressHeader.isDefined) addressHeader.get.value() else null

    }

    if (remoteAddress != null && remoteAddress.nonEmpty)
      node = node.copy(lastAddress = remoteAddress)

    /*
     * Update the registered `node` and write to node database
     */
    node = node.copy(enrolled = true, lastCheckIn = System.currentTimeMillis)
    api.nodeUpdate(node)

    log.info(s"Node ${node.nodeKey} enrolled.")
    buildResponse(nodeInvalid = false, Option(node.nodeKey))

  }

}