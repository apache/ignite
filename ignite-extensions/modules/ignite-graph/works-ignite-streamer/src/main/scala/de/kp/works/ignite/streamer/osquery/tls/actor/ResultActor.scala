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
import de.kp.works.ignite.streamer.osquery.tls.{TLSEvent, TLSEventHandler, TLSNormalizer}
import de.kp.works.ignite.streamer.osquery.tls.actor.ResultActor._
import de.kp.works.ignite.streamer.osquery.OsqueryConstants
import de.kp.works.ignite.streamer.osquery.tls.db.{DBApi, OsqueryNode}
import de.kp.works.ignite.streamer.osquery.tls.table.TLSTransformer

/*
 * EVENT FORMAT
 *
 * Example output of SELECT name, path, pid FROM processes
 * {
 * #
 * # For incremental data, marks whether the entry was added
 * # or removed. It can be one of "added", "removed", or "snapshot".
 * #
 * "action": "added",
 * "columns": {
 *   "name": "osqueryd",
 *   "path": "/usr/local/bin/osqueryd",
 *   "pid": "97830"
 * },
 *
 * #
 * # The name of the query that generated this event
 * #
 * "name": "processes",
 * #
 * # The identifier for the host on which the osquery agent is
 * # running. Normally the hostname.
 * #
 * "hostIdentifier": "hostname.local",
 * #
 * # String representation of the collection time, as formatted
 * # by osquery.
 * #
 * "calendarTime": "Tue Sep 30 17:37:30 2014",
 * #
 * # Unix timestamp of the event, in seconds since the epoch.
 * # Used for computing the `@timestamp` column.
 * #
 * "unixTime": "1412123850",
 * "epoch": "314159265",
 * "counter": "1",
 * "numerics": false
 * }
 *
 *
 * SNAPSHOT FORMAT
 *
 * Snapshot queries attempt to mimic the differential event format,
 * instead of emitting "columns", the snapshot data is stored using
 * "snapshot".
 *
 *
 * {
 * "action": "snapshot",
 * "snapshot": [
 *   {
 *     "parent": "0",
 *     "path": "/sbin/launchd",
 *     "pid": "1"
 *   },
 *   {
 *     "parent": "1",
 *     "path": "/usr/sbin/syslogd",
 *     "pid": "51"
 *   },
 *   {
 *     "parent": "1",
 *     "path": "/usr/libexec/UserEventAgent",
 *     "pid": "52"
 *   },
 *   {
 *     "parent": "1",
 *     "path": "/usr/libexec/kextd",
 *     "pid": "54"
 *   }
 * ],
 * "name": "process_snapshot",
 * "hostIdentifier": "hostname.local",
 * "calendarTime": "Mon May  2 22:27:32 2016 UTC",
 * "unixTime": "1462228052",
 * "epoch": "314159265",
 * "counter": "1",
 * "numerics": false
 * }
 */

class ResultActor(api:DBApi, handler:TLSEventHandler) extends BaseActor(api) {

  override def receive: Receive = {

    case request:ResultReq =>
      /*
       * Send response message to `origin` immediately
       * as the logging task may last some time
       */
      val origin = sender
      origin ! ResultRsp("Logging started", success = true)

      try {

        val node = request.node
        val data = request.data

        /*
         * This implementation supports different query result
         * withing a single result request.
         *
         * Send each batch of a certain table (or query) name
         * to the output channel
         */
        val transformed = TLSNormalizer.normalize(node, data)
        transformed.foreach{case(table, batch) => {

          val event = TLSEvent(eventType = s"tls/$table", eventData = batch.toString)
          handler.eventArrived(event)

        }}

      } catch {
        case t:Throwable => origin ! ResultRsp("Result logging failed: " + t.getLocalizedMessage, success = false)
      }
  }

  override def execute(request: HttpRequest): String = {
    throw new Exception("Not implemented.")
  }

}

object ResultActor {

  case class ResultReq(node:OsqueryNode, data:JsonArray)
  case class ResultRsp(message:String, success:Boolean)

}
