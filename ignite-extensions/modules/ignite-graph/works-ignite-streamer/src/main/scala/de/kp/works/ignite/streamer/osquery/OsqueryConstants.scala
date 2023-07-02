package de.kp.works.ignite.streamer.osquery
/*
 * Copyright (c) 20129 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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

object OsqueryConstants {
  /**
   * The cache name used to temporarily store
   * Osquery agent query results and status
   * messages
   */
  val OSQUERY_CACHE:String = "osquery_events"

  val FIELD_TYPE:String = "eventType"
  val FIELD_DATA:String = "eventData"

  /**
   * The subsequent fields support the administration
   * use cases of the TLS server
   */
  val CONFIG_REQUEST  = "config_request"
  val ERROR_MESSAGE   = "error_message"
  val REQUEST_INVALID = "request_invalid"
  val REQUEST_TYPE    = "request_type"
  val REQUEST_DATA    = "request_data"


  val ADHOC_EVENT:String  = "osquery_adhoc"
  val RESULT_EVENT:String = "osquery_result"
  val STATUS_EVENT:String = "osquery_status"

  val DATA            = "data"
  val ENROLL_SECRET   = "enroll_secret"
  val HOST_IDENTIFIER = "host_identifier"
  val LOG_TYPE        = "log_type"
  val NODE_IDENT      = "node_ident"
  val NODE_INVALID    = "node_invalid"
  val NODE_KEY        = "node_key"
  val QUERIES         = "queries"
  val STATUSES        = "statuses"

  /* Normalized fields */
  val ACTION        = "action"
  val ADDED         = "added"
  val CALENDAR_TIME = "calendarTime"
  val COLUMNS       = "columns"
  val DIFF_RESULTS  = "diffResults"
  val HOST          = "host"
  val HOSTNAME      = "hostname"
  val MESSAGE       = "message"
  val NAME          = "name"
  val NODE          = "node"
  val REMOVED       = "removed"
  val SNAPSHOT      = "snapshot"
  val TIMESTAMP     = "timestamp"
  val UID           = "uid"

}
