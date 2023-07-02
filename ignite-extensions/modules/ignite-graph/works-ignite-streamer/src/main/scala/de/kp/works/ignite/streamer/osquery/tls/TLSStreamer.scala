package de.kp.works.ignite.streamer.osquery.tls

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

import de.kp.works.ignite.core.IgniteStreamer
import de.kp.works.ignite.streamer.osquery.tls.db.DBApi
import org.apache.ignite.stream.StreamAdapter
import org.apache.ignite.IgniteException

class TLSStreamer[K,V](api:DBApi)
  extends StreamAdapter[TLSEvent, K, V] with TLSEventHandler with IgniteStreamer {

  /** TLS server */

  private var server:Option[TLSServer] = None

  /** State keeping. */
  private val stopped = true

  /** Start streamer  **/

  override def start():Unit = {

    if (!stopped)
      throw new IgniteException("Attempted to start an already started TLS Streamer.")

    server = Some(new TLSServer(api))
    server.get.setEventHandler(this)

    server.get.launch()

  }

  /** Stop streamer **/

  override def stop():Unit = {

    if (stopped)
      throw new IgniteException("Failed to stop TLS Streamer (already stopped).")

    if (server.isEmpty)
      throw new IgniteException("Failed to stop the TLS Server (never started).")

    /*
     * Stopping the streamer equals stopping
     * the Osquery server
     */
    server.get.stop()

  }

  /********************************
   *
   * Osquery event handler method
   *
   *******************************/

  override def eventArrived(event: TLSEvent): Unit = {

    val log = getIgnite.log()
    /*
     * The leveraged extractors below must be explicitly
     * defined when initiating this streamer
     */
    if (getMultipleTupleExtractor != null) {

      val entries:java.util.Map[K,V] = getMultipleTupleExtractor.extract(event)
      if (log.isTraceEnabled)
        log.trace("Adding cache entries: " + entries)

      getStreamer.addData(entries)

    }
    else {

      val entry:java.util.Map.Entry[K,V] = getSingleTupleExtractor.extract(event)
      if (log.isTraceEnabled)
        log.trace("Adding cache entry: " + entry)

      getStreamer.addData(entry)

    }

  }
}
