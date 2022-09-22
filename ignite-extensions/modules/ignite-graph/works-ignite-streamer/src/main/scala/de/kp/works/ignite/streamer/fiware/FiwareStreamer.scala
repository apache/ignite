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

import de.kp.works.ignite.core.IgniteStreamer
import org.apache.ignite.IgniteException
import org.apache.ignite.stream.StreamAdapter

trait FiwareEventHandler {

  def connectionLost():Unit

  def notificationArrived(notification:FiwareEvent):Unit

}
/**
 * [FiwareStreamer] class is responsible for writing Fiware notifications
 * to a temporary Ignite cache; the streamer connects to the Fiware Context
 * Broker by providing a HTTP notification endpoint.
 */
class FiwareStreamer[K,V]
  extends StreamAdapter[FiwareEvent, K, V] with FiwareEventHandler with IgniteStreamer {

  /** FiwareServer */

  private var server:Option[FiwareServer] = None

  /** State keeping. */
  private val stopped = true

  /** Start streamer  **/

  override def start():Unit = {

    if (!stopped)
      throw new IgniteException("Attempted to start an already started Fiware Streamer.")

    server = Some(new FiwareServer())
    server.get.setEventHandler(this)

    server.get.setIgnite(ignite)
    server.get.launch()

  }

  /** Stop streamer **/

  override def stop():Unit = {

    if (stopped)
      throw new IgniteException("Failed to stop Fiware Streamer (already stopped).")

    if (server.isEmpty)
      throw new IgniteException("Failed to stop the Fiware Server (never started).")
    /*
     * Stopping the streamer equals stopping
     * the Fiware notification server
     */
    server.get.stop()

  }

  /********************************
   *
   * Fiware event handler methods
   *
   *******************************/

  override def connectionLost():Unit = {
  }

  override def notificationArrived(notification: FiwareEvent): Unit = {

    val log = getIgnite.log()
    /*
     * The leveraged extractors below must be explicitly
     * defined when initiating this streamer
     */
    if (getMultipleTupleExtractor != null) {

      val entries:java.util.Map[K,V] = getMultipleTupleExtractor.extract(notification)
      if (log.isTraceEnabled)
        log.trace("Adding cache entries: " + entries)

      getStreamer.addData(entries)

    }
    else {

      val entry:java.util.Map.Entry[K,V] = getSingleTupleExtractor.extract(notification)
      if (log.isTraceEnabled)
        log.trace("Adding cache entry: " + entry)

      getStreamer.addData(entry)

    }

  }
}
