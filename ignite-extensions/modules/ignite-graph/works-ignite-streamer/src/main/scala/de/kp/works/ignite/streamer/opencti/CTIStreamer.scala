package de.kp.works.ignite.streamer.opencti
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
import de.kp.works.ignite.sse.{SseEvent, SseEventHandler}
import org.apache.ignite.IgniteException
import org.apache.ignite.stream.StreamAdapter

class CTIStreamer[K,V]
  extends StreamAdapter[SseEvent, K, V] with SseEventHandler with IgniteStreamer {

  /** OpenCTI Service */

  private var service:Option[CTIService] = None

  /** State keeping. */
  private val stopped = true

  /** Start streamer  **/

  override def start():Unit = {

    if (!stopped)
      throw new IgniteException("Attempted to start an already started OpenCTI Streamer.")

    service = Some(new CTIService())
    service.get.setEventHandler(this)

    service.get.start()

  }

  /** Stop streamer **/

  override def stop():Unit = {

    if (stopped)
      throw new IgniteException("Failed to stop OpenCTI Streamer (already stopped).")

    if (service.isEmpty)
      throw new IgniteException("Failed to stop the OpenCTI Service (never started).")
    /*
     * Stopping the streamer equals stopping
     * the OpenCTI event service
     */
    service.get.stop()

  }

  /********************************
   *
   *  OpenCTI event handler methods
   *
   *******************************/

  override def connectionLost(): Unit = {/* Do nothing */}
  /*
   * Events format :: SseEvent(id, event, data)
   *
   * The events published by OpenCTI are based on the STIX format:
   *
   * id: {Event stream id} -> Like 1620249512318-0
   * event: {Event type} -> create / update / delete
   * data: { -> The complete event data
   *    markings: [] -> Array of markings IDS of the element
   *    origin: {Data Origin} -> Complex object with different information about the origin of the event
   *    data: {STIX data} -> The STIX representation of the data.
   *    message -> A simple string to easy understand the event
   *    version -> The version number of the event
   * }
   */
  override def eventArrived(event: SseEvent): Unit = {

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
