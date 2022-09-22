package de.kp.works.ignite.streamer.beat

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

import de.kp.works.ignite.core.IgniteStreamer
import de.kp.works.ignite.sse.{SseEvent, SseEventHandler}
import org.apache.ignite.IgniteException
import org.apache.ignite.stream.StreamAdapter

class BeatStreamer[K,V]
  extends StreamAdapter[SseEvent, K, V] with SseEventHandler with IgniteStreamer {

  /** Beat Service */

  private var service:Option[BeatService] = None

  /** State keeping. */
  private val stopped = true

  /** Start streamer  **/

  override def start():Unit = {

    if (!stopped)
      throw new IgniteException("Attempted to start an already started Beat Streamer.")

    service = Some(new BeatService())
    service.get.setEventHandler(this)

    service.get.start()

  }

  /** Stop streamer **/

  override def stop():Unit = {

    if (stopped)
      throw new IgniteException("Failed to stop Beat Streamer (already stopped).")

    if (service.isEmpty)
      throw new IgniteException("Failed to stop the Beat Service (never started).")
    /*
     * Stopping the streamer equals stopping
     * the OpenCTI event service
     */
    service.get.stop()

  }

  /********************************
   *
   *  Beat event handler methods
   *
   *******************************/

  override def connectionLost(): Unit = {/* Do nothing */}
  /**
   * Events format :: SseEvent(id, event, data)
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
