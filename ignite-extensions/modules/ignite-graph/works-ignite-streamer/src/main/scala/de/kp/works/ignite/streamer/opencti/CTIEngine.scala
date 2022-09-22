package de.kp.works.ignite.streamer.opencti
/**
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

import de.kp.works.ignite.IgniteConnect
import de.kp.works.ignite.conf.WorksConf
import de.kp.works.ignite.core.{BaseEngine, IgniteStream, IgniteStreamContext}
import de.kp.works.ignite.sse.SseEvent
import org.apache.ignite.IgniteCache
import org.apache.ignite.binary.BinaryObject
import org.apache.ignite.stream.StreamSingleTupleExtractor

import java.security.MessageDigest
import scala.collection.JavaConversions.mapAsJavaMap

/**
 * [CTIEngine] is responsible for streaming threat intelligence
 * events into a temporary cache and also their final processing
 * as edges & vertices of an information network.
 */
class CTIEngine(connect:IgniteConnect) extends BaseEngine(connect) {

  override var cacheName: String = CTIConstants.OPENCTI_CACHE

  if (!WorksConf.isInit)
    throw new Exception("[CTIEngine] No configuration initialized. Streaming cannot be started.")

  private val conf = WorksConf.getStreamerCfg(WorksConf.OPENCTI_CONF)
  /**
   * This is the main method to build the OpenCTI
   * streaming service (see CTIStream object).
   *
   * The respective [IgniteCTIContext] combines
   * the plain Ignite streamer with the cache
   * and its specific processor.
   *
   * The context also comprises the connector to
   * the OpenCTI event stream.
   */
  override def buildStream:Option[IgniteStreamContext] = {

    try {

      val (myCache, myStreamer) = prepareStreamer
      val myThreads = conf.getInt("numThreads")
      /*
       * Build stream
       */
      val myStream: IgniteStream = new IgniteStream {
        override val processor = new CTIProcessor(myCache, connect)
      }
      /*
       * Build stream context
       */
      val myStreamContext: IgniteStreamContext = new IgniteStreamContext {
        override val stream: IgniteStream = myStream
        override val streamer: CTIStreamer[String, BinaryObject] = myStreamer

        override val numThreads: Int = myThreads
      }

      Some(myStreamContext)

    } catch {
      case t: Throwable =>
        println(s"[ERROR] Stream preparation for 'ingestion' operation failed: ${t.getLocalizedMessage}")
        None
    }
  }

  private def prepareStreamer:(IgniteCache[String,BinaryObject],CTIStreamer[String,BinaryObject]) = {
    /*
     * The auto flush frequency of the stream buffer is
     * internally set to 0.5 sec (500 ms)
     */
    val autoFlushFrequency = conf.getInt("autoFlushFrequency")
    /*
     * The cache is configured with sliding window holding
     * N seconds of the streaming data; note, that we delete
     * an already equal named cache
     */
    deleteCache()
    /*
     * The time window specifies the batch window that
     * is used to gather stream events
     */
    val timeWindow = conf.getInt("timeWindow")

    val config = createCacheConfig(timeWindow)
    val cache = ignite.getOrCreateCache(config)

    val streamer = ignite.dataStreamer[String,BinaryObject](cache.getName)
    /*
     * allowOverwrite(boolean) - Sets flag enabling overwriting
     * existing values in cache. Data streamer will perform better
     * if this flag is disabled, which is the default setting.
     */
    streamer.allowOverwrite(false)
    /*
     * IgniteDataStreamer buffers the data and most likely it just
     * waits for buffers to fill up. We set the time interval after
     * which buffers will be flushed even if they are not full
     */
    streamer.autoFlushFrequency(autoFlushFrequency)
    val ctiStreamer = new CTIStreamer[String,BinaryObject]()

    ctiStreamer.setIgnite(ignite)
    ctiStreamer.setStreamer(streamer)
    /*
     * The OpenCTI extractor is the linking element between the
     * OpenCTI events and its specification as Apache Ignite
     * cache entry.
     *
     * We currently leverage a single tuple extractor as we do
     * not have experience whether we should introduce multiple
     * tuple extraction. Additional performance requirements can
     * lead to a channel in the selected extractor
     */
    val ctiExtractor = createExtractor
    ctiStreamer.setSingleTupleExtractor(ctiExtractor)

    (cache, ctiStreamer)

  }

  private def createExtractor: StreamSingleTupleExtractor[SseEvent, String, BinaryObject] = {

    new StreamSingleTupleExtractor[SseEvent,String,BinaryObject]() {

      override def extract(event:SseEvent):java.util.Map.Entry[String,BinaryObject] = {

        val entries = scala.collection.mutable.HashMap.empty[String,BinaryObject]
        try {

          val (cacheKey, cacheValue) = buildEntry(event)
          entries.put(cacheKey,cacheValue)

        } catch {
          case e:Exception => e.printStackTrace()
        }
        entries.entrySet().iterator().next

      }
    }
  }
  /**
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
  private def buildEntry(event:SseEvent):(String, BinaryObject) = {

    val builder = ignite.binary().builder(CTIConstants.OPENCTI_CACHE)
    builder.setField(CTIConstants.FIELD_ID, event.eventId)

    builder.setField(CTIConstants.FIELD_TYPE, event.eventType)
    builder.setField(CTIConstants.FIELD_DATA, event.data)

    val cacheValue = builder.build()
    /*
     * The cache key is built from the content
     * to enable the detection of duplicates.
     *
     * (see CTIProcessor)
     */
    val serialized = Seq(
      event.eventId,
      event.eventType,
      event.data).mkString("#")

    val cacheKey = new String(MessageDigest.getInstance("MD5")
      .digest(serialized.getBytes("UTF-8")))

    (cacheKey, cacheValue)
  }
  /**
   * A helper method to build the fields of an Apache
   * Ignite QueryEntity; this entity reflects the format
   * of an SSE message
   */
  override def buildFields():java.util.LinkedHashMap[String,String] = {

    val fields = new java.util.LinkedHashMap[String,String]()
    /*
     * The event identifier
     */
    fields.put(CTIConstants.FIELD_ID,"java.lang.String")
    /*
     * The event type
     */
    fields.put(CTIConstants.FIELD_TYPE,"java.lang.String")
    /*
     * The data that is associated with the event
     */
    fields.put(CTIConstants.FIELD_DATA,"java.lang.String")
    fields

  }

}
