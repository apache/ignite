package de.kp.works.ignite.streamer.osquery.tls

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

import de.kp.works.ignite.IgniteConnect
import de.kp.works.ignite.conf.WorksConf
import de.kp.works.ignite.core.{BaseEngine, IgniteStream, IgniteStreamContext}
import de.kp.works.ignite.streamer.osquery.tls.db.DBApi
import de.kp.works.ignite.streamer.osquery.OsqueryConstants
import org.apache.ignite.IgniteCache
import org.apache.ignite.binary.BinaryObject
import org.apache.ignite.stream.StreamSingleTupleExtractor

import java.security.MessageDigest
import java.util
import scala.collection.JavaConversions._
/**
 * [TLSEngine] is responsible for streaming Osquery query results
 * and status messages into a temporary cache and also for their final
 * processing as query specific Apache Ignite tables.
 */
class TLSEngine(connect:IgniteConnect) extends BaseEngine(connect) {
  /*
   * The name of the temporary cache to write Osquery events to
   */
  override protected var cacheName: String = OsqueryConstants.OSQUERY_CACHE

  if (!WorksConf.isInit)
    throw new Exception("[TLSEngine] No configuration initialized. Streaming cannot be started.")

  private val name = WorksConf.OSQUERY_CONF
  private val conf = WorksConf.getStreamerCfg(name)

  private val api:DBApi = new DBApi(connect.getIgnite)
  /**
   * This is the main method to build the Osquery
   * streaming service (see OsqueryStream object).
   *
   * The respective [IgniteStreamContext] combines
   * the plain Ignite streamer with the cache and its
   * specific processor.
   *
   * The context also comprises the connector to the
   * Osquery event stream.
   */
  override def buildStream: Option[IgniteStreamContext] = {

    try {

      val (myCache,myStreamer) = prepareStreamer
      val myThreads = conf.getInt("numThreads")
      /*
       * Build stream
       */
      val myStream: IgniteStream = new IgniteStream {
        override val processor = new TLSProcessor(myCache, connect)
      }
      /*
       * Build stream context
       */
      val myStreamContext: IgniteStreamContext = new IgniteStreamContext {
        override val stream: IgniteStream = myStream
        override val streamer: TLSStreamer[String, BinaryObject] = myStreamer

        override val numThreads: Int = myThreads
      }

      Some(myStreamContext)

    } catch {
      case t:Throwable =>
        println(s"[ERROR] Stream preparation for 'ingestion' operation failed: ${t.getLocalizedMessage}")
        None
    }

  }

  private def prepareStreamer:(IgniteCache[String,BinaryObject],TLSStreamer[String,BinaryObject]) = {
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
    val osqueryStreamer = new TLSStreamer[String,BinaryObject](api)

    osqueryStreamer.setIgnite(ignite)
    osqueryStreamer.setStreamer(streamer)
    /*
     * The Osquery extractor is the linking element between the
     * Osquery events and its specification as Apache Ignite
     * cache entry.
     *
     * We currently leverage a single tuple extractor as we do
     * not have experience whether we should introduce multiple
     * tuple extraction. Additional performance requirements can
     * lead to a channel in the selected extractor
     */
    val extractor = createExtractor
    osqueryStreamer.setSingleTupleExtractor(extractor)

    (cache, osqueryStreamer)
  }

  private def createExtractor: StreamSingleTupleExtractor[TLSEvent, String, BinaryObject] = {

    new StreamSingleTupleExtractor[TLSEvent,String,BinaryObject]() {

      override def extract(event:TLSEvent):java.util.Map.Entry[String,BinaryObject] = {

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

  private def buildEntry(event:TLSEvent):(String, BinaryObject) = {

    val builder = ignite.binary().builder(cacheName)

    builder.setField(OsqueryConstants.FIELD_TYPE, event.eventType)
    builder.setField(OsqueryConstants.FIELD_DATA, event.eventData)

    val cacheValue = builder.build()
    /*
     * The cache key is built from the content
     * to enable the detection of duplicates.
     *
     * (see OsqueryProcessor)
     */
    val serialized = Seq(
      event.eventType,
      event.eventData).mkString("#")

    val cacheKey = new String(MessageDigest.getInstance("MD5")
      .digest(serialized.getBytes("UTF-8")))

    (cacheKey, cacheValue)

  }

  override protected def buildFields(): util.LinkedHashMap[String, String] = {

    val fields = new java.util.LinkedHashMap[String,String]()
    /*
     * The event type
     */
    fields.put(OsqueryConstants.FIELD_TYPE,"java.lang.String")
    /*
     * The data that is associated with the event
     */
    fields.put(OsqueryConstants.FIELD_DATA,"java.lang.String")
    fields

  }

}
