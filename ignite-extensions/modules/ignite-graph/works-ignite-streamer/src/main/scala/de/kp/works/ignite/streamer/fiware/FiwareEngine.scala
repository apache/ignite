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
import de.kp.works.ignite.IgniteConnect
import de.kp.works.ignite.conf.WorksConf
import de.kp.works.ignite.core.{BaseEngine, IgniteStream, IgniteStreamContext}
import org.apache.ignite.IgniteCache
import org.apache.ignite.binary.BinaryObject
import org.apache.ignite.stream.StreamSingleTupleExtractor

import java.security.MessageDigest
import scala.collection.JavaConversions.mapAsJavaMap
/**
 * [FiwareIgnite] is responsible for streaming Fiware Broker
 * notifications into a temporary cache and also their final
 * processing as edges & vertices of an information network.
 */
class FiwareEngine(connect:IgniteConnect) extends BaseEngine(connect) {

  override var cacheName:String = FiwareConstants.FIWARE_CACHE

  if (!WorksConf.isInit)
    throw new Exception("[FiwareIgnite] No configuration initialized. Streaming cannot be started.")

  private val conf = WorksConf.getStreamerCfg(WorksConf.FIWARE_CONF)

  override def buildStream:Option[IgniteStreamContext] = {

    try {

      val (myCache,myStreamer) = prepareStreamer
      val myThreads = conf.getInt("numThreads")
      /*
       * Build stream
       */
      val myStream: IgniteStream = new IgniteStream {
        override val processor = new FiwareProcessor(myCache, connect)
      }
      /*
       * Build stream context
       */
      val myStreamContext: IgniteStreamContext = new IgniteStreamContext {
        override val stream: IgniteStream = myStream
        override val streamer: FiwareStreamer[String, BinaryObject] = myStreamer

        override val numThreads: Int = myThreads
      }

      Some(myStreamContext)

    } catch {
      case t:Throwable =>
        println(s"[ERROR] Stream preparation for 'ingestion' operation failed: ${t.getLocalizedMessage}")
        None
    }

  }

  private def prepareStreamer:(IgniteCache[String,BinaryObject],FiwareStreamer[String,BinaryObject]) = {
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
    val fiwareStreamer = new FiwareStreamer[String,BinaryObject]()

    fiwareStreamer.setIgnite(ignite)
    fiwareStreamer.setStreamer(streamer)
    /*
     * The Fiware extractor is the linking element between the
     * Fiware notification and its specification as Apache Ignite
     * cache entry.
     *
     * We currently leverage a single tuple extractor as we do
     * not have experience whether we should introduce multiple
     * tuple extraction. Additional performance requirements can
     * lead to a channel in the selected extractor
     */
    val fiwareExtractor = createExtractor
    fiwareStreamer.setSingleTupleExtractor(fiwareExtractor)

    (cache, fiwareStreamer)
  }

  private def createExtractor: StreamSingleTupleExtractor[FiwareEvent, String, BinaryObject] = {

    new StreamSingleTupleExtractor[FiwareEvent,String,BinaryObject]() {

      override def extract(notification:FiwareEvent):java.util.Map.Entry[String,BinaryObject] = {

        val entries = scala.collection.mutable.HashMap.empty[String,BinaryObject]
        try {

          val (cacheKey, cacheValue) = buildEntry(notification)
          entries.put(cacheKey,cacheValue)

        } catch {
          case e:Exception => e.printStackTrace()
        }

        entries.entrySet().iterator().next

      }
    }
  }

  private def buildEntry(notification:FiwareEvent): (String, BinaryObject) = {

    val builder = ignite.binary().builder(FiwareConstants.FIWARE_CACHE)
    builder.setField(FiwareConstants.FIELD_SERVICE, notification.service)

    builder.setField(FiwareConstants.FIELD_SERVICE_PATH, notification.servicePath)
    builder.setField(FiwareConstants.FIELD_PAYLOAD, notification.payload.toString)

    val cacheValue = builder.build()
    /*
     * The cache key is built from the content
     * to enable the detection of duplicates.
     *
     * (see FiwareProcessor)
     */
    val serialized = Seq(
      notification.service,
      notification.servicePath,
      notification.payload.toString).mkString("#")

    val cacheKey = new String(MessageDigest.getInstance("MD5")
      .digest(serialized.getBytes("UTF-8")))

    (cacheKey, cacheValue)

  }
  /**
   * A helper method to build the fields of an Apache
   * Ignite QueryEntity; this entity reflects the format
   * of a Fiware notification
   */
  override def buildFields():java.util.LinkedHashMap[String,String] = {

    val fields = new java.util.LinkedHashMap[String,String]()
    /*
     * The service that is associated
     * with the notification
     */
    fields.put(FiwareConstants.FIELD_SERVICE,"java.lang.String")
    /*
     * The service path that is associated
     * with the notification
     */
    fields.put(FiwareConstants.FIELD_SERVICE_PATH,"java.lang.String")
    /*
     * The payload that is associated
     * with the notification
     */
    fields.put(FiwareConstants.FIELD_PAYLOAD,"java.lang.String")
    fields

  }

}
