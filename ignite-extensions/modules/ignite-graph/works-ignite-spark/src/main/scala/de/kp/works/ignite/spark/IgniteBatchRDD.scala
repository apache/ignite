package de.kp.works.ignite.spark
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
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.internal.processors.datastreamer.DataStreamerCacheUpdaters
import org.apache.ignite.spark.{IgniteContext, IgniteRDD}
import org.apache.spark.rdd.RDD

class IgniteBatchRDD[K,V](
   override val ic: IgniteContext,
   override val cacheName: String,
   override val cacheCfg: CacheConfiguration[K, V],
   override val keepBinary: Boolean) extends IgniteRDD[K,V](ic,cacheName,cacheCfg,keepBinary) {
    /**
     * Saves values from the given RDD into Ignite.
     *
     * @param rdd RDD instance to save values from.
     * @param f Transformation function.
     * @param overwrite Boolean flag indicating whether the call on this method should overwrite existing
     *      values in Ignite cache.
     */
    override def savePairs[T](rdd: RDD[T], f: (T, IgniteContext) ⇒ (K, V), overwrite: Boolean, skipStore:Boolean): Unit = {

      rdd.foreachPartition(it ⇒ {

        val ig = ic.ignite()

        /* Make sure to deploy the cache */
        ensureCache()
        val streamer = ig.dataStreamer[K, V](cacheName)
        /*
         * Sets flag enabling overwriting existing values in cache. Data streamer
         * will perform better if this flag is disabled, which is the default setting.
         */
        streamer.allowOverwrite(overwrite)
        /*
         * Sets flag indicating that write-through behavior should be disabled for
         * data streaming.
         */
        streamer.skipStore(skipStore)
        /*
         * Automatic flush frequency in milliseconds. Essentially, this is the
         * time after which the streamer will make an attempt to submit all data
         * added so far to remote nodes.
         */
        streamer.autoFlushFrequency(0)
        /*
         * Introduce batch updater (a) to reach writeAll, and, (b) to improve
         * performance
         */
        val receiver = DataStreamerCacheUpdaters.batched[K,V]()
        streamer.receiver(receiver)

        try {

          it.foreach(t ⇒ {
            val tup = f(t, ic)

            streamer.addData(tup._1, tup._2)
          })

        }
        catch {
          case t:Throwable => t.printStackTrace()
        }
        finally {
          streamer.close()
        }
      })
    }

}
