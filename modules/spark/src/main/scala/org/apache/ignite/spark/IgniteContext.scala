/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spark

import org.apache.ignite.cluster.ClusterNode
import org.apache.ignite.internal.IgnitionEx
import org.apache.ignite.spark.util.using
import org.apache.ignite.{Ignition, IgniteCache, Ignite}
import org.apache.ignite.configuration.{CacheConfiguration, IgniteConfiguration}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class IgniteContext[K, V](
    sc: SparkContext,
    igniteCfg: IgniteConfiguration,
    val cacheName: String,
    cacheCfg: CacheConfiguration[K, V]
) {
    def this(
        sc: SparkContext,
        springUrl: String,
        cacheName: String
    ) {
        this(sc, IgnitionEx.loadConfiguration(springUrl).get1(), cacheName, null)
    }

    def this(
        sc: SparkContext,
        igniteCfg: IgniteConfiguration,
        cacheName: String
    ) {
        this(sc, igniteCfg, cacheName, null)
    }

    def this(
        sc: SparkContext,
        igniteCfg: IgniteConfiguration,
        cacheCfg: CacheConfiguration[K, V]
    ) {
        this(sc, igniteCfg, cacheCfg.getName, cacheCfg)
    }

    def this(
        sc: SparkContext,
        springUrl: String,
        cacheCfg: CacheConfiguration[K, V]
    ) {
        this(sc, IgnitionEx.loadConfiguration(springUrl).get1(), cacheCfg.getName, cacheCfg)
    }

    def sparkContext() = sc

    def saveToIgnite(rdd: RDD[V], keyFunc: (IgniteContext[K, V], V, ClusterNode) => K = affinityKeyFunc) = {
        rdd.foreachPartition(it => {
            // TODO get affinity node

            using(ignite().dataStreamer[K, V](cacheName)) { streamer =>
                it.foreach(value => {
                    val key: K = keyFunc(this, value, null)
                    streamer.addData(key, value)
                })
            }
        })
    }

    def ignite(): Ignite = {
        try {
            Ignition.ignite(igniteCfg.getGridName)
        }
        catch {
            case e: Exception =>
                igniteCfg.setClientMode(true)

                Ignition.start(igniteCfg)
        }
    }

    def igniteCache(): IgniteCache[K, V] = {
//        new IgniteRDD[Object, K, V](this, (k: K, v: V) => {true})

        ignite().cache(cacheName)
    }

    private def affinityKeyFunc(ic: IgniteContext[K, V], key: K, node: ClusterNode) = {
        null
    }
}
