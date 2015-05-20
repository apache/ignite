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

import javax.cache.Cache

import org.apache.ignite.cache.query.{Query, ScanQuery}
import org.apache.ignite.cluster.ClusterNode
import org.apache.ignite.internal.IgnitionEx
import org.apache.ignite.lang.IgniteUuid
import org.apache.ignite.spark.util.SerializablePredicate2
import org.apache.ignite.{Ignition, IgniteCache, Ignite}
import org.apache.ignite.configuration.{CacheConfiguration, IgniteConfiguration}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

class IgniteContext[K, V](
    @scala.transient sc: SparkContext,
    cfgF: () => IgniteConfiguration,
    val cacheName: String,
    cacheCfg: CacheConfiguration[K, V]
) extends Serializable {
    type ScanRDD[K1, V1] = IgniteRDD[Cache.Entry[K1, V1], K1, V1]

    def this(
        sc: SparkContext,
        springUrl: String,
        cacheName: String
    ) {
        this(sc, () => IgnitionEx.loadConfiguration(springUrl).get1(), cacheName, null)
    }

    def this(
        sc: SparkContext,
        cfgF: () => IgniteConfiguration,
        cacheName: String
    ) {
        this(sc, cfgF, cacheName, null)
    }

    def this(
        sc: SparkContext,
        cfgF: () => IgniteConfiguration,
        cacheCfg: CacheConfiguration[K, V]
    ) {
        this(sc, cfgF, cacheCfg.getName, cacheCfg)
    }

    def this(
        sc: SparkContext,
        springUrl: String,
        cacheCfg: CacheConfiguration[K, V]
    ) {
        this(sc, () => IgnitionEx.loadConfiguration(springUrl).get1(), cacheCfg.getName, cacheCfg)
    }

    def sparkContext() = sc

    def scan(p: (K, V) => Boolean = (_, _) => true): ScanRDD[K, V] = {
        new ScanRDD(this, new ScanQuery[K, V](new SerializablePredicate2[K, V](p)))
    }

    def scan[R:ClassTag](qry: Query[R]): IgniteRDD[R, K, V] = {
        new IgniteRDD[R, K, V](this, qry)
    }

    def saveToIgnite[T](rdd: RDD[V], keyFunc: (IgniteContext[K, V], V, ClusterNode) => T = affinityKeyFunc(_: IgniteContext[K, V], _:V, _: ClusterNode)) = {
        rdd.foreachPartition(it => {
            println("Using scala version: " + scala.util.Properties.versionString)
            // Make sure to deploy the cache
            igniteCache()

            val ig = ignite()

            val locNode = ig.cluster().localNode()

            val node: Option[ClusterNode] = ig.cluster().forHost(locNode).nodes().find(!_.eq(locNode))

            val streamer = ignite().dataStreamer[T, V](cacheName)

            try {
                it.foreach(value => {
                    val key: T = keyFunc(this, value, node.orNull)

                    println("Saving: " + key + ", " + value)

                    streamer.addData(key, value)
                })
            }
            finally {
                streamer.close()
            }
        })
    }

    def ignite(): Ignite = {
        val igniteCfg = cfgF()

        try {
            Ignition.ignite(igniteCfg.getGridName)
        }
        catch {
            case e: Exception =>
                try {
                    igniteCfg.setClientMode(true)

                    Ignition.start(igniteCfg)
                }
                catch {
                    case e: Exception => Ignition.ignite(igniteCfg.getGridName)
                }
        }
    }

    private def igniteCache(): IgniteCache[K, V] = {
        if (cacheCfg == null)
            ignite().getOrCreateCache(cacheName)
        else
            ignite().getOrCreateCache(cacheCfg)
    }

    private def affinityKeyFunc(ic: IgniteContext[K, V], value: V, node: ClusterNode): Object = {
        IgniteUuid.randomUuid()
    }
}
