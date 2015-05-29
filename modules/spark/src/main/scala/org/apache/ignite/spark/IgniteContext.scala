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


import org.apache.ignite.internal.IgnitionEx
import org.apache.ignite.{Ignition, Ignite}
import org.apache.ignite.configuration.{CacheConfiguration, IgniteConfiguration}
import org.apache.spark.SparkContext

/**
 * Ignite context.
 *
 * @param sparkContext Spark context.
 * @param cfgF Configuration factory.
 * @tparam K Key type.
 * @tparam V Value type.
 */
class IgniteContext[K, V](
    @scala.transient val sparkContext: SparkContext,
    cfgF: () ⇒ IgniteConfiguration
) extends Serializable {
    def this(
        sc: SparkContext,
        springUrl: String
    ) {
        this(sc, () ⇒ IgnitionEx.loadConfiguration(springUrl).get1())
    }

    def fromCache(cacheName: String): IgniteRDD[K, V] = {
        new IgniteRDD[K, V](this, cacheName, null)
    }

    def fromCache(cacheCfg: CacheConfiguration[K, V]) = {
        new IgniteRDD[K, V](this, cacheCfg.getName, cacheCfg)
    }

    def ignite(): Ignite = {
        val igniteCfg = cfgF()

        try {
            Ignition.ignite(igniteCfg.getGridName)
        }
        catch {
            case e: Exception ⇒
                try {
                    igniteCfg.setClientMode(true)

                    Ignition.start(igniteCfg)
                }
                catch {
                    case e: Exception ⇒ Ignition.ignite(igniteCfg.getGridName)
                }
        }
    }

    def close() = {
        val igniteCfg = cfgF()

        Ignition.stop(igniteCfg.getGridName, false)
    }
}
