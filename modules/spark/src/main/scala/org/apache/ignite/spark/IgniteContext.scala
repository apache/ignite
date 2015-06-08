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
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.sql.SQLContext

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
    cfgF: () ⇒ IgniteConfiguration,
    client: Boolean = true
) extends Serializable with Logging {
    @scala.transient private val driver = true

    if (!client) {
        val workers = sparkContext.getExecutorStorageStatus.length - 1

        if (workers <= 0)
            throw new IllegalStateException("No Spark executors found to start Ignite nodes.")

        logInfo("Will start Ignite nodes on " + workers + " workers")

        // Start ignite server node on each worker in server mode.
        sparkContext.parallelize(1 to workers, workers).foreach(it ⇒ ignite())
    }

    def this(
        sc: SparkContext,
        springUrl: String
    ) {
        this(sc, () ⇒ IgnitionEx.loadConfiguration(springUrl).get1())
    }

    val sqlContext = new SQLContext(sparkContext)

    /**
     * Creates an `IgniteRDD` instance from the given cache name. If the cache does not exist, it will be
     * automatically started from template on the first invoked RDD action.
     *
     * @param cacheName Cache name.
     * @return `IgniteRDD` instance.
     */
    def fromCache(cacheName: String): IgniteRDD[K, V] = {
        new IgniteRDD[K, V](this, cacheName, null)
    }

    /**
     * Creates an `IgniteRDD` instance from the given cache configuration. If the cache does not exist, it will be
     * automatically started using the configuration provided on the first invoked RDD action.
     *
     * @param cacheCfg Cache configuration to use.
     * @return `IgniteRDD` instance.
     */
    def fromCache(cacheCfg: CacheConfiguration[K, V]) = {
        new IgniteRDD[K, V](this, cacheCfg.getName, cacheCfg)
    }

    /**
     * Gets an Ignite instance supporting this context. Ignite instance will be started
     * if it has not been started yet.
     *
     * @return Ignite instance.
     */
    def ignite(): Ignite = {
        val igniteCfg = cfgF()

        try {
            Ignition.ignite(igniteCfg.getGridName)
        }
        catch {
            case e: Exception ⇒
                try {
                    igniteCfg.setClientMode(client || driver)

                    Ignition.start(igniteCfg)
                }
                catch {
                    case e: Exception ⇒ Ignition.ignite(igniteCfg.getGridName)
                }
        }
    }

    /**
     * Stops supporting ignite instance. If ignite instance has been already stopped, this operation will be
     * a no-op.
     */
    def close() = {
        val igniteCfg = cfgF()

        Ignition.stop(igniteCfg.getGridName, false)
    }
}
