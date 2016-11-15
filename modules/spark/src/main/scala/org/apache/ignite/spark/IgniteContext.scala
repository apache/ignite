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

import org.apache.ignite._
import org.apache.ignite.configuration.{CacheConfiguration, IgniteConfiguration}
import org.apache.ignite.internal.IgnitionEx
import org.apache.ignite.internal.util.IgniteUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{Logging, SparkContext}

/**
 * Ignite context.
 *
 * @param sparkContext Spark context.
 * @param cfgF Configuration factory.
 */
class IgniteContext(
    @transient val sparkContext: SparkContext,
    cfgF: () ⇒ IgniteConfiguration,
    standalone: Boolean = true
    ) extends Serializable with Logging {
    private val cfgClo = new Once(cfgF)

    private val igniteHome = IgniteUtils.getIgniteHome

    if (!standalone) {
        // Get required number of executors with default equals to number of available executors.
        val workers = sparkContext.getConf.getInt("spark.executor.instances",
            sparkContext.getExecutorStorageStatus.length)

        if (workers <= 0)
            throw new IllegalStateException("No Spark executors found to start Ignite nodes.")

        logInfo("Will start Ignite nodes on " + workers + " workers")

        // Start ignite server node on each worker in server mode.
        sparkContext.parallelize(1 to workers, workers).foreachPartition(it ⇒ ignite())
    }

    // Make sure to start Ignite on context creation.
    ignite()

    /**
     * Creates an instance of IgniteContext with the given spring configuration.
     *
     * @param sc Spark context.
     * @param springUrl Spring configuration path.
     */
    def this(
        sc: SparkContext,
        springUrl: String,
        client: Boolean
        ) {
        this(sc, () ⇒ IgnitionEx.loadConfiguration(springUrl).get1(), client)
    }

    /**
     * Creates an instance of IgniteContext with the given spring configuration.
     *
     * @param sc Spark context.
     * @param springUrl Spring configuration path.
     */
    def this(
        sc: SparkContext,
        springUrl: String
        ) {
        this(sc, () ⇒ IgnitionEx.loadConfiguration(springUrl).get1())
    }

    /**
     * Creates an instance of IgniteContext with default Ignite configuration.
     * By default this method will use grid configuration defined in `IGNITE_HOME/config/default-config.xml`
     * configuration file.
     *
     * @param sc Spark context.
     */
    def this(sc: SparkContext) {
        this(sc, IgnitionEx.DFLT_CFG)
    }

    val sqlContext = new SQLContext(sparkContext)

    /**
     * Creates an `IgniteRDD` instance from the given cache name. If the cache does not exist, it will be
     * automatically started from template on the first invoked RDD action.
     *
     * @param cacheName Cache name.
     * @return `IgniteRDD` instance.
     */
    def fromCache[K, V](cacheName: String): IgniteRDD[K, V] = {
        new IgniteRDD[K, V](this, cacheName, null, false)
    }

    /**
     * Creates an `IgniteRDD` instance from the given cache configuration. If the cache does not exist, it will be
     * automatically started using the configuration provided on the first invoked RDD action.
     *
     * @param cacheCfg Cache configuration to use.
     * @return `IgniteRDD` instance.
     */
    def fromCache[K, V](cacheCfg: CacheConfiguration[K, V]) = {
        new IgniteRDD[K, V](this, cacheCfg.getName, cacheCfg, false)
    }

    /**
     * Get or start Ignite instance it it's not started yet.
     * @return
     */
    def ignite(): Ignite = {
        val home = IgniteUtils.getIgniteHome

        if (home == null && igniteHome != null) {
            logInfo("Setting IGNITE_HOME from driver not as it is not available on this worker: " + igniteHome)

            IgniteUtils.nullifyHomeDirectory()

            System.setProperty(IgniteSystemProperties.IGNITE_HOME, igniteHome)
        }

        val igniteCfg = cfgClo()

        // check if called from driver
        if (sparkContext != null) igniteCfg.setClientMode(true)

        try {
            Ignition.getOrStart(igniteCfg)
        }
        catch {
            case e: IgniteException ⇒
                logError("Failed to start Ignite.", e)

                throw e
        }
    }

    /**
     * Stops supporting ignite instance. If ignite instance has been already stopped, this operation will be
     * a no-op.
     */
    def close(shutdownIgniteOnWorkers: Boolean = false) = {
        // additional check if called from driver
        if (sparkContext != null && shutdownIgniteOnWorkers) {
            // Get required number of executors with default equals to number of available executors.
            val workers = sparkContext.getConf.getInt("spark.executor.instances",
                sparkContext.getExecutorStorageStatus.length)

            if (workers > 0) {
                logInfo("Will stop Ignite nodes on " + workers + " workers")

                // Start ignite server node on each worker in server mode.
                sparkContext.parallelize(1 to workers, workers).foreachPartition(it ⇒ doClose())
            }
        }

        doClose()
    }

    private def doClose() = {
        val igniteCfg = cfgClo()

        Ignition.stop(igniteCfg.getGridName, false)
    }
}

/**
 * Auxiliary closure that ensures that passed in closure is executed only once.
 *
 * @param clo Closure to wrap.
 */
private class Once(clo: () ⇒ IgniteConfiguration) extends Serializable {
    @transient @volatile var res: IgniteConfiguration = null

    def apply(): IgniteConfiguration = {
        if (res == null) {

            this.synchronized {

                if (res == null)

                    res = clo()
            }
        }

        res
    }
}
