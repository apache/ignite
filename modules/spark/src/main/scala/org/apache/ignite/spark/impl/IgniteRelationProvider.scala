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

package org.apache.ignite.spark.impl

import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.internal.IgnitionEx
import org.apache.ignite.internal.util.IgniteUtils
import org.apache.ignite.spark.IgniteDataFrameOptions.TABLE
import org.apache.ignite.spark.IgniteContext
import org.apache.ignite.spark.IgniteDataFrameOptions._
import org.apache.ignite.{IgniteException, Ignition}
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._

/**
  * Apache Ignite relation provider.
  */
class IgniteRelationProvider extends RelationProvider with DataSourceRegister {
    /**
      * @return "ignite" - name of relation provider.
      */
    override def shortName(): String = IGNITE

    /**
      * To create IgniteRelation we need a link to a ignite cluster and a table name.
      * To refer cluster user have to specify one of config parameter:
      * <ul>
      *     <li><code>config</code> - path to ignite configuration file.
      *     <li><code>grid</code> - grid name. Note that grid has to be started in the same jvm.
      * <ul>
      * Existing table inside Apache Ignite should be referred via <code>table</code> parameter.
      *
      * @param sqlCtx SQLContext.
      * @param params Parameters for relation creation.
      * @return IgniteRelation.
      * @see IgniteRelation
      * @see IgnitionEx#grid(String)
      */
    override def createRelation(sqlCtx: SQLContext, params: Map[String, String]): BaseRelation = {
        val igniteHome = IgniteUtils.getIgniteHome

        def configProvider: () ⇒ IgniteConfiguration = {
            if (params.contains(CONFIG_FILE))
                () ⇒ {
                    IgniteContext.setIgniteHome(igniteHome)

                    val cfg = IgnitionEx.loadConfiguration(params(CONFIG_FILE)).get1()

                    cfg.setClientMode(true)

                    cfg
                }
            else if (params.contains(GRID))
                () ⇒ {
                    IgniteContext.setIgniteHome(igniteHome)

                    val cfg = ignite(params(GRID)).configuration()

                    cfg.setClientMode(true)

                    cfg
                }
            else
                throw new IgniteException("'config' or 'grid' must be specified to connect to ignite cluster.")
        }

        val cfg = configProvider()

        sqlCtx.sparkContext.addSparkListener(new SparkListener {
            override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
                Ignition.stop(cfg.getIgniteInstanceName, true)
            }
        })

        val ic = IgniteContext(sqlCtx.sparkContext, configProvider)

        if (params.contains(TABLE))
            IgniteSQLRelation(ic, params(TABLE).toUpperCase, sqlCtx)
        else if (params.contains(CACHE)) {
            val cache = params(CACHE)

            if (!params.contains(KEY_CLASS) || !params.contains(VALUE_CLASS))
                throw new IgniteException("'keyClass' and 'valueClass' must be specified for a 'cache'")

            val keepBinary = params.getOrElse(KEEP_BINARY, "true").toBoolean

            IgniteCacheRelation(ic, cache, Class.forName(params(KEY_CLASS)),
                Class.forName(params(VALUE_CLASS)), keepBinary, sqlCtx)
        }
        else
            throw new IgniteException("'table' or 'cache' must be specified for loading ignite data.")
    }
}
