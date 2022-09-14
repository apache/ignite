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

package org.apache.spark.sql.ignite

import org.apache.ignite.spark.IgniteContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.catalog.{ExternalCatalogEvent, ExternalCatalogEventListener, ExternalCatalogWithListener}
import org.apache.spark.sql.internal.SharedState

/**
  * Shared state to override link to IgniteExternalCatalog
  */
private[ignite] class IgniteSharedState (
    igniteContext: IgniteContext,
    sparkContext: SparkContext) extends SharedState(sparkContext) {
    /** @inheritdoc */
    override lazy val externalCatalog: ExternalCatalogWithListener = {
        val externalCatalog = new IgniteExternalCatalog(igniteContext)

        val wrapped = new ExternalCatalogWithListener(externalCatalog)

        wrapped.addListener(new ExternalCatalogEventListener {
            override def onEvent(event: ExternalCatalogEvent): Unit = {
                sparkContext.listenerBus.post(event)
            }
        })

        wrapped
    }
}
