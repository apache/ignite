/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.apache.spark.sql.catalyst.catalog.{ExternalCatalog, ExternalCatalogEvent, ExternalCatalogEventListener}
import org.apache.spark.sql.internal.SharedState

/**
  * Shared state to override link to IgniteExternalCatalog
  */
private[ignite] class IgniteSharedState (
    igniteContext: IgniteContext,
    sparkContext: SparkContext) extends SharedState(sparkContext) {
    /** @inheritdoc */
    override lazy val externalCatalog: ExternalCatalog = {
        val externalCatalog = new IgniteExternalCatalog(igniteContext)

        externalCatalog.addListener(new ExternalCatalogEventListener {
            override def onEvent(event: ExternalCatalogEvent): Unit = {
                sparkContext.listenerBus.post(event)
            }
        })

        externalCatalog
    }
}
