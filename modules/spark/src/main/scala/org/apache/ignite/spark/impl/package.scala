/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.spark

import org.apache.commons.lang.StringUtils.equalsIgnoreCase
import org.apache.ignite.{Ignite, IgniteException, IgniteState, Ignition}
import org.apache.ignite.cache.{CacheMode, QueryEntity}
import org.apache.ignite.cluster.ClusterNode
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.internal.util.lang.GridFunc.contains
import org.apache.spark.Partition
import org.apache.spark.sql.catalyst.catalog.SessionCatalog

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

package object impl {
    /**
      * Checks named instance of Ignite exists.
      * Throws IgniteException if not.
      *
      * @param gridName Name of grid.
      */
    def ensureIgnite(gridName: String): Unit =
        if (!igniteExists(gridName))
            throw new IgniteException(s"Ignite grid with name '$gridName' does not exist.")

    /**
      * @param gridName Name of grid.
      * @return True if named instance of Ignite exists false otherwise.
      */
    def igniteExists(gridName: String): Boolean =
        if (gridName == "")
            Ignition.state() == IgniteState.STARTED
        else
            Ignition.state(gridName) == IgniteState.STARTED

    /**
      * @param g Ignite.
      * @return Name of Ignite. If name is null empty string returned.
      */
    def igniteName(g: Ignite): String =
        if(g.name() != null)
            g.name
        else
            ""

    /**
      * @param name Name of grid..
      * @param default Default instance.
      * @return Named grid instance if it exists. If not default instance returned.
      */
    def igniteOrDefault(name: String, default: Ignite): Ignite =
        if (name == SessionCatalog.DEFAULT_DATABASE) {
            if (igniteExists(name))
                ignite(name)
            else
                default
        }
        else
            ignite(name)

    /**
      * @param gridName Name of grid.
      * @return Named instance of grid. If 'gridName' is empty unnamed instance returned.
      */
    def ignite(gridName: String): Ignite =
        if (gridName == "")
            Ignition.ignite()
        else
            Ignition.ignite(gridName)

    /**
      * @param ignite Ignite instance.
      * @param tabName Table name.
      * @return True if table exists false otherwise.
      */
    def sqlTableExists(ignite: Ignite, tabName: String): Boolean =
        sqlTableInfo(ignite, tabName).isDefined

    /**
      * @param ignite Ignite instance.
      * @param tabName Table name.
      * @return QueryEntity for a given table.
      */
    def igniteSQLTable(ignite: Ignite, tabName: String): Option[QueryEntity] =
        sqlTableInfo[Any, Any](ignite, tabName).map(_._2)

    /**
      * @param ignite Ignite instance.
      * @param tabName Table name.
      * @return Cache name for given table.
      */
    def sqlCacheName(ignite: Ignite, tabName: String): Option[String] =
        sqlTableInfo[Any, Any](ignite, tabName).map(_._1.getName)

    /**
      * @param ignite Ignite instance.
      * @param tabName Table name.
      * @tparam K Key class.
      * @tparam V Value class.
      * @return CacheConfiguration and QueryEntity for a given table.
      */
    def sqlTableInfo[K, V](ignite: Ignite, tabName: String): Option[(CacheConfiguration[K, V], QueryEntity)] =
        ignite.cacheNames().map { cacheName ⇒
            val ccfg = ignite.cache[K, V](cacheName).getConfiguration(classOf[CacheConfiguration[K, V]])

            val queryEntities = ccfg.getQueryEntities

            queryEntities.find(_.getTableName.equalsIgnoreCase(tabName)).map(qe ⇒ (ccfg, qe))
        }.find(_.isDefined).flatten

    /**
      * @param table Table.
      * @param column Column name.
      * @return `True` if column is key.
      */
    def isKeyColumn(table: QueryEntity, column: String): Boolean =
        contains(table.getKeyFields, column) || equalsIgnoreCase(table.getKeyFieldName, column)

    /**
      * Computes spark partitions for a given cache.
      *
      * @param ic Ignite context.
      * @param cacheName Cache name
      * @return Array of IgniteDataFramPartition
      */
    def calcPartitions(ic: IgniteContext, cacheName: String): Array[Partition] = {
        val cache = ic.ignite().cache[Any, Any](cacheName)

        val ccfg = cache.getConfiguration(classOf[CacheConfiguration[Any, Any]])

        if (ccfg.getCacheMode == CacheMode.REPLICATED) {
            val serverNodes = ic.ignite().cluster().forCacheNodes(cacheName).forServers().nodes()

            Array(IgniteDataFramePartition(0, serverNodes.head, Stream.from(0).take(1024).toList))
        }
        else {
            val aff = ic.ignite().affinity(cacheName)

            val parts = aff.partitions()

            val nodesToParts = (0 until parts).foldLeft(Map[ClusterNode, ArrayBuffer[Int]]()) {
                case (nodeToParts, ignitePartIdx) ⇒
                    val primary = aff.mapPartitionToPrimaryAndBackups(ignitePartIdx).head

                    if (nodeToParts.contains(primary)) {
                        nodeToParts(primary) += ignitePartIdx

                        nodeToParts
                    }
                    else
                        nodeToParts + (primary → ArrayBuffer[Int](ignitePartIdx))
            }

            val partitions = nodesToParts.toIndexedSeq.zipWithIndex.map { case ((node, nodesParts), i) ⇒
                IgniteDataFramePartition(i, node, nodesParts.toList)
            }

            partitions.toArray
        }
    }
}
