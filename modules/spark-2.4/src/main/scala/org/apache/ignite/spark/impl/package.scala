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

import org.apache.commons.lang.StringUtils.equalsIgnoreCase
import org.apache.ignite.cache.CacheMode
import org.apache.ignite.cluster.ClusterNode
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.internal.IgniteEx
import org.apache.ignite.internal.processors.query.{GridQueryTypeDescriptor, QueryTypeDescriptorImpl}
import org.apache.ignite.internal.processors.query.QueryUtils.normalizeSchemaName
import org.apache.ignite.internal.util.lang.GridFunc.contains
import org.apache.ignite.{Ignite, Ignition}
import org.apache.spark.Partition
import org.apache.spark.sql.catalyst.catalog.SessionCatalog

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

package object impl {
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
      * @param schema Name of schema.
      * @param default Default schema.
      * @return Schema to use.
      */
    def schemaOrDefault(schema: String, default: String): String =
        if (schema == SessionCatalog.DEFAULT_DATABASE)
            default
        else
            schema

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
      * @param schemaName Optional schema name.
      * @return True if table exists false otherwise.
      */
    def sqlTableExists(ignite: Ignite, tabName: String, schemaName: Option[String]): Boolean =
        sqlTableInfo(ignite, tabName, schemaName).isDefined

    /**
      * @param ignite Ignite instance.
      * @param tabName Table name.
      * @param schemaName Optional schema name.
      * @return Cache name for given table.
      */
    def sqlCacheName(ignite: Ignite, tabName: String, schemaName: Option[String]): Option[String] =
        sqlTableInfo(ignite, tabName, schemaName).map(_.asInstanceOf[QueryTypeDescriptorImpl].cacheName)

    /**
      * @param ignite Ignite instance.
      * @return All schemas in given Ignite instance.
      */
    def allSchemas(ignite: Ignite): Seq[String] = ignite.cacheNames
        .map(name =>
            normalizeSchemaName(name,
                ignite.cache[Any,Any](name).getConfiguration(classOf[CacheConfiguration[Any,Any]]).getSqlSchema))
        .toSeq
        .distinct

    /**
      * @param ignite Ignite instance.
      * @param schemaName Schema name.
      * @return All cache configurations for the given schema.
      */
    def cachesForSchema[K,V](ignite: Ignite, schemaName: Option[String]): Seq[CacheConfiguration[K,V]] =
        ignite.cacheNames
            .map(ignite.cache[K,V](_).getConfiguration(classOf[CacheConfiguration[K,V]]))
            .filter(ccfg =>
                schemaName.forall(normalizeSchemaName(ccfg.getName, ccfg.getSqlSchema).equalsIgnoreCase(_)) ||
                schemaName.contains(SessionCatalog.DEFAULT_DATABASE))
            .toSeq

    /**
      * @param ignite Ignite instance.
      * @param tabName Table name.
      * @param schemaName Optional schema name.
      * @return GridQueryTypeDescriptor for a given table.
      */
    def sqlTableInfo(ignite: Ignite, tabName: String, schemaName: Option[String]): Option[GridQueryTypeDescriptor] =
        ignite.asInstanceOf[IgniteEx].context.cache.publicCacheNames
            .flatMap(cacheName => ignite.asInstanceOf[IgniteEx].context.query.types(cacheName))
            .find(table => table.tableName.equalsIgnoreCase(tabName) && isValidSchema(table, schemaName))

    /**
      * @param table GridQueryTypeDescriptor for a given table.
      * @param schemaName Optional schema name.
      * @return `True` if schema is valid.
      */
    def isValidSchema(table: GridQueryTypeDescriptor, schemaName: Option[String]): Boolean =
        schemaName match {
            case Some(schema) =>
                schema.equalsIgnoreCase(table.schemaName) || schema.equals(SessionCatalog.DEFAULT_DATABASE)
            case None =>
                true
        }

    /**
      * @param table Table.
      * @param column Column name.
      * @return `True` if column is key.
      */
    def isKeyColumn(table: GridQueryTypeDescriptor, column: String): Boolean =
        contains(allKeyFields(table), column) || equalsIgnoreCase(table.keyFieldName, column)

    /**
      * @param table Table.
      * @return All the key fields in a Set.
      */
    def allKeyFields(table: GridQueryTypeDescriptor): scala.collection.Set[String] =
        table.fields.filter(entry => table.property(entry._1).key).keySet

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
