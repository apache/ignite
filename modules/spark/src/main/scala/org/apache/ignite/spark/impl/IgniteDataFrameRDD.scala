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

import org.apache.ignite.Ignition
import org.apache.ignite.cache.CacheMode
import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.cluster.ClusterNode
import org.apache.ignite.configuration.{CacheConfiguration, IgniteConfiguration}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Implementation of Spark RDD for Apache Ignite to support Data Frame API.
  */
class IgniteDataFrameRDD(
    @transient sparkContext: SparkContext,
    cfg: () ⇒ IgniteConfiguration,
    cacheName: String,
    schema: StructType,
    query: String,
    args: List[Any]) extends RDD[Row](sparkContext, Nil) with Logging {

    /**
      *
      * @param partition Partition.
      * @param context   TaskContext.
      * @return Results of query for specific partition.
      */
    override def compute(partition: Partition, context: TaskContext): Iterator[Row] = {
        val igniteDataFramePartition = partition.asInstanceOf[IgniteDataFramePartition]

        val config = cfg()

        val ignite = Ignition.getOrStart(config)

        val cache = ignite.cache[Any, Any](cacheName)

        val qry = new SqlFieldsQuery(query)

        if (args.nonEmpty)
            qry.setArgs(args.map(_.asInstanceOf[Object]): _*)

        val ccfg = cache.getConfiguration(classOf[CacheConfiguration[Any, Any]])

        if (ccfg.getCacheMode != CacheMode.REPLICATED)
            qry.setPartitions(igniteDataFramePartition.ignitePartitions:_*)

        val cur = cache.query(qry)

        TaskContext.get().addTaskCompletionListener((_) ⇒ cur.close())

        val iter = cur.iterator()

        new Iterator[Row]() {
            override def hasNext = iter.hasNext

            override def next() =
                new GenericRowWithSchema(iter.next.asScala.toArray, schema)
        }
    }

    /**
      * @return Array of IgnitePartition for this RDD
      */
    override protected def getPartitions: Array[Partition] = {
        val ignite = Ignition.getOrStart(cfg())

        val cache = ignite.cache[Any, Any](cacheName)

        val ccfg = cache.getConfiguration(classOf[CacheConfiguration[Any, Any]])

        if (ccfg.getCacheMode == CacheMode.REPLICATED) {
            val serverNodes = ignite.cluster().forCacheNodes(cacheName).forServers().nodes().asScala

            Array(IgniteDataFramePartition(0, serverNodes.iterator.next, Stream.from(1).take(1024).toList))
        }
        else {
            val parts = ignite.affinity(cache.getName).partitions()
            val aff = ignite.affinity(cacheName)

            val nodesToPartitions = (0 until parts).foldLeft(Map[ClusterNode, ArrayBuffer[Int]]()) {
                case(nodeToPartitions, ignitePartitionIdx) ⇒
                    val primary = aff.mapPartitionToPrimaryAndBackups(ignitePartitionIdx).iterator().next()

                    if (nodeToPartitions.contains(primary)) {
                        nodeToPartitions(primary) += ignitePartitionIdx

                        nodeToPartitions
                    } else
                        nodeToPartitions + (primary → ArrayBuffer[Int](ignitePartitionIdx))
            }

            val partitions = nodesToPartitions.zipWithIndex.map { case ((node, partitions), i) ⇒
                IgniteDataFramePartition(i, node, partitions.toList)
            }

            partitions.toArray
        }
    }
}

object IgniteDataFrameRDD {
    def apply(sparkContext: SparkContext, cfg: () ⇒ IgniteConfiguration, cacheName: String, schema: StructType, query: String,
        arguments: List[Any]) =
        new IgniteDataFrameRDD(sparkContext, cfg, cacheName, schema, query, arguments)
}
