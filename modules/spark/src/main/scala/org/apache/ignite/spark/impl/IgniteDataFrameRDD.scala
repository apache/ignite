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

import org.apache.ignite.cache.CacheMode
import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.spark.IgniteContext
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Partition, TaskContext}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
  * Implementation of Spark RDD for Apache Ignite to support Data Frame API.
  */
class IgniteDataFrameRDD(
    ctx: IgniteContext,
    cacheName: String,
    schema: StructType,
    query: String,
    args: List[Any]) extends RDD[Row](ctx.sparkContext, Nil) {

    /**
      *
      * @param partition Partition.
      * @param context   TaskContext.
      * @return Results of query for specific partition.
      */
    override def compute(partition: Partition, context: TaskContext): Iterator[Row] = {
        val ignite = ctx.ignite()

        val cache = ignite.cache[Any, Any](cacheName)

        val qry = new SqlFieldsQuery(query)
        if (args.nonEmpty)
            qry.setArgs(args.map(_.asInstanceOf[AnyRef]): _*)

        val ccfg = cache.getConfiguration(classOf[CacheConfiguration[Any, Any]])

        if (ccfg.getCacheMode != CacheMode.REPLICATED)
            qry.setPartitions(partition.index)

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
        val ignite = ctx.ignite()

        val cache = ignite.cache[Any, Any](cacheName)

        val ccfg = cache.getConfiguration(classOf[CacheConfiguration[Any, Any]])

        if (ccfg.getCacheMode == CacheMode.REPLICATED) {
            Array(IgnitePartition(0))
        }
        else {
            val parts = ignite.affinity(cache.getName).partitions()

            (0 until parts).map(IgnitePartition).toArray
        }
    }

    /**
      * Gets preferred locations for the given partition.
      *
      * @param split Split partition.
      * @return Sequence of preferred locations.
      */
    override protected[spark] def getPreferredLocations(split: Partition): Seq[String] = {
        val ignite = ctx.ignite()

        ignite.affinity(cacheName).mapPartitionToPrimaryAndBackups(split.index)
            .flatMap(_.asInstanceOf[TcpDiscoveryNode].socketAddresses()).map(_.getHostName).toList
    }

}

object IgniteDataFrameRDD {
    def apply(ctx: IgniteContext, cacheName: String, schema: StructType, query: String,
        arguments: List[Any]) =
        new IgniteDataFrameRDD(ctx, cacheName, schema, query, arguments)
}
