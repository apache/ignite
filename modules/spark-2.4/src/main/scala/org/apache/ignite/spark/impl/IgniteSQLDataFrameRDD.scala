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
import org.apache.ignite.spark.{IgniteContext, IgniteRDD}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Partition, TaskContext}
import java.util.{List ⇒ JList}

/**
  * Implementation of Spark RDD for Apache Ignite to support Data Frame API.
  */
class IgniteSQLDataFrameRDD[K, V](
    ic: IgniteContext,
    cacheName: String,
    schema: StructType,
    qryStr: String,
    args: List[_],
    parts: Array[Partition],
    distributedJoin: Boolean) extends
    IgniteSqlRDD[Row, JList[_], K, V](
        ic,
        cacheName,
        cacheCfg = null,
        qry = null,
        r ⇒ new GenericRowWithSchema(r.toArray.map(IgniteRDD.convertIfNeeded), schema),
        keepBinary = true,
        parts) {

    /**
      * Executes an Ignite query for this RDD and return Iterator to iterate throw results.
      *
      * @param partition Partition.
      * @param context   TaskContext.
      * @return Results of query for specific partition.
      */
    override def compute(partition: Partition, context: TaskContext): Iterator[Row] = {
        val qry0 = new SqlFieldsQuery(qryStr)

        qry0.setDistributedJoins(distributedJoin)

        if (args.nonEmpty)
            qry0.setArgs(args.map(_.asInstanceOf[Object]): _*)

        val ccfg = ic.ignite().cache[K, V](cacheName).getConfiguration(classOf[CacheConfiguration[K, V]])

        val ignitePartition = partition.asInstanceOf[IgniteDataFramePartition]

        if (ccfg.getCacheMode != CacheMode.REPLICATED && ignitePartition.igniteParts.nonEmpty && !distributedJoin)
            qry0.setPartitions(ignitePartition.igniteParts: _*)

        qry = qry0

        super.compute(partition, context)
    }
}

object IgniteSQLDataFrameRDD {
    def apply[K, V](ic: IgniteContext,
        cacheName: String,
        schema: StructType,
        qryStr: String,
        args: List[_],
        parts: Array[Partition] = Array(IgnitePartition(0)),
        distributedJoin: Boolean = false): IgniteSQLDataFrameRDD[K, V] = {
        new IgniteSQLDataFrameRDD[K, V](ic, cacheName, schema, qryStr, args, parts, distributedJoin)
    }
}
