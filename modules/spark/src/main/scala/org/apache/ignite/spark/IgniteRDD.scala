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

import javax.cache.Cache

import org.apache.ignite.cache.query.{SqlFieldsQuery, SqlQuery, ScanQuery}
import org.apache.ignite.cluster.ClusterNode
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.lang.IgniteUuid
import org.apache.ignite.spark.impl.{IgniteAbstractRDD, IgniteSqlRDD, IgnitePartition, IgniteQueryIterator}
import org.apache.spark.rdd.RDD
import org.apache.spark.{TaskContext, Partition}

import scala.collection.JavaConversions._

class IgniteRDD[K, V] (
    ic: IgniteContext[K, V],
    cacheName: String,
    cacheCfg: CacheConfiguration[K, V]
) extends IgniteAbstractRDD[(K, V), K, V] (ic, cacheName, cacheCfg) {

    override def compute(part: Partition, context: TaskContext): Iterator[(K, V)] = {
        val cache = ensureCache()

        val qry: ScanQuery[K, V] = new ScanQuery[K, V]()

        qry.setPartition(part.index)

        val it: java.util.Iterator[Cache.Entry[K, V]] = cache.query(qry).iterator()

        new IgniteQueryIterator[Cache.Entry[K, V], (K, V)](it, entry => {
            (entry.getKey, entry.getValue)
        })
    }

    override protected def getPartitions: Array[Partition] = {
        ensureCache()

        val parts = ic.ignite().affinity(cacheName).partitions()

        (0 until parts).map(new IgnitePartition(_)).toArray
    }

    override protected def getPreferredLocations(split: Partition): Seq[String] = {
        ensureCache()

        ic.ignite().affinity(cacheName).mapPartitionToPrimaryAndBackups(split.index).map(_.addresses()).flatten.toList
    }

    def objectSql(typeName: String, sql: String, args: Any*): RDD[(K, V)] = {
        val qry: SqlQuery[K, V] = new SqlQuery[K, V](typeName, sql)

        qry.setArgs(args.map(_.asInstanceOf[Object]):_*)

        new IgniteSqlRDD[(K, V), Cache.Entry[K, V], K, V](ic, cacheName, cacheCfg, qry, entry => (entry.getKey, entry.getValue))
    }

    def sql(sql: String, args: Any*): RDD[Seq[Any]] = {
        val qry = new SqlFieldsQuery(sql)

        qry.setArgs(args.map(_.asInstanceOf[Object]):_*)

        new IgniteSqlRDD[Seq[Any], java.util.List[_], K, V](ic, cacheName, cacheCfg, qry, list => list)
    }

    def saveValues(rdd: RDD[V]) = {
        rdd.foreachPartition(it => {
            val ig = ic.ignite()

            ensureCache()

            val locNode = ig.cluster().localNode()

            val node: Option[ClusterNode] = ig.cluster().forHost(locNode).nodes().find(!_.eq(locNode))

            val streamer = ig.dataStreamer[Object, V](cacheName)

            try {
                it.foreach(value => {
                    val key = affinityKeyFunc(value, node.orNull)

                    streamer.addData(key, value)
                })
            }
            finally {
                streamer.close()
            }
        })
    }

    def saveTuples(rdd: RDD[(K, V)]) = {
        rdd.foreachPartition(it => {
            val ig = ic.ignite()

            // Make sure to deploy the cache
            ensureCache()

            val locNode = ig.cluster().localNode()

            val node: Option[ClusterNode] = ig.cluster().forHost(locNode).nodes().find(!_.eq(locNode))

            val streamer = ig.dataStreamer[K, V](cacheName)

            try {
                it.foreach(tup => {
                    streamer.addData(tup._1, tup._2)
                })
            }
            finally {
                streamer.close()
            }
        })
    }

    private def affinityKeyFunc(value: V, node: ClusterNode): Object = {
        IgniteUuid.randomUuid()
    }
}
