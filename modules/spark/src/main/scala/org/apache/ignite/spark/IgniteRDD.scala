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

import org.apache.ignite.cache.query.{ScanQuery, Query}
import org.apache.ignite.scalar.lang.ScalarPredicate2
import org.apache.ignite.spark.impl.{IgniteQueryIterator, IgnitePartition}
import org.apache.spark.{TaskContext, Partition}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

class IgniteRDD[R, K, V](
    ic: IgniteContext[K, V],
    qry: Query[R]
) extends RDD[R] (ic.sparkContext(), deps = Nil) {
    def this(
        ic: IgniteContext[K, V],
        p: (K, V) => Boolean
    ) = {
        this(ic, new ScanQuery[K, V](new ScalarPredicate2[K, V](p)))
    }

    override def compute(part: Partition, context: TaskContext): Iterator[R] = {
        new IgniteQueryIterator[R, K, V](ic, part, qry)
    }

    override protected def getPartitions: Array[Partition] = {
        val parts = ic.ignite().affinity(ic.cacheName).partitions()

        (0 until parts).map(new IgnitePartition(_)).toArray
    }

    override protected def getPreferredLocations(split: Partition): Seq[String] = {
        ic.ignite().affinity(ic.cacheName).mapPartitionToPrimaryAndBackups(split.index).map(_.addresses()).flatten.toList
    }
}
