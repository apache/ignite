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

import org.apache.ignite.IgniteException
import org.apache.ignite.cache.{CacheMode, QueryEntity}
import org.apache.ignite.cluster.ClusterNode
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.spark.{IgniteContext, IgniteRDD}
import org.apache.spark.Partition
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Apache Ignite implementation of Spark BaseRelation with PrunedFilteredScan for Ignite SQL Tables
  */
class IgniteSQLRelation[K, V](
    private[spark] val ic: IgniteContext,
    private[spark] val tableName: String)
    (@transient val sqlContext: SQLContext) extends BaseRelation with PrunedFilteredScan with Logging {

    /**
      * @return Schema of Ignite SQL table.
      */
    override def schema: StructType =
        igniteSQLTable(ic.ignite(), tableName)
            .map(IgniteSQLRelation.schema)
            .getOrElse(throw new IgniteException(s"Unknown table $tableName"))

    /**
      * Builds Apache Ignite SQL Query for given table, columns and filters.
      *
      * @param columns Columns to select.
      * @param filters Filters to apply.
      * @return Apache Ignite RDD implementation.
      */
    override def buildScan(columns: Array[String], filters: Array[Filter]): RDD[Row] = {
        val qryAndArgs = queryAndArgs(columns, filters)

        IgniteSQLDataFrameRDD[K, V](ic, cacheName, schema, qryAndArgs._1, qryAndArgs._2, calcPartitions(filters))
    }

    override def toString = s"IgniteSQLRelation[table=$tableName]"

    /**
      * @param columns Columns to select.
      * @param filters Filters to apply.
      * @return SQL query string and arguments for it.
      */
    private def queryAndArgs(columns: Array[String], filters: Array[Filter]): (String, List[Any]) = {
        val columnsStr =
            if (columns.isEmpty)
                "*"
            else
                columns.mkString(",")

        //Creating corresponding Ignite SQL query.
        //Query will be executed by Ignite SQL Engine.
        val qryAndArgs = filters match {
            case Array(_, _*) ⇒
                val where = QueryUtils.compileWhere(filters)

                (s"SELECT $columnsStr FROM $tableName WHERE ${where._1}", where._2)

            case _ ⇒
                (s"SELECT $columnsStr FROM $tableName", List.empty)
        }

        logInfo(qryAndArgs._1)

        qryAndArgs
    }

    private def calcPartitions(filters: Array[Filter]): Array[Partition] = {
        val cache = ic.ignite().cache[K, V](cacheName)

        val ccfg = cache.getConfiguration(classOf[CacheConfiguration[K, V]])

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

            val partitions = nodesToParts.zipWithIndex.map { case ((node, nodesParts), i) ⇒
                IgniteDataFramePartition(i, node, nodesParts.toList)
            }

            partitions.toArray
        }
    }

    /**
      * Cache name for a table name.
      */
    private lazy val cacheName: String =
        sqlCacheName(ic.ignite(), tableName)
            .getOrElse(throw new IgniteException(s"Unknown table $tableName"))

    /**
      * Utility method to add clause to sql WHERE string.
      *
      * @param filterStr Current filter string
      * @param clause Clause to add.
      * @return Filter string.
      */
    private def addStrClause(filterStr: String, clause: String) =
        if (filterStr.isEmpty)
            clause
        else
            filterStr + " AND " + clause

}

object IgniteSQLRelation {
    /**
      * Converts Apache Ignite table description: <code>QueryEntity</code> to Spark description: <code>StructType</code>.
      *
      * @param table Ignite table descirption.
      * @return Spark table descirption
      */
    def schema(table: QueryEntity): StructType = {
        //Partition columns has to be in the end of list.
        //See `org.apache.spark.sql.catalyst.catalog.CatalogTable#partitionSchema`
        val columns = table.getFields.toList.sortBy(c ⇒ isKeyColumn(table, c._1))

        StructType(columns.map { case (name, dataType) ⇒
            StructField(
                name = name,
                dataType = IgniteRDD.dataType(dataType, name),
                nullable = !isKeyColumn(table, name),
                metadata = Metadata.empty)
        })
    }

    def apply[K, V](ic: IgniteContext, tableName: String, sqlContext: SQLContext): IgniteSQLRelation[K, V] =
        new IgniteSQLRelation[K, V](ic,tableName)(sqlContext)
}
