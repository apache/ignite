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

import org.apache.ignite.IgniteException
import org.apache.ignite.cache.{CacheMode, QueryEntity}
import org.apache.ignite.cluster.ClusterNode
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.spark.impl.{IgniteDataFramePartition, IgniteSQLDataFrameRDD}
import org.apache.ignite.spark.IgniteSQLRelation.sqlCacheName
import org.apache.spark.Partition
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
    (@transient val sqlContext: SQLContext) extends BaseRelation with PrunedFilteredScan {
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
        val columnsStr =
            if (columns.isEmpty)
                "*"
            else
                columns.mkString(",")

        //Creating corresponding Ignite SQL query.
        //Query will be executed by Ignite SQL Engine.
        val qryAndArgs = filters match {
            case Array(_, _*) ⇒
                val where = compileWhere(filters)
                (s"SELECT $columnsStr FROM $tableName WHERE ${where._1}", where._2)
            case _ ⇒
                (s"SELECT $columnsStr FROM $tableName", List.empty)
        }

        val cache = ic.ignite().cache[K, V](sqlCacheName(tableName))

        val ccfg = cache.getConfiguration(classOf[CacheConfiguration[K, V]])

        IgniteSQLDataFrameRDD[K, V](ic, sqlCacheName(tableName), schema, qryAndArgs._1, qryAndArgs._2, calcPartitions(filters))
    }

    override def toString = s"IgniteSQLRelation[table=$tableName]"

    /**
      * Builds `where` part of SQL query.
      *
      * @param filters Filter to apply.
      * @return Tuple contains `where` string and `List[Any]` of query parameters.
      */
    private def compileWhere(filters: Array[Filter]): (String, List[Any]) =
        filters.foldLeft(("", List[Any]()))(buildSingleClause)

    /**
      * Adds single where clause to `state` and returns new state.
      *
      * @param state Current `where` state.
      * @param clause Clause to add.
      * @return `where` with given clause.
      */
    private def buildSingleClause(state: (String, List[Any]), clause: Filter): (String, List[Any]) = {
        val filterStr = state._1
        val params = state._2

        clause match {
            case EqualTo(attr, value) ⇒ (addStrClause(filterStr, s"$attr = ?"), params :+ value)

            case EqualNullSafe(attr, value) ⇒ (addStrClause(filterStr, s"($attr IS NULL OR $attr = ?)"), params :+ value)

            case GreaterThan(attr, value) ⇒ (addStrClause(filterStr, s"$attr > ?"), params :+ value)

            case GreaterThanOrEqual(attr, value) ⇒ (addStrClause(filterStr, s"$attr >= ?"), params :+ value)

            case LessThan(attr, value) ⇒ (addStrClause(filterStr, s"$attr < ?"), params :+ value)

            case LessThanOrEqual(attr, value) ⇒ (addStrClause(filterStr, s"$attr <= ?"), params :+ value)

            case In(attr, values) ⇒ (addStrClause(filterStr, s"$attr IN (${values.map(_ ⇒ "?").mkString(",")})"), params ++ values)

            case IsNull(attr) ⇒ (addStrClause(filterStr, s"$attr IS NULL"), params)

            case IsNotNull(attr) ⇒ (addStrClause(filterStr, s"$attr IS NOT NULL"), params)

            case And(left, right) ⇒
                val leftClause = buildSingleClause(("", params), left)
                val rightClause = buildSingleClause(("", leftClause._2), right)

                (addStrClause(filterStr, s"${leftClause._1} AND ${rightClause._1}"), rightClause._2)

            case Or(left, right) ⇒
                val leftClause = buildSingleClause(("", params), left)
                val rightClause = buildSingleClause(("", leftClause._2), right)

                (addStrClause(filterStr, s"${leftClause._1} OR ${rightClause._1}"), rightClause._2)

            case Not(child) ⇒
                val innerClause = buildSingleClause(("", params), child)

                (addStrClause(filterStr, s"NOT ${innerClause._1}"), innerClause._2)

            case StringStartsWith(attr, value) ⇒
                (addStrClause(filterStr, s"$attr LIKE ?"), params :+ (value + "%"))

            case StringEndsWith(attr, value) ⇒
                (addStrClause(filterStr, s"$attr LIKE ?"), params :+ ("%" + value))

            case StringContains(attr, value) ⇒
                (addStrClause(filterStr, s"$attr LIKE ?"), params :+ ("%" + value + "%"))
        }
    }

    private def calcPartitions(filters: Array[Filter]): Array[Partition] = {
        val cache = ic.ignite().cache[K, V](sqlCacheName(tableName))

        val ccfg = cache.getConfiguration(classOf[CacheConfiguration[K, V]])

        if (ccfg.getCacheMode == CacheMode.REPLICATED) {
            val serverNodes = ic.ignite().cluster().forCacheNodes(sqlCacheName(tableName)).forServers().nodes()

            Array(IgniteDataFramePartition(0, serverNodes.head, Stream.from(0).take(1024).toList))
        }
        else {
            val aff = ic.ignite().affinity(sqlCacheName(tableName))

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

    /**
      * @param tableName Ignite table name.
      *
      * @return Cache name for given table.
      */
    def sqlCacheName(tableName: String): String = s"SQL_PUBLIC_${tableName.toUpperCase}"

    def apply[K, V](ic: IgniteContext, tableName: String, sqlContext: SQLContext): IgniteSQLRelation[K, V] =
        new IgniteSQLRelation[K, V](ic,tableName)(sqlContext)
}
