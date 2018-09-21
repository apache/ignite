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
import org.apache.ignite.cache.QueryEntity
import org.apache.ignite.spark.{IgniteContext, IgniteRDD, impl}
import org.apache.spark.Partition
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.JavaConversions._

/**
  * Apache Ignite implementation of Spark BaseRelation with PrunedFilteredScan for Ignite SQL Tables
  */
class IgniteSQLRelation[K, V](
    private[apache] val ic: IgniteContext,
    private[apache] val tableName: String)
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

    /**
      * Computes spark partitions for this relation.
      *
      * @return Array of IgniteDataFramPartition.
      */
    private def calcPartitions(filters: Array[Filter]): Array[Partition] =
        impl.calcPartitions(ic, cacheName)

    /**
      * Cache name for a table name.
      */
    private lazy val cacheName: String =
        sqlCacheName(ic.ignite(), tableName)
            .getOrElse(throw new IgniteException(s"Unknown table $tableName"))
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
                name = table.getAliases.getOrDefault(name, name),
                dataType = IgniteRDD.dataType(dataType, name),
                nullable = !isKeyColumn(table, name),
                metadata = Metadata.empty)
        })
    }

    def apply[K, V](ic: IgniteContext, tableName: String, sqlContext: SQLContext): IgniteSQLRelation[K, V] =
        new IgniteSQLRelation[K, V](ic,tableName)(sqlContext)
}
