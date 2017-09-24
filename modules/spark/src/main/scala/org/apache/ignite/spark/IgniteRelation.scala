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

import org.apache.ignite.cache.QueryEntity
import org.apache.ignite.configuration.{CacheConfiguration, IgniteConfiguration}
import org.apache.ignite.spark.impl.IgniteDataFrameRDD
import org.apache.ignite.{IgniteException, Ignition}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.JavaConverters._

/**
  * Apache Ignite implementation of Spark BaseRelation with PrunedFilteredScan
  */
case class IgniteRelation(cfg: () ⇒ IgniteConfiguration, tableName: String)(@transient val sqlContext: SQLContext)
    extends BaseRelation with PrunedFilteredScan with Logging {

    override def schema: StructType = {
        val cache = Ignition.getOrStart(cfg()).cache[Any, Any](IgniteRelation.cacheName(tableName))

        val ccfg = cache.getConfiguration(classOf[CacheConfiguration[Any, Any]])

        val table = ccfg.getQueryEntities.asScala
            .find(_.getTableName == tableName)
            .getOrElse(throw new IgniteException(s"Table '$tableName' not found"))

        IgniteRelation.schema(table)
    }

    /**
      * Builds Apache Ignite SQL Query for given table, columns and filters.
      * @param columns Columns to select.
      * @param filters Filters to apply.
      * @return Apache Ignite RDD implementation.
      */
    override def buildScan(columns: Array[String], filters: Array[Filter]): RDD[Row] = {
        val qryAndArgs = filters match {
            case Array(_, _*) ⇒
                val where = buildWhere(filters)

                (s"SELECT ${columns.mkString(",")} FROM $tableName WHERE ${where._1}", where._2)
            case _ ⇒
                (s"SELECT ${columns.mkString(",")} FROM $tableName", List.empty)
        }

        IgniteDataFrameRDD(sqlContext.sparkContext, cfg, IgniteRelation.cacheName(tableName), schema, qryAndArgs._1, qryAndArgs._2)
    }

    /**
      * Builds `where` part of SQL query.
      *
      * @param filters To apply.
      * @return Tuple contains `where` string and `List[Any]` of query parameters.
      */
    private def buildWhere(filters: Array[Filter]): (String, List[Any]) =
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

            case In(attr, values) ⇒ (addStrClause(filterStr, s"$attr IN (${values.mkString(",")})"), params ++ values)

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

                (addStrClause(filterStr, s"NOT(${innerClause._1}"), innerClause._2)

            case StringStartsWith(attr, value) ⇒
                (addStrClause(filterStr, s"$attr LIKE ?"), params :+ (value + "%"))

            case StringEndsWith(attr, value) ⇒
                (addStrClause(filterStr, s"$attr LIKE ?"), params :+ ("%" + value))

            case StringContains(attr, value) ⇒
                (addStrClause(filterStr, s"$attr LIKE ?"), params :+ ("%" + value + "%"))
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

object IgniteRelation {
    /**
      * Converts Apache Ignite table description: <code>QueryEntity</code> to Spark description: <code>StructType</code>.
      *
      * @param table Ignite table descirption.
      * @return Spark table descirption
      */
    def schema(table: QueryEntity): StructType = {
        def toStructField(nameAndDataType: (String, String)) =
            StructField(
                name = nameAndDataType._1,
                dataType = IgniteRDD.dataType(nameAndDataType._2),
                nullable = false,
                metadata = Metadata.empty)

        val fields = table.getFields.asScala.toList
        val keys = table.getKeyFields.asScala

        val (keyFields, valueFields) = fields.partition(nameAndDataType ⇒ keys.contains(nameAndDataType._1))

        StructType(valueFields.map(toStructField) ++ keyFields.sorted.map(toStructField))
    }

    /**
      * @param tableName Ignite table name.
      * @return Cache name for given table.
      */
    def cacheName(tableName: String): String = s"SQL_PUBLIC_${tableName.toUpperCase}"
}
