/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.spark.impl

import org.apache.ignite.IgniteException
import org.apache.ignite.internal.processors.query.{GridQueryTypeDescriptor, QueryTypeDescriptorImpl}
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
    private[apache] val tableName: String,
    private[apache] val schemaName: Option[String])
    (@transient val sqlContext: SQLContext) extends BaseRelation with PrunedFilteredScan with Logging {

    /**
      * @return Schema of Ignite SQL table.
      */
    override def schema: StructType =
        sqlTableInfo(ic.ignite(), tableName, schemaName)
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
        sqlCacheName(ic.ignite(), tableName, schemaName)
            .getOrElse(throw new IgniteException(s"Unknown table $tableName"))
}

object IgniteSQLRelation {
    /**
      * Converts Apache Ignite table description: <code>QueryEntity</code> to Spark description: <code>StructType</code>.
      *
      * @param table Ignite table descirption.
      * @return Spark table descirption
      */
    def schema(table: GridQueryTypeDescriptor): StructType = {
        //Partition columns has to be in the end of list.
        //See `org.apache.spark.sql.catalyst.catalog.CatalogTable#partitionSchema`
        val columns = table.fields.toList.sortBy(c ⇒ isKeyColumn(table, c._1))

        StructType(columns.map { case (name, dataType) ⇒
            StructField(
                name = table.asInstanceOf[QueryTypeDescriptorImpl].aliases.getOrDefault(name, name),
                dataType = IgniteRDD.dataType(dataType.getName, name),
                nullable = !isKeyColumn(table, name),
                metadata = Metadata.empty)
        })
    }

    def apply[K, V](ic: IgniteContext, tableName: String, schemaName: Option[String],
        sqlContext: SQLContext): IgniteSQLRelation[K, V] =
        new IgniteSQLRelation[K, V](ic, tableName, schemaName)(sqlContext)
}
