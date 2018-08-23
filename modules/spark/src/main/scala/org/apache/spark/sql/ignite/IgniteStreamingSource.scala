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

package org.apache.spark.sql.ignite

import java.sql.Timestamp

import org.apache.ignite.spark.IgniteContext
import org.apache.ignite.spark.IgniteDataFrameSettings.FORMAT_IGNITE
import org.apache.ignite.spark.impl._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.unsafe.types.UTF8String

/** A [Source] that reads data from Ignite. */
class IgniteStreamingSource(
    sqlCtx: SQLContext,
    userDefinedSchema: Option[StructType],
    tblName: String,
    igniteCtx: IgniteContext,
    params: Map[String, String]
) extends Source {
    private val ignite = igniteCtx.ignite()
    private val offPlc = OffsetPolicy.apply(tblName, ignite, params)

    /** @inheritdoc */
    override def schema: StructType = userDefinedSchema match {
        case Some(s) => s
        case None => igniteSQLTable(ignite, tblName)
            .map(IgniteSQLRelation.schema)
            .getOrElse(
                throw new IllegalArgumentException(s"Table '$tblName' does not exist in source '$FORMAT_IGNITE'")
            )
    }

    /** @inheritdoc */
    override def getOffset: Option[Offset] = offPlc.getOffset

    /** @inheritdoc */
    override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
        val cacheName = sqlCacheName(ignite, tblName).getOrElse(
            throw new IllegalArgumentException(s"Table '$tblName' does not exist in source '$FORMAT_IGNITE'")
        )

        val qry = offPlc.getQuery(start, end, schema.fields.map(f => f.name))

        val rdd = IgniteSQLDataFrameRDD(
            igniteCtx,
            cacheName,
            schema,
            qry._1,
            qry._2,
            calcPartitions(igniteCtx, cacheName)
        ).map { r =>
            InternalRow.fromSeq(r.toSeq.map {
                case ts: Timestamp => DateTimeUtils.fromJavaTimestamp(ts)
                case s: String => UTF8String.fromString(s)
                case e => e
            })
        }

        sqlCtx.internalCreateDataFrame(rdd, schema, isStreaming = true)
    }

    /** @inheritdoc */
    override def stop(): Unit = {}
}
