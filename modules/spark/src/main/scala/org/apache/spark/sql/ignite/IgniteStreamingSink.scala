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

import org.apache.ignite.spark.IgniteContext
import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.apache.ignite.spark.impl.QueryHelper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils.SQLTimestamp
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.types.{DataType, StringType, TimestampType}

class IgniteStreamingSink(
    tblName: String,
    igniteCtx: IgniteContext,
    params: Map[String, String]
) extends Sink {
    override def addBatch(batchId: Long, data: DataFrame): Unit = {
        val valMapper = (vt: (Any, DataType)) => vt._2 match {
            case _: TimestampType => DateTimeUtils.toJavaTimestamp(vt._1.asInstanceOf[SQLTimestamp])
            case _: StringType => vt._1.toString
            case _ => vt._1.asInstanceOf[Object]
        }

        QueryHelper.saveTable(
            data.queryExecution,
            tblName,
            valMapper,
            igniteCtx,
            params.get(OPTION_STREAMER_ALLOW_OVERWRITE).map(_.toBoolean),
            params.get(OPTION_STREAMER_FLUSH_FREQUENCY).map(_.toLong),
            params.get(OPTION_STREAMER_PER_NODE_BUFFER_SIZE).map(_.toInt),
            params.get(OPTION_STREAMER_PER_NODE_PARALLEL_OPERATIONS).map(_.toInt),
            params.get(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS),
            params.get(OPTION_CREATE_TABLE_PARAMETERS)
        )
    }
}
