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

import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.spark.IgniteDataFrameSettings._
import QueryUtils.{compileCreateTable, compileDropTable, compileInsert}
import org.apache.ignite.internal.IgniteEx
import org.apache.ignite.internal.processors.query.QueryUtils.DFLT_SCHEMA
import org.apache.ignite.spark.IgniteContext
import org.apache.ignite.{Ignite, IgniteException}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Helper class for executing DDL queries.
  */
private[apache] object QueryHelper {
    /**
      * Drops provided table.
      *
      * @param tableName Table name.
      * @param ignite Ignite.
      */
    def dropTable(tableName: String, ignite: Ignite): Unit = {
        val qryProcessor = ignite.asInstanceOf[IgniteEx].context().query()

        val qry = compileDropTable(tableName)

        qryProcessor.querySqlFields(new SqlFieldsQuery(qry), true).getAll
    }

    /**
      * Creates table.
      *
      * @param schema Schema.
      * @param tblName Table name.
      * @param primaryKeyFields Primary key fields.
      * @param createTblOpts Ignite specific options.
      * @param ignite Ignite.
      */
    def createTable(schema: StructType, tblName: String, primaryKeyFields: Seq[String], createTblOpts: Option[String],
        ignite: Ignite): Unit = {
        val qryProcessor = ignite.asInstanceOf[IgniteEx].context().query()

        val qry = compileCreateTable(schema, tblName, primaryKeyFields, createTblOpts)

        qryProcessor.querySqlFields(new SqlFieldsQuery(qry), true).getAll
    }

    /**
      * Ensures all options are specified correctly to create table based on provided `schema`.
      *
      * @param schema Schema of new table.
      * @param params Parameters.
      */
    def ensureCreateTableOptions(schema: StructType, params: Map[String, String], ctx: IgniteContext): Unit = {
        if (!params.contains(OPTION_TABLE) && !params.contains("path"))
            throw new IgniteException("'table' must be specified.")

        if (params.contains(OPTION_SCHEMA) && !params(OPTION_SCHEMA).equalsIgnoreCase(DFLT_SCHEMA)) {
            throw new IgniteException("Creating new tables in schema " + params(OPTION_SCHEMA) + " is not valid, tables"
                + " must only be created in " + DFLT_SCHEMA)
        }

        params.get(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS)
            .map(_.split(','))
            .getOrElse(throw new IgniteException("Can't create table! Primary key fields has to be specified."))
            .map(_.trim)
            .foreach { pkField ⇒
                if (pkField == "")
                    throw new IgniteException("PK field can't be empty.")

                if (!schema.exists(_.name.equalsIgnoreCase(pkField)))
                    throw new IgniteException(s"'$pkField' doesn't exists in DataFrame schema.")

            }
    }

    /**
      * Saves data to the table.
      *
      * @param data Data.
      * @param tblName Table name.
      * @param schemaName Optional schema name.
      * @param ctx Ignite context.
      * @param streamerAllowOverwrite Flag enabling overwriting existing values in cache.
      * @param streamerFlushFrequency Insert query streamer automatic flush frequency.
      * @param streamerPerNodeBufferSize Insert query streamer size of per node query buffer.
      * @param streamerPerNodeParallelOperations Insert query streamer maximum number of parallel operations for a single node.
      *
      * @see [[org.apache.ignite.IgniteDataStreamer]]
      * @see [[org.apache.ignite.IgniteDataStreamer#allowOverwrite(boolean)]]
      * @see [[org.apache.ignite.IgniteDataStreamer#autoFlushFrequency(long)]]
      * @see [[org.apache.ignite.IgniteDataStreamer#perNodeBufferSize(int)]]
      * @see [[org.apache.ignite.IgniteDataStreamer#perNodeParallelOperations(int)]]
      */
    def saveTable(data: DataFrame,
        tblName: String,
        schemaName: Option[String],
        ctx: IgniteContext,
        streamerAllowOverwrite: Option[Boolean],
        streamerFlushFrequency: Option[Long],
        streamerPerNodeBufferSize: Option[Int],
        streamerPerNodeParallelOperations: Option[Int]
    ): Unit = {
        val insertQry = compileInsert(tblName, data.schema)

        data.rdd.foreachPartition(iterator =>
            savePartition(iterator,
                insertQry,
                tblName,
                schemaName,
                ctx,
                streamerAllowOverwrite,
                streamerFlushFrequency,
                streamerPerNodeBufferSize,
                streamerPerNodeParallelOperations
            ))
    }

    /**
      * Saves partition data to the Ignite table.
      *
      * @param iterator Data iterator.
      * @param insertQry Insert query.
      * @param tblName Table name.
      * @param schemaName Optional schema name.
      * @param ctx Ignite context.
      * @param streamerAllowOverwrite Flag enabling overwriting existing values in cache.
      * @param streamerFlushFrequency Insert query streamer automatic flush frequency.
      * @param streamerPerNodeBufferSize Insert query streamer size of per node query buffer.
      * @param streamerPerNodeParallelOperations Insert query streamer maximum number of parallel operations for a single node.
      *
      * @see [[org.apache.ignite.IgniteDataStreamer]]
      * @see [[org.apache.ignite.IgniteDataStreamer#allowOverwrite(boolean)]]
      * @see [[org.apache.ignite.IgniteDataStreamer#autoFlushFrequency(long)]]
      * @see [[org.apache.ignite.IgniteDataStreamer#perNodeBufferSize(int)]]
      * @see [[org.apache.ignite.IgniteDataStreamer#perNodeParallelOperations(int)]]
      */
    private def savePartition(iterator: Iterator[Row],
        insertQry: String,
        tblName: String,
        schemaName: Option[String],
        ctx: IgniteContext,
        streamerAllowOverwrite: Option[Boolean],
        streamerFlushFrequency: Option[Long],
        streamerPerNodeBufferSize: Option[Int],
        streamerPerNodeParallelOperations: Option[Int]
    ): Unit = {
        val tblInfo = sqlTableInfo[Any, Any](ctx.ignite(), tblName, schemaName).get

        val streamer = ctx.ignite().dataStreamer(tblInfo._1.getName)

        streamerAllowOverwrite.foreach(v ⇒ streamer.allowOverwrite(v))

        streamerFlushFrequency.foreach(v ⇒ streamer.autoFlushFrequency(v))

        streamerPerNodeBufferSize.foreach(v ⇒ streamer.perNodeBufferSize(v))

        streamerPerNodeParallelOperations.foreach(v ⇒ streamer.perNodeParallelOperations(v))

        try {
            val qryProcessor = ctx.ignite().asInstanceOf[IgniteEx].context().query()

            iterator.foreach { row ⇒
                val schema = row.schema

                val args = schema.map { f ⇒
                    row.get(row.fieldIndex(f.name)).asInstanceOf[Object]
                }

                qryProcessor.streamUpdateQuery(tblInfo._1.getName,
                    tblInfo._1.getSqlSchema, streamer, insertQry, args.toArray)
            }
        }
        finally {
            streamer.close()
        }

    }
}
