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
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.internal.IgnitionEx
import org.apache.ignite.internal.util.IgniteUtils
import org.apache.ignite.spark.IgniteContext
import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.apache.ignite.spark.impl.QueryHelper.{createTable, dropTable, ensureCreateTableOptions, saveTable}
import org.apache.spark.sql.SaveMode.{Append, Overwrite}
import org.apache.spark.sql.ignite.IgniteExternalCatalog.{IGNITE_PROTOCOL, OPTION_GRID}
import org.apache.spark.sql.ignite.IgniteOptimization
import org.apache.spark.sql.sources._
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * Apache Ignite relation provider.
  */
class IgniteRelationProvider extends RelationProvider
    with CreatableRelationProvider
    with DataSourceRegister {
    /**
      * @return "ignite" - name of relation provider.
      */
    override def shortName(): String = FORMAT_IGNITE

    /**
      * To create IgniteRelation we need a link to a ignite cluster and a table name.
      * To refer cluster user have to specify one of config parameter:
      * <ul>
      *     <li><code>config</code> - path to ignite configuration file.
      * </ul>
      * Existing table inside Apache Ignite should be referred via <code>table</code> parameter.
      *
      * @param sqlCtx SQLContext.
      * @param params Parameters for relation creation.
      * @return IgniteRelation.
      * @see IgniteRelation
      * @see IgnitionEx#grid(String)
      * @see org.apache.ignite.spark.IgniteDataFrameSettings.OPTION_TABLE
      * @see org.apache.ignite.spark.IgniteDataFrameSettings.OPTION_SCHEMA
      * @see org.apache.ignite.spark.IgniteDataFrameSettings.OPTION_CONFIG_FILE
      */
    override def createRelation(sqlCtx: SQLContext, params: Map[String, String]): BaseRelation =
        createRelation(
            igniteContext(params, sqlCtx),
            params.getOrElse(OPTION_TABLE, throw new IgniteException("'table' must be specified.")),
            params.get(OPTION_SCHEMA),
            sqlCtx)

    /**
      * Save `data` to corresponding Ignite table and returns Relation for saved data.
      *
      * To save data or create IgniteRelation we need a link to a ignite cluster and a table name.
      * To refer cluster user have to specify one of config parameter:
      * <ul>
      *     <li><code>config</code> - path to ignite configuration file.
      * </ul>
      * Existing table inside Apache Ignite should be referred via <code>table</code> or <code>path</code> parameter.
      *
      * If table doesn't exists it will be created.
      * If `mode` is Overwrite and `table` already exists it will be recreated(DROP TABLE, CREATE TABLE).
      *
      * If table create is required use can set following options:
      *
      * <ul>
      *     <li>`OPTION_PRIMARY_KEY_FIELDS` - required option. comma separated list of fields for primary key.</li>
      *     <li>`OPTION_CACHE_FOR_DDL` - required option. Existing cache name for executing SQL DDL statements.
      *     <li>`OPTION_CREATE_TABLE_OPTIONS` - Ignite specific parameters for a new table. See WITH [https://apacheignite-sql.readme.io/docs/create-table].</li>
      * </ul>
      *
      * Data write executed 'by partition'. User can set `OPTION_WRITE_PARTITIONS_NUM` - number of partition for data.
      *
      * @param sqlCtx SQLContext.
      * @param mode Save mode.
      * @param params Additional parameters.
      * @param data Data to save.
      * @return IgniteRelation.
      */
    override def createRelation(sqlCtx: SQLContext,
        mode: SaveMode,
        params: Map[String, String],
        data: DataFrame): BaseRelation = {

        val ctx = igniteContext(params, sqlCtx)

        val tblName = tableName(params)

        val tblInfoOption = sqlTableInfo(ctx.ignite(), tblName, params.get(OPTION_SCHEMA))

        if (tblInfoOption.isDefined) {
            mode match {
                case Overwrite ⇒
                    ensureCreateTableOptions(data.schema, params, ctx)

                    dropTable(tblName, ctx.ignite())

                    val createTblOpts = params.get(OPTION_CREATE_TABLE_PARAMETERS)

                    createTable(data.schema,
                        tblName,
                        primaryKeyFields(params),
                        createTblOpts,
                        ctx.ignite())

                    saveTable(data,
                        tblName,
                        params.get(OPTION_SCHEMA),
                        ctx,
                        params.get(OPTION_STREAMER_ALLOW_OVERWRITE).map(_.toBoolean),
                        params.get(OPTION_STREAMER_SKIP_STORE).map(_.toBoolean),
                        params.get(OPTION_STREAMER_FLUSH_FREQUENCY).map(_.toLong),
                        params.get(OPTION_STREAMER_PER_NODE_BUFFER_SIZE).map(_.toInt),
                        params.get(OPTION_STREAMER_PER_NODE_PARALLEL_OPERATIONS).map(_.toInt))

                case Append ⇒
                    saveTable(data,
                        tblName,
                        params.get(OPTION_SCHEMA),
                        ctx,
                        params.get(OPTION_STREAMER_ALLOW_OVERWRITE).map(_.toBoolean),
                        params.get(OPTION_STREAMER_SKIP_STORE).map(_.toBoolean),
                        params.get(OPTION_STREAMER_FLUSH_FREQUENCY).map(_.toLong),
                        params.get(OPTION_STREAMER_PER_NODE_BUFFER_SIZE).map(_.toInt),
                        params.get(OPTION_STREAMER_PER_NODE_PARALLEL_OPERATIONS).map(_.toInt))

                case SaveMode.ErrorIfExists =>
                    throw new IgniteException(s"Table or view '$tblName' already exists. SaveMode: ErrorIfExists.")

                case SaveMode.Ignore =>
                    // With `SaveMode.Ignore` mode, if table already exists, the save operation is expected
                    // to not save the contents of the DataFrame and to not change the existing data.
                    // Therefore, it is okay to do nothing here and then just return the relation below.
            }
        }
        else {
            ensureCreateTableOptions(data.schema, params, ctx)

            val primaryKeyFields = params(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS).split(",")

            val createTblOpts = params.get(OPTION_CREATE_TABLE_PARAMETERS)

            createTable(data.schema,
                tblName,
                primaryKeyFields,
                createTblOpts,
                ctx.ignite())

            saveTable(data,
                tblName,
                params.get(OPTION_SCHEMA),
                ctx,
                params.get(OPTION_STREAMER_ALLOW_OVERWRITE).map(_.toBoolean),
                params.get(OPTION_STREAMER_SKIP_STORE).map(_.toBoolean),
                params.get(OPTION_STREAMER_FLUSH_FREQUENCY).map(_.toLong),
                params.get(OPTION_STREAMER_PER_NODE_BUFFER_SIZE).map(_.toInt),
                params.get(OPTION_STREAMER_PER_NODE_PARALLEL_OPERATIONS).map(_.toInt))
        }

        createRelation(ctx,
            tblName,
            params.get(OPTION_SCHEMA),
            sqlCtx)
    }

    /**
      * @param igniteCtx Ignite context.
      * @param tblName Table name.
      * @param schema Optional schema name.
      * @param sqlCtx SQL context.
      * @return Ignite SQL relation.
      */
    private def createRelation(igniteCtx: IgniteContext, tblName: String, schema: Option[String], sqlCtx: SQLContext):
    BaseRelation = {
        val optimizationDisabled =
            sqlCtx.sparkSession.conf.get(OPTION_DISABLE_SPARK_SQL_OPTIMIZATION, "false").toBoolean

        val experimentalMethods = sqlCtx.sparkSession.sessionState.experimentalMethods

        if (optimizationDisabled) {
            experimentalMethods.extraOptimizations =
                experimentalMethods.extraOptimizations.filter(_ != IgniteOptimization)
        }
        else {
            val optimizationExists = experimentalMethods.extraOptimizations.contains(IgniteOptimization)

            if (!optimizationExists)
                experimentalMethods.extraOptimizations = experimentalMethods.extraOptimizations :+ IgniteOptimization
        }

        IgniteSQLRelation(
            igniteCtx,
            tblName,
            schema,
            sqlCtx)
    }

    /**
      * @param params Params.
      * @param sqlCtx SQL Context.
      * @return IgniteContext.
      */
    private def igniteContext(params: Map[String, String], sqlCtx: SQLContext): IgniteContext = {
        val igniteHome = IgniteUtils.getIgniteHome

        def configProvider: () ⇒ IgniteConfiguration = {
            if (params.contains(OPTION_CONFIG_FILE))
                () ⇒ {
                    IgniteContext.setIgniteHome(igniteHome)

                    val cfg = IgnitionEx.loadConfiguration(params(OPTION_CONFIG_FILE)).get1()

                    cfg.setClientMode(true)

                    cfg
                }
            else if (params.contains(OPTION_GRID))
                () ⇒ {
                    IgniteContext.setIgniteHome(igniteHome)

                    val cfg = ignite(params(OPTION_GRID)).configuration()

                    cfg.setClientMode(true)

                    cfg
                }
            else
                throw new IgniteException("'config' must be specified to connect to ignite cluster.")
        }

        IgniteContext(sqlCtx.sparkContext, configProvider)
    }

    /**
      * @param params Params.
      * @return Table name.
      */
    private def tableName(params: Map[String, String]): String = {
        val tblName = params.getOrElse(OPTION_TABLE,
            params.getOrElse("path", throw new IgniteException("'table' or 'path' must be specified.")))

        if (tblName.startsWith(IGNITE_PROTOCOL))
            tblName.replace(IGNITE_PROTOCOL, "").toUpperCase()
        else
            tblName.toUpperCase
    }

    /**
      * @param params Params.
      * @return Sequence of primary key fields.
      */
    private def primaryKeyFields(params: Map[String, String]): Seq[String] =
        params(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS).split(",")
}
