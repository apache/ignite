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

/**
  */
object IgniteDataFrameSettings {
    /**
      * Name of DataSource format for loading data from Apache Ignite.
      */
    val FORMAT_IGNITE = "ignite"

    /**
      * Config option to specify path to ignite config file.
      * Config from this file will be used to connect to existing Ignite cluster.
      *
      * @note All nodes for executing Spark task forcibly will be started in client mode.
      *
      * @example {{{
      * val igniteDF = spark.read.format(IGNITE)
      *     .option(OPTION_CONFIG_FILE, CONFIG_FILE)
      *     // other options ...
      *     .load()
      * }}}
      */
    val OPTION_CONFIG_FILE = "config"

    /**
      * Config option to specify Ignite SQL table name to load data from.
      *
      * @example {{{
      * val igniteDF = spark.read.format(IGNITE)
      *     // other options ...
      *     .option(OPTION_TABLE, "mytable")
      *     .load()
      * }}}
      *
      * @see [[org.apache.ignite.cache.QueryEntity#tableName]]
      */
    val OPTION_TABLE = "table"

    /**
      * Config option to specify the Ignite SQL schema name in which the specified table is present.
      * If this is not specified, all schemata will be scanned for a table name which matches the given table
      * name and the first matching table will be used. This option can be used when there are multiple tables in
      * different schemata with the same table name to disambiguate the tables.
      *
      * @example {{{
      * val igniteDF = spark.read.format(IGNITE)
      *     .option(OPTION_TABLE, "myTable")
      *     .option(OPTION_SCHEMA, "mySchema")
      * }}}
      */
    val OPTION_SCHEMA = "schema"

    /**
      * Config option to specify newly created Ignite SQL table parameters.
      * Value of these option will be used in `CREATE TABLE ...  WITH "option value goes here"`
      *
      * @example {{{
      * val igniteDF = spark.write.format(IGNITE)
      *     // other options ...
      *     .option( OPTION_CREATE_TABLE_PARAMETERS, "backups=1, template=replicated")
      *     .save()
      * }}}
      *
      * @see [[https://apacheignite-sql.readme.io/docs/create-table]]
      */
    val OPTION_CREATE_TABLE_PARAMETERS = "createTableParameters"

    /**
      * Config option to specify comma separated list of primary key fields for a newly created Ignite SQL table.
      *
      * @example {{{
      * val igniteDF = spark.write.format(IGNITE)
      *     // other options ...
      *     .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "id")
      *     .save()
      * }}}
      *
      * @see [[https://apacheignite-sql.readme.io/docs/create-table]]
      */
    val OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS = "primaryKeyFields"

    /**
      * Config option for saving data frame.
      * Internally all SQL inserts are done through `IgniteDataStreamer`.
      * This options sets `allowOverwrite` property of streamer.
      * If `true` then row with same primary key value will be written to the table.
      * If `false` then row with same primary key value will be skipped. Existing row will be left in the table.
      * Default value if `false`.
      *
      * @example {{{
      * val igniteDF = spark.write.format(IGNITE)
      *     // other options ...
      *     .option(OPTION_STREAMER_ALLOW_OVERWRITE, true)
      *     .save()
      * }}}
      *
      * @see [[org.apache.ignite.IgniteDataStreamer]]
      * @see [[org.apache.ignite.IgniteDataStreamer#allowOverwrite(boolean)]]
      */
    val OPTION_STREAMER_ALLOW_OVERWRITE = "streamerAllowOverwrite"

    /**
      * Config option for saving data frame.
      * Internally all SQL inserts are done through `IgniteDataStreamer`.
      * This options sets `autoFlushFrequency` property of streamer.
      *
      * @example {{{
      * val igniteDF = spark.write.format(IGNITE)
      *     // other options ...
      *     .option(OPTION_STREAMING_FLUSH_FREQUENCY, 10000)
      *     .save()
      * }}}
      *
      * @see [[org.apache.ignite.IgniteDataStreamer]]
      * @see [[org.apache.ignite.IgniteDataStreamer#autoFlushFrequency(long)]]
      */
    val OPTION_STREAMER_FLUSH_FREQUENCY = "streamerFlushFrequency"

    /**
      * Config option for saving data frame.
      * Internally all SQL inserts are done through `IgniteDataStreamer`.
      * This options sets `perNodeBufferSize` property of streamer.
      *
      * @example {{{
      * val igniteDF = spark.write.format(IGNITE)
      *     // other options ...
      *     .option(OPTION_STREAMING_PER_NODE_BUFFER_SIZE, 1024)
      *     .save()
      * }}}
      *
      * @see [[org.apache.ignite.IgniteDataStreamer]]
      * @see [[org.apache.ignite.IgniteDataStreamer#perNodeBufferSize(int)]]
      */
    val OPTION_STREAMER_PER_NODE_BUFFER_SIZE = "streamerPerNodeBufferSize"

    /**
      * Config option for saving data frame.
      * Internally all SQL inserts are done through `IgniteDataStreamer`.
      * This options sets `perNodeParallelOperations` property of streamer.
      *
      * @example {{{
      * val igniteDF = spark.write.format(IGNITE)
      *     // other options ...
      *     .option(OPTION_STREAMING_PER_NODE_PARALLEL_OPERATIONS, 42)
      *     .save()
      * }}}
      *
      * @see [[org.apache.ignite.IgniteDataStreamer]]
      * @see [[org.apache.ignite.IgniteDataStreamer#perNodeParallelOperations(int)]]
      */
    val OPTION_STREAMER_PER_NODE_PARALLEL_OPERATIONS = "streamerPerNodeParallelOperations"

    /**
      * Option for a [[org.apache.spark.sql.SparkSession]] configuration.
      * If `true` then all Ignite optimization of Spark SQL statements will be disabled.
      * Default value is `false`.
      *
      * @see [[org.apache.spark.sql.ignite.IgniteOptimization]]
      */
    val OPTION_DISABLE_SPARK_SQL_OPTIMIZATION = "ignite.disableSparkSQLOptimization"
}
