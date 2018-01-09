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
}
