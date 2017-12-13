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
object IgniteDataFrameOptions {
    /**
      * Name of DataSource format for loading data from Apache Ignite.
      */
    val IGNITE = "ignite"

    /**
      * Config option to specify named grid instance to connect when loading data.
      *
      * @example {{{
      * val igniteDF = spark.read.format(IGNITE)
      *     .option(GRID, "my-grid")
      *     //.... other options ...
      *     .load()
      * }}}
      *
      * @see [[org.apache.ignite.Ignite#name()]]
      */
    val GRID = "grid"

    /**
      * Config option to specify path to ignite config file.
      * Config from this file will be used to connect to existing Ignite cluster.
      *
      * @note All nodes for executing Spark task forcibly will be started in client mode.
      *
      * @example {{{
      * val igniteDF = spark.read.format(IGNITE)
      *     .option(CONFIG_FILE, CONFIG_FILE)
      *     // other options ...
      *     .load()
      * }}}
      */
    val CONFIG_FILE = "config"

    /**
      * Config option to specify Ignite SQL table name to load data from.
      *
      * @example {{{
      * val igniteDF = spark.read.format(IGNITE)
      *     // other options ...
      *     .option(TABLE, "mytable")
      *     .load()
      * }}}
      *
      * @see [[org.apache.ignite.cache.QueryEntity#tableName]]
      */
    val TABLE = "table"

    /**
      * Config option to specify Ignite cache name to load data from.
      *
      * @example {{{
      * val df = spark.read.format(IGNITE)
      *     // other options ...
      *     .option(CACHE, "mycache")
      *     .option(KEY_CLASS, "java.lang.Long")
      *     .option(VALUE_CLASS, "java.lang.String")
      *     .load()
      * }}}
      *
      * @see [[javax.cache.Cache#getName()]]
      */
    val CACHE = "cache"

    /**
      * Config option to specify java class for Ignite cache keys.
      *
      * @example {{{
      * val df = spark.read.format(IGNITE)
      *     //other options ...
      *     .option(CACHE, "mycache")
      *     .option(KEY_CLASS, "java.lang.Long")
      *     .load()
      * }}}
      *
      */
    val KEY_CLASS = "keyClass"

    /**
      * Config option to specify java class for Ignite cache values.
      *
      * @example {{{
      * val df = spark.read.format(IGNITE)
      *     // other options ...
      *     .option(CACHE, "mycache")
      *     .option(VALUE_CLASS, "java.lang.Long")
      *     .load()
      * }}}
      *
      */
    val VALUE_CLASS = "valueClass"

    /**
      * Config option to specify usage of Ignite BinaryMarshaller.
      * Can be used only in combination with `CACHE` as long as all SQL table in Ignite already has `keepBinary=true`.
      * Default value is `true`.
      *
      * @see [[org.apache.ignite.IgniteCache#withKeepBinary()]]
      * @see [[org.apache.ignite.internal.binary.BinaryMarshaller]]
      * @see [[org.apache.ignite.spark.impl.IgniteRelationProvider#CACHE]]
      */
    val KEEP_BINARY = "keepBinary"

}
