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

package org.apache.ignite.examples.spark

import java.lang.{Long ⇒ JLong, String ⇒ JString}

import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.{Ignite, Ignition}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.ignite.spark.IgniteDataFrameSettings._

/**
  * Example application showing use-cases for Ignite implementation of Spark DataFrame API.
  */
object IgniteDataFrameExample extends App {
    /**
      * Ignite config file.
      */
    private val CONFIG = "examples/config/example-ignite.xml"

    /**
      * Test cache name.
      */
    private val CACHE_NAME = "testCache"

    //Starting Ignite server node.
    val ignite = setupServerAndData

    closeAfter(ignite) { ignite ⇒
        //Creating spark session.
        implicit val spark = SparkSession.builder()
            .appName("Spark Ignite data sources example")
            .master("local")
            .config("spark.executor.instances", "2")
            .getOrCreate()

        // Adjust the logger to exclude the logs of no interest.
        Logger.getRootLogger.setLevel(Level.ERROR)
        Logger.getLogger("org.apache.ignite").setLevel(Level.INFO)

        // Executing examples.

        sparkDSLExample

        nativeSparkSqlExample
    }

    /**
      * Examples of usage Ignite DataFrame implementation.
      * Selecting data throw Spark DSL.
      *
      * @param spark SparkSession.
      */
    def sparkDSLExample(implicit spark: SparkSession): Unit = {
        println("Querying using Spark DSL.")
        println

        val igniteDF = spark.read
            .format(FORMAT_IGNITE) //Data source type.
            .option(OPTION_TABLE, "person") //Table to read.
            .option(OPTION_CONFIG_FILE, CONFIG) //Ignite config.
            .load()
            .filter(col("id") >= 2) //Filter clause.
            .filter(col("name") like "%M%") //Another filter clause.

        println("Data frame schema:")

        igniteDF.printSchema() //Printing query schema to console.

        println("Data frame content:")

        igniteDF.show() //Printing query results to console.
    }

    /**
      * Examples of usage Ignite DataFrame implementation.
      * Registration of Ignite DataFrame for following usage.
      * Selecting data by Spark SQL query.
      *
      * @param spark SparkSession.
      */
    def nativeSparkSqlExample(implicit spark: SparkSession): Unit = {
        println("Querying using Spark SQL.")
        println

        val df = spark.read
            .format(FORMAT_IGNITE) //Data source type.
            .option(OPTION_TABLE, "person") //Table to read.
            .option(OPTION_CONFIG_FILE, CONFIG) //Ignite config.
            .load()

        //Registering DataFrame as Spark view.
        df.createOrReplaceTempView("person")

        //Selecting data from Ignite throw Spark SQL Engine.
        val igniteDF = spark.sql("SELECT * FROM person WHERE id >= 2 AND name = 'Mary Major'")

        println("Result schema:")

        igniteDF.printSchema() //Printing query schema to console.

        println("Result content:")

        igniteDF.show() //Printing query results to console.
    }

    def setupServerAndData: Ignite = {
        //Starting Ignite.
        val ignite = Ignition.start(CONFIG)

        //Creating first test cache.
        val ccfg = new CacheConfiguration[JLong, JString](CACHE_NAME).setSqlSchema("PUBLIC")

        val cache = ignite.getOrCreateCache(ccfg)

        //Creating SQL tables.
        cache.query(new SqlFieldsQuery(
            "CREATE TABLE city (id LONG PRIMARY KEY, name VARCHAR) WITH \"template=replicated\"")).getAll

        cache.query(new SqlFieldsQuery(
            "CREATE TABLE person (id LONG, name VARCHAR, city_id LONG, PRIMARY KEY (id, city_id)) " +
                "WITH \"backups=1, affinityKey=city_id\"")).getAll

        cache.query(new SqlFieldsQuery("CREATE INDEX on Person (city_id)")).getAll

        //Inserting some data to tables.
        var qry = new SqlFieldsQuery("INSERT INTO city (id, name) VALUES (?, ?)")

        cache.query(qry.setArgs(1L.asInstanceOf[JLong], "Forest Hill")).getAll
        cache.query(qry.setArgs(2L.asInstanceOf[JLong], "Denver")).getAll
        cache.query(qry.setArgs(3L.asInstanceOf[JLong], "St. Petersburg")).getAll

        qry = new SqlFieldsQuery("INSERT INTO person (id, name, city_id) values (?, ?, ?)")

        cache.query(qry.setArgs(1L.asInstanceOf[JLong], "John Doe", 3L.asInstanceOf[JLong])).getAll
        cache.query(qry.setArgs(2L.asInstanceOf[JLong], "Jane Roe", 2L.asInstanceOf[JLong])).getAll
        cache.query(qry.setArgs(3L.asInstanceOf[JLong], "Mary Major", 1L.asInstanceOf[JLong])).getAll
        cache.query(qry.setArgs(4L.asInstanceOf[JLong], "Richard Miles", 2L.asInstanceOf[JLong])).getAll

        ignite
    }
}
