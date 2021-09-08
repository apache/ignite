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

import java.lang.{Integer => JInt, Long => JLong, String => JString}

import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.apache.ignite.{Ignite, Ignition}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * Example application demonstrates the join operations between two dataframes or Spark tables with data saved in Ignite caches.
 */
object IgniteDataFrameJoinExample extends App {
    /** Ignite config file. */
    private val CONFIG = "examples/config/example-ignite.xml"

    /** Test cache name. */
    private val CACHE_NAME = "testCache"

    // Starting Ignite server node.
    val ignite = setupServerAndData

    closeAfter(ignite) { ignite ⇒
        //Creating spark session.
        implicit val spark = SparkSession.builder()
            .appName("IgniteDataFrameJoinExample")
            .master("local")
            .config("spark.executor.instances", "2")
            .getOrCreate()

        // Adjust the logger to exclude the logs of no interest.
        Logger.getRootLogger.setLevel(Level.ERROR)
        Logger.getLogger("org.apache.ignite").setLevel(Level.INFO)

        // Executing examples.
        sparkDSLJoinExample
        nativeSparkSqlJoinExample
    }

    /**
      * Examples of usage Ignite DataFrame implementation.
      * Selecting data throw Spark DSL.
      *
      * @param spark SparkSession.
      */
    def sparkDSLJoinExample(implicit spark: SparkSession): Unit = {
        println("Querying using Spark DSL.")
        println

        val persons = spark.read
          .format(FORMAT_IGNITE)
          .option(OPTION_TABLE, "person")
          .option(OPTION_CONFIG_FILE, CONFIG)
          .load()

        persons.printSchema()
        persons.show()

        val cities = spark.read
          .format(FORMAT_IGNITE)
          .option(OPTION_TABLE, "city")
          .option(OPTION_CONFIG_FILE, CONFIG)
          .load()

        persons.printSchema()
        persons.show()

        val joinResult = persons.join(cities, persons("city_id") === cities("id"))
          .select(persons("name").as("person"), persons("age"), cities("name").as("city"), cities("country"))

        joinResult.explain(true)
        joinResult.printSchema()
        joinResult.show()
    }

     /**
      * Examples of usage Ignite DataFrame implementation.
      * Registration of Ignite DataFrame for following usage.
      * Selecting data by Spark SQL query.
      *
      * @param spark SparkSession.
      */
    def nativeSparkSqlJoinExample(implicit spark: SparkSession): Unit = {
        println("Querying using Spark SQL.")
        println

        val persons = spark.read
          .format(FORMAT_IGNITE)
          .option(OPTION_TABLE, "person")
          .option(OPTION_CONFIG_FILE, CONFIG)
          .load()

        persons.printSchema()
        persons.show()

        val cities = spark.read
          .format(FORMAT_IGNITE)
          .option(OPTION_TABLE, "city")
          .option(OPTION_CONFIG_FILE, CONFIG)
          .load()

        persons.printSchema()
        persons.show()

        // Registering DataFrame as Spark view.
        persons.createOrReplaceTempView("person")
        cities.createOrReplaceTempView("city")

        // Selecting data from Ignite throw Spark SQL Engine.
        val joinResult = spark.sql("""
                                     | SELECT
                                     |   person.name AS person,
                                     |   age,
                                     |   city.name AS city,
                                     |   country
                                     | FROM
                                     |   person JOIN
                                     |   city ON person.city_id = city.id
                                   """.stripMargin);

        joinResult.explain(true)
        joinResult.printSchema()
        joinResult.show()
    }

    def setupServerAndData: Ignite = {
        // Starting Ignite.
        val ignite = Ignition.start(CONFIG)

        // Creating first test cache.
        val ccfg = new CacheConfiguration[JLong, JString](CACHE_NAME).setSqlSchema("PUBLIC")

        val cache = ignite.getOrCreateCache(ccfg)

        // Creating SQL tables.
        cache.query(new SqlFieldsQuery(
            "CREATE TABLE city (id LONG PRIMARY KEY, name VARCHAR, country VARCHAR) WITH \"template=replicated\"")).getAll

        cache.query(new SqlFieldsQuery(
            "CREATE TABLE person (id LONG, name VARCHAR, age INT, city_id LONG, PRIMARY KEY (id, city_id)) " +
                "WITH \"backups=1, affinityKey=city_id\"")).getAll

        cache.query(new SqlFieldsQuery("CREATE INDEX on Person (city_id)")).getAll

        // Inserting some data to tables.
        var qry = new SqlFieldsQuery("INSERT INTO city (id, name, country) VALUES (?, ?, ?)")

        cache.query(qry.setArgs(1L.asInstanceOf[JLong], "Forest Hill", "USA")).getAll
        cache.query(qry.setArgs(2L.asInstanceOf[JLong], "Denver", "USA")).getAll
        cache.query(qry.setArgs(3L.asInstanceOf[JLong], "St. Petersburg", "Russia")).getAll

        qry = new SqlFieldsQuery("INSERT INTO person (id, name, age, city_id) values (?, ?, ?, ?)")

        cache.query(qry.setArgs(1L.asInstanceOf[JLong], "John Doe", 31.asInstanceOf[JInt], 3L.asInstanceOf[JLong])).getAll
        cache.query(qry.setArgs(2L.asInstanceOf[JLong], "Jane Roe", 27.asInstanceOf[JInt], 2L.asInstanceOf[JLong])).getAll
        cache.query(qry.setArgs(3L.asInstanceOf[JLong], "Mary Major", 86.asInstanceOf[JInt], 1L.asInstanceOf[JLong])).getAll
        cache.query(qry.setArgs(4L.asInstanceOf[JLong], "Richard Miles", 19.asInstanceOf[JInt], 2L.asInstanceOf[JLong])).getAll

        ignite
    }
}
