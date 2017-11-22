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
import java.util.Calendar

import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.{Ignite, Ignition}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.ignite.spark.IgniteRelationProvider._

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

        //Adjust the logger to exclude the logs of no interest.
        Logger.getRootLogger.setLevel(Level.ERROR)
        Logger.getLogger("org.apache.ignite").setLevel(Level.INFO)

        //Executing examples.

        sparkDSMExample

        nativeSparkSqlExample

        nativeSparkSqlFromCacheExample

        nativeSparkSqlFromCacheExample2
    }

    /**
      * Examples of usage Ignite DataFrame implementation.
      * Selecting data throw Spark DSL.
      *
      * @param spark SparkSession.
      */
    def sparkDSMExample(implicit spark: SparkSession): Unit = {
        val igniteDF = spark.read
            .format(IGNITE) //Data source type.
            .option(TABLE, "person") //Table to read.
            .option(CONFIG_FILE, CONFIG) //Ignite config.
            .load()
            .filter(col("id") >= 2) //Filter clause.
            .filter(col("name") like "%M%") //Another filter clause.

        igniteDF.printSchema() //Printing query schema to console.
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
        val df = spark.read
            .format(IGNITE) //Data source type.
            .option(TABLE, "person") //Table to read.
            .option(CONFIG_FILE, CONFIG) //Ignite config.
            .load()

        //Registering DataFrame as Spark view.
        df.createOrReplaceTempView("person")

        //Selecting data from Ignite throw Spark SQL Engine.
        val igniteDF = spark.sql("SELECT * FROM person WHERE id >= 2 AND name = 'Mary Major'")

        igniteDF.printSchema() //Printing query schema to console.
        igniteDF.show() //Printing query results to console.
    }

    /**
      * Examples of usage Ignite DataFrame implementation.
      * Registration of Ignite DataFrame for following usage.
      * Selecting data from key-value cache by Spark SQL query.
      *
      * @param spark SparkSession.
      */
    def nativeSparkSqlFromCacheExample(implicit spark: SparkSession): Unit = {
        val df = spark.read
            .format(IGNITE) //Data source type.
            .option(CONFIG_FILE, CONFIG) //Ignite config.
            .option(CACHE, CACHE_NAME) //Cache name.
            .option(KEY_CLASS, "java.lang.Long") //Class of keys in cache.
            .option(VALUE_CLASS, "java.lang.String") //Class of value in cache.
            .load()

        //Printing data frame schema to console.
        df.printSchema()

        //Registering DataFrame as Spark view.
        df.createOrReplaceTempView("testCache")

        //Selecting data.
        val igniteDF = spark.sql("SELECT key, value FROM testCache WHERE key >= 2 AND value like '%0'")

        igniteDF.printSchema() //Printing query schema to console.
        igniteDF.show() //Printing query results to console.
    }

    /**
      * Examples of usage Ignite DataFrame implementation.
      * Registration of Ignite DataFrame for following usage.
      * Selecting data from key-value cache by Spark SQL query.
      *
      * @param spark SparkSession.
      */
    def nativeSparkSqlFromCacheExample2(implicit spark: SparkSession): Unit = {
        val df = spark.read
            .format(IGNITE) //Data source type.
            .option(CONFIG_FILE, CONFIG) //Ignite config.
            .option(CACHE, "testCache2") //Cache name.
            .option(KEY_CLASS, "java.lang.Long") //Class of keys in cache.
            .option(VALUE_CLASS, classOf[Person].getName) //Class of value in cache.
            .load()

        //Printing data frame schema to console.
        df.printSchema()

        //Printing data frame schema to console.
        df.createOrReplaceTempView("testCache")

        //Selecting data.
        val igniteDF = spark.sql(
            """
              | SELECT
              |  key,
              |  `value.name`,
              |  `value.birthDate`
              | FROM
              |  testCache
              | WHERE
              |  key >= 2 AND
              |  `value.name` like '%0' """.stripMargin)

        igniteDF.printSchema() //Printing query schema to console.
        igniteDF.show() //Printing query results to console.
    }

    def setupServerAndData: Ignite = {
        //Starting Ignite.
        val ignite = Ignition.start(CONFIG)

        //Creating first test cache.
        val ccfg = new CacheConfiguration[JLong, JString](CACHE_NAME).setSqlSchema("PUBLIC")

        val cache = ignite.getOrCreateCache(ccfg)

        //Putting some data first cache.
        for (i ← 1L to 100L)
            cache.put(i, i.toString)

        //Creating second test cache.
        val cache2 = ignite.getOrCreateCache(new CacheConfiguration[JLong, Person](CACHE_NAME + "2"))

        //Putting some data to second cache.
        for (i ← 1L to 100L) {
            val d = Calendar.getInstance

            d.set(Calendar.DAY_OF_YEAR, i.toInt)

            cache2.put(i, Person(s"John Connor $i", d.getTime))
        }

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

        println("Populated data.")

        ignite
    }

    case class Person(name: String, birthDate: java.util.Date)
}
