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
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._

/**
  * Example application showing use-case for writing Spark DataFrame API to Ignite.
  */
object IgniteDataFrameWriteExample extends App {
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

    closeAfter(ignite) { _ ⇒
        //Creating spark session.
        implicit val spark: SparkSession = SparkSession.builder()
            .appName("Spark Ignite data sources write example")
            .master("local")
            .config("spark.executor.instances", "2")
            .getOrCreate()

        // Adjust the logger to exclude the logs of no interest.
        Logger.getRootLogger.setLevel(Level.INFO)
        Logger.getLogger("org.apache.ignite").setLevel(Level.INFO)

        // Executing examples.
        println("Example of writing json file to Ignite:")

        writeJSonToIgnite

        println("Example of modifying existing Ignite table data through Data Fram API:")

        editDataAndSaveToNewTable
    }

    def writeJSonToIgnite(implicit spark: SparkSession): Unit = {
        //Load content of json file to data frame.
        val personsDataFrame = spark.read.json("examples/src/main/resources/person.json")

        println()
        println("Json file content:")
        println()

        //Printing content of json file to console.
        personsDataFrame.show()

        println()
        println("Writing Data Frame to Ignite:")
        println()

        //Writing content of data frame to Ignite.
        personsDataFrame.write
            .format(FORMAT_IGNITE)
            .option(OPTION_CONFIG_FILE, CONFIG)
            .option(OPTION_TABLE, "json_person")
            .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "id")
            .option(OPTION_CREATE_TABLE_PARAMETERS, "template=replicated")
            .save()

        println("Done!")

        println()
        println("Reading data from Ignite table:")
        println()

        val cache = ignite.cache[Any, Any](CACHE_NAME)

        //Reading saved data from Ignite.
        val data = cache.query(new SqlFieldsQuery("SELECT id, name, department FROM json_person")).getAll

        data.foreach { row ⇒ println(row.mkString("[", ", ", "]")) }
    }

    def editDataAndSaveToNewTable(implicit spark: SparkSession): Unit = {
        //Load content of Ignite table to data frame.
        val personDataFrame = spark.read
            .format(FORMAT_IGNITE)
            .option(OPTION_CONFIG_FILE, CONFIG)
            .option(OPTION_TABLE, "person")
            .load()

        println()
        println("Data frame content:")
        println()

        //Printing content of data frame to console.
        personDataFrame.show()

        println()
        println("Modifying Data Frame and write it to Ignite:")
        println()

        personDataFrame
            .withColumn("id", col("id") + 42) //Edit id column
            .withColumn("name", reverse(col("name"))) //Edit name column
            .write.format(FORMAT_IGNITE)
            .option(OPTION_CONFIG_FILE, CONFIG)
            .option(OPTION_TABLE, "new_persons")
            .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "id, city_id")
            .option(OPTION_CREATE_TABLE_PARAMETERS, "backups=1")
            .mode(SaveMode.Overwrite) //Overwriting entire table.
            .save()

        println("Done!")

        println()
        println("Reading data from Ignite table:")
        println()

        val cache = ignite.cache[Any, Any](CACHE_NAME)

        //Reading saved data from Ignite.
        val data = cache.query(new SqlFieldsQuery("SELECT id, name, city_id FROM new_persons")).getAll

        data.foreach { row ⇒ println(row.mkString("[", ", ", "]")) }
    }

    def setupServerAndData: Ignite = {
        //Starting Ignite.
        val ignite = Ignition.start(CONFIG)

        //Creating first test cache.
        val ccfg = new CacheConfiguration[JLong, JString](CACHE_NAME).setSqlSchema("PUBLIC")

        val cache = ignite.getOrCreateCache(ccfg)

        //Creating SQL table.
        cache.query(new SqlFieldsQuery(
            "CREATE TABLE person (id LONG, name VARCHAR, city_id LONG, PRIMARY KEY (id)) " +
                "WITH \"backups=1\"")).getAll

        cache.query(new SqlFieldsQuery("CREATE INDEX on Person (city_id)")).getAll

        //Inserting some data to tables.
        val qry = new SqlFieldsQuery("INSERT INTO person (id, name, city_id) values (?, ?, ?)")

        cache.query(qry.setArgs(1L.asInstanceOf[JLong], "John Doe", 3L.asInstanceOf[JLong])).getAll
        cache.query(qry.setArgs(2L.asInstanceOf[JLong], "Jane Roe", 2L.asInstanceOf[JLong])).getAll
        cache.query(qry.setArgs(3L.asInstanceOf[JLong], "Mary Major", 1L.asInstanceOf[JLong])).getAll
        cache.query(qry.setArgs(4L.asInstanceOf[JLong], "Richard Miles", 2L.asInstanceOf[JLong])).getAll

        ignite
    }
}
