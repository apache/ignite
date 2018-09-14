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

import java.lang.{Long ⇒ JLong}

import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.{Ignite, Ignition}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.ignite.IgniteSparkSession

/**
  * Example application to show use-case for Ignite implementation of Spark SQL {@link org.apache.spark.sql.catalog.Catalog}.
  * Catalog provides ability to automatically resolve SQL tables created in Ignite.
  */
object IgniteCatalogExample extends App {
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
        //Creating Ignite-specific implementation of Spark session.
        val igniteSession = IgniteSparkSession.builder()
            .appName("Spark Ignite catalog example")
            .master("local")
            .config("spark.executor.instances", "2")
            .igniteConfig(CONFIG)
            .getOrCreate()

        //Adjust the logger to exclude the logs of no interest.
        Logger.getRootLogger.setLevel(Level.ERROR)
        Logger.getLogger("org.apache.ignite").setLevel(Level.INFO)

        println("List of available tables:")

        //Showing existing tables.
        igniteSession.catalog.listTables().show()

        println("PERSON table description:")

        //Showing `person` schema.
        igniteSession.catalog.listColumns("person").show()

        println("CITY table description:")

        //Showing `city` schema.
        igniteSession.catalog.listColumns("city").show()

        println("Querying all persons from city with ID=2.")
        println

        //Selecting data throw Spark SQL engine.
        val df = igniteSession.sql("SELECT * FROM person WHERE CITY_ID = 2")

        println("Result schema:")

        df.printSchema()

        println("Result content:")

        df.show()

        println("Querying all persons living in Denver.")
        println

        //Selecting data throw Spark SQL engine.
        val df2 = igniteSession.sql("SELECT * FROM person p JOIN city c ON c.ID = p.CITY_ID WHERE c.NAME = 'Denver'")

        println("Result schema:")

        df2.printSchema()

        println("Result content:")

        df2.show()
    }

    /**
      * Starting ignite server node and creating.
      *
      * @return Ignite server node.
      */
    def setupServerAndData: Ignite = {
        //Starting Ignite.
        val ignite = Ignition.start(CONFIG)

        //Creating cache.
        val ccfg = new CacheConfiguration[Int, Int](CACHE_NAME).setSqlSchema("PUBLIC")

        val cache = ignite.getOrCreateCache(ccfg)

        //Create tables.
        cache.query(new SqlFieldsQuery(
            "CREATE TABLE city (id LONG PRIMARY KEY, name VARCHAR) WITH \"template=replicated\"")).getAll

        cache.query(new SqlFieldsQuery(
            "CREATE TABLE person (id LONG, name VARCHAR, city_id LONG, PRIMARY KEY (id, city_id)) " +
                "WITH \"backups=1, affinityKey=city_id\"")).getAll

        cache.query(new SqlFieldsQuery("CREATE INDEX on Person (city_id)")).getAll

        //Inserting some data into table.
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
