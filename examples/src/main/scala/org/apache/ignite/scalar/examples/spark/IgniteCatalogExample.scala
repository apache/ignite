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

package org.apache.ignite.scalar.examples.spark

import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.utils.closeAfter
import org.apache.ignite.{Ignite, Ignition}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import java.lang.{Long ⇒ JLong}

import org.apache.spark.sql.ignite.{IgniteOptimization, IgniteSparkSession, IgniteStrategy}

/**
  */
object IgniteCatalogExample extends App {
    // Defines spring cache Configuration path.
    private val CONFIG = "examples/config/example-ignite.xml"
    private val CACHE_NAME = "testCache"

    closeAfter(setupServerAndData) { server ⇒
        //Spark session
        val spark = SparkSession.builder()
            .appName("Spark Ignite data sources example")
            .master("local")
            .config("spark.executor.instances", "2")
            .getOrCreate()

        // Adjust the logger to exclude the logs of no interest.
        Logger.getRootLogger.setLevel(Level.ERROR)
        Logger.getLogger("org.apache.ignite").setLevel(Level.INFO)

        val igniteSession = new IgniteSparkSession(spark, server)

        igniteSession.catalog.listTables().show()

        igniteSession.catalog.listColumns("person").show()
        igniteSession.catalog.listColumns("city").show()

        val df = igniteSession.sql("SELECT * FROM person WHERE CITY_ID = 2")

        df.printSchema()
        df.show()

        val df2 = igniteSession.sql("SELECT * FROM person p JOIN city c ON c.ID = p.CITY_ID WHERE p.CITY_ID = 2")

        df2.printSchema()
        df2.show()
    }

    def setupServerAndData: Ignite = {
        val ignite = Ignition.start(CONFIG)

        val ccfg = new CacheConfiguration[Int, Int](CACHE_NAME).setSqlSchema("PUBLIC")

        val cache = ignite.getOrCreateCache(ccfg)

        for (i ← 1 to 100)
            cache.put(i, i)

        cache.query(new SqlFieldsQuery(
            "CREATE TABLE city (id LONG PRIMARY KEY, name VARCHAR) WITH \"template=replicated\"")).getAll

        cache.query(new SqlFieldsQuery(
            "CREATE TABLE person (id LONG, name VARCHAR, city_id LONG, PRIMARY KEY (id, city_id)) " +
                "WITH \"backups=1, affinityKey=city_id\"")).getAll

        cache.query(new SqlFieldsQuery("CREATE INDEX on Person (city_id)")).getAll

        println("Created database objects.")

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

}
