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

package org.apache.ignite.examples.spark;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.spark.IgniteDataFrameSettings;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Example application demonstrates the join operations between two dataframes or Spark tables with data saved in Ignite caches.
 */
public class JavaIgniteDataFrameJoinExample {
    /** Ignite config file. */
    private static final String CONFIG = "examples/config/example-ignite.xml";

    /** Test cache name. */
    private static final String CACHE_NAME = "testCache";

    /** */
    public static void main(String args[]) {

        setupServerAndData();

        // Creating spark session.
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaIgniteDataFrameJoinExample")
                .master("local")
                .config("spark.executor.instances", "2")
                .getOrCreate();

        // Adjust the logger to exclude the logs of no interest.
        Logger.getRootLogger().setLevel(Level.ERROR);
        Logger.getLogger("org.apache.ignite").setLevel(Level.INFO);

        // Executing examples.
        sparkDSLJoinExample(spark);
        nativeSparkSqlJoinExample(spark);

        Ignition.stop(false);
    }

    /** */
    private static void sparkDSLJoinExample(SparkSession spark) {
        System.out.println("Querying using Spark DSL.");

        Dataset<Row> persons = spark.read()
                .format(IgniteDataFrameSettings.FORMAT_IGNITE())
                .option(IgniteDataFrameSettings.OPTION_TABLE(), "person")
                .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG)
                .load();

        persons.printSchema();
        persons.show();

        Dataset<Row> cities = spark.read()
                .format(IgniteDataFrameSettings.FORMAT_IGNITE())
                .option(IgniteDataFrameSettings.OPTION_TABLE(), "city")
                .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG)
                .load();

        cities.printSchema();
        cities.show();

        Dataset<Row> joinResult = persons.join(cities, persons.col("city_id").equalTo(cities.col("id")))
                .select(persons.col("name").as("person"), persons.col("age"), cities.col("name").as("city"), cities.col("country"));

        joinResult.explain(true);
        joinResult.printSchema();
        joinResult.show();
    }

    /** */
    private static void nativeSparkSqlJoinExample(SparkSession spark) {
        System.out.println("Querying using Spark SQL.");

        Dataset<Row> persons = spark.read()
                .format(IgniteDataFrameSettings.FORMAT_IGNITE())
                .option(IgniteDataFrameSettings.OPTION_TABLE(), "person")
                .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG)
                .load();

        persons.printSchema();
        persons.show();

        Dataset<Row> cities = spark.read()
                .format(IgniteDataFrameSettings.FORMAT_IGNITE())
                .option(IgniteDataFrameSettings.OPTION_TABLE(), "city")
                .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG)
                .load();

        cities.printSchema();
        cities.show();

        // Registering DataFrame as Spark view.
        persons.createOrReplaceTempView("person");
        cities.createOrReplaceTempView("city");

        // Selecting data from Ignite through Spark SQL Engine.
        Dataset<Row> joinResult = spark.sql("SELECT person.name AS person, age, city.name AS city, country FROM person JOIN city ON person.city_id = city.id");

        joinResult.explain(true);
        joinResult.printSchema();
        joinResult.show();
    }

    /** */
    private static void setupServerAndData() {
        // Starting Ignite.
        Ignite ignite = Ignition.start(CONFIG);

        // Creating first test cache.
        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(CACHE_NAME).setSqlSchema("PUBLIC");

        IgniteCache<?, ?> cache = ignite.getOrCreateCache(ccfg);

        // Creating SQL tables.
        cache.query(new SqlFieldsQuery(
                "CREATE TABLE city (id LONG PRIMARY KEY, name VARCHAR, country VARCHAR) WITH \"template=replicated\"")).getAll();

        cache.query(new SqlFieldsQuery(
                "CREATE TABLE person (id LONG, name VARCHAR, age INT, city_id LONG, PRIMARY KEY (id, city_id)) " +
                        "WITH \"backups=1, affinity_key=city_id\"")).getAll();

        cache.query(new SqlFieldsQuery("CREATE INDEX on Person (city_id)")).getAll();

        SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO city (id, name, country) VALUES (?, ?, ?)");

        // Inserting some data to tables.
        cache.query(qry.setArgs(1L, "Forest Hill", "USA")).getAll();
        cache.query(qry.setArgs(2L, "Denver", "USA")).getAll();
        cache.query(qry.setArgs(3L, "St. Petersburg", "Russia")).getAll();

        qry = new SqlFieldsQuery("INSERT INTO person (id, name, age, city_id) values (?, ?, ?, ?)");

        cache.query(qry.setArgs(1L, "Alexey Zinoviev", 31, 3L)).getAll();
        cache.query(qry.setArgs(2L, "Jane Roe", 27, 2L)).getAll();
        cache.query(qry.setArgs(3L, "Mary Major", 86, 1L)).getAll();
        cache.query(qry.setArgs(4L, "Richard Miles", 19, 2L)).getAll();
    }
}
