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

import static org.apache.spark.sql.functions.col;

/**
 *
 */
public class JavaIgniteDataFrameExample {
    /**
     * Ignite config file.
     */
    private static final String CONFIG = "examples/config/example-ignite.xml";

    /**
     * Test cache name.
     */
    private static final String CACHE_NAME = "testCache";

    /** */
    public static void main(String args[]) {

        setupServerAndData();

        //Creating spark session.
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaIgniteDataFrameExample")
                .master("local")
                .config("spark.executor.instances", "2")
                .getOrCreate();

        // Adjust the logger to exclude the logs of no interest.
        Logger.getRootLogger().setLevel(Level.ERROR);
        Logger.getLogger("org.apache.ignite").setLevel(Level.INFO);

        // Executing examples.

        sparkDSLExample(spark);

        nativeSparkSqlExample(spark);

        Ignition.stop(false);
    }

    /** */
    private static void sparkDSLExample(SparkSession spark) {
        System.out.println("Querying using Spark DSL.");

        Dataset<Row> igniteDF = spark.read()
                .format(IgniteDataFrameSettings.FORMAT_IGNITE()) //Data source type.
                .option(IgniteDataFrameSettings.OPTION_TABLE(), "person") //Table to read.
                .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG) //Ignite config.
                .load()
                .filter(col("id").geq(2)) //Filter clause.
                .filter(col("name").like("%M%")); //Another filter clause.

        System.out.println("Data frame schema:");

        igniteDF.printSchema(); //Printing query schema to console.

        System.out.println("Data frame content:");

        igniteDF.show(); //Printing query results to console.
    }

    /** */
    private static void nativeSparkSqlExample(SparkSession spark) {
        System.out.println("Querying using Spark SQL.");

        Dataset<Row> df = spark.read()
                .format(IgniteDataFrameSettings.FORMAT_IGNITE()) //Data source type.
                .option(IgniteDataFrameSettings.OPTION_TABLE(), "person") //Table to read.
                .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG) //Ignite config.
                .load();

        //Registering DataFrame as Spark view.
        df.createOrReplaceTempView("person");

        //Selecting data from Ignite through Spark SQL Engine.
        Dataset<Row> igniteDF = spark.sql("SELECT * FROM person WHERE id >= 2 AND name = 'Mary Major'");

        System.out.println("Result schema:");

        igniteDF.printSchema(); //Printing query schema to console.

        System.out.println("Result content:");

        igniteDF.show(); //Printing query results to console.
    }

    /** */
    private static void setupServerAndData() {
        //Starting Ignite.
        Ignite ignite = Ignition.start(CONFIG);

        //Creating first test cache.
        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(CACHE_NAME).setSqlSchema("PUBLIC");

        IgniteCache<?, ?> cache = ignite.getOrCreateCache(ccfg);

        //Creating SQL tables.
        cache.query(new SqlFieldsQuery(
                "CREATE TABLE city (id LONG PRIMARY KEY, name VARCHAR) WITH \"template=replicated\"")).getAll();

        cache.query(new SqlFieldsQuery(
                "CREATE TABLE person (id LONG, name VARCHAR, city_id LONG, PRIMARY KEY (id, city_id)) " +
                        "WITH \"backups=1, affinity_key=city_id\"")).getAll();

        cache.query(new SqlFieldsQuery("CREATE INDEX on Person (city_id)")).getAll();

        SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO city (id, name) VALUES (?, ?)");

        //Inserting some data to tables.
        cache.query(qry.setArgs(1L, "Forest Hill")).getAll();
        cache.query(qry.setArgs(2L, "Denver")).getAll();
        cache.query(qry.setArgs(3L, "St. Petersburg")).getAll();

        qry = new SqlFieldsQuery("INSERT INTO person (id, name, city_id) values (?, ?, ?)");

        cache.query(qry.setArgs(1L, "John Doe", 3L)).getAll();
        cache.query(qry.setArgs(2L, "Jane Roe", 2L)).getAll();
        cache.query(qry.setArgs(3L, "Mary Major", 1L)).getAll();
        cache.query(qry.setArgs(4L, "Richard Miles", 2L)).getAll();
    }
}
