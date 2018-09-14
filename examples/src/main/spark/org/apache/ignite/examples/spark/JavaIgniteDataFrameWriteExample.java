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
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.spark.IgniteDataFrameSettings;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static org.apache.ignite.internal.util.IgniteUtils.resolveIgnitePath;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.reverse;

/**
 *
 */
public class JavaIgniteDataFrameWriteExample {
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
        //Starting Ignite.
        Ignite ignite = Ignition.start(CONFIG);

        //Starting Ignite server node.
        setupServerAndData(ignite);

        //Creating spark session.
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Ignite data sources write example")
                .master("local")
                .config("spark.executor.instances", "2")
                .getOrCreate();

        // Adjust the logger to exclude the logs of no interest.
        Logger.getRootLogger().setLevel(Level.ERROR);
        Logger.getLogger("org.apache.ignite").setLevel(Level.INFO);

        // Executing examples.
        System.out.println("Example of writing json file to Ignite:");

        writeJSonToIgnite(ignite, spark);

        System.out.println("Example of modifying existing Ignite table data through Data Fram API:");

        editDataAndSaveToNewTable(ignite, spark);

        Ignition.stop(false);
    }

    /** */
    private static void writeJSonToIgnite(Ignite ignite, SparkSession spark) {
        //Load content of json file to data frame.
        Dataset<Row> personsDataFrame = spark.read().json(
            resolveIgnitePath("examples/src/main/resources/person.json").getAbsolutePath());

        System.out.println("Json file content:");

        //Printing content of json file to console.
        personsDataFrame.show();

        System.out.println("Writing Data Frame to Ignite:");

        //Writing content of data frame to Ignite.
        personsDataFrame.write()
                .format(IgniteDataFrameSettings.FORMAT_IGNITE())
                .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG)
                .option(IgniteDataFrameSettings.OPTION_TABLE(), "json_person")
                .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS(), "id")
                .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PARAMETERS(), "template=replicated")
                .save();

        System.out.println("Done!");

        System.out.println("Reading data from Ignite table:");

        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(CACHE_NAME);

        IgniteCache<?, ?> cache = ignite.getOrCreateCache(ccfg);

        //Reading saved data from Ignite.
        List<List<?>> data = cache.query(new SqlFieldsQuery("SELECT id, name, department FROM json_person")).getAll();

        System.out.println(data);
    }

    /** */
    private static void editDataAndSaveToNewTable(Ignite ignite, SparkSession spark) {
        //Load content of Ignite table to data frame.
        Dataset<Row> personDataFrame = spark.read()
                .format(IgniteDataFrameSettings.FORMAT_IGNITE())
                .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG)
                .option(IgniteDataFrameSettings.OPTION_TABLE(), "person")
                .load();

        System.out.println("Data frame content:");

        //Printing content of data frame to console.
        personDataFrame.show();

        System.out.println("Modifying Data Frame and write it to Ignite:");

        personDataFrame
                .withColumn("id", col("id").plus(42)) //Edit id column
                .withColumn("name", reverse(col("name"))) //Edit name column
                .write().format(IgniteDataFrameSettings.FORMAT_IGNITE())
                .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG)
                .option(IgniteDataFrameSettings.OPTION_TABLE(), "new_persons")
                .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS(), "id, city_id")
                .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PARAMETERS(), "backups=1")
                .mode(SaveMode.Overwrite) //Overwriting entire table.
                .save();

        System.out.println("Done!");

        System.out.println("Reading data from Ignite table:");

        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(CACHE_NAME);

        IgniteCache<?, ?> cache = ignite.getOrCreateCache(ccfg);

        //Reading saved data from Ignite.
        List<List<?>> data = cache.query(new SqlFieldsQuery("SELECT id, name, city_id FROM new_persons")).getAll();

        System.out.println(data);
    }

    /** */
    private static void setupServerAndData(Ignite ignite) {
        //Creating first test cache.
        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(CACHE_NAME).setSqlSchema("PUBLIC");

        IgniteCache<?, ?> cache = ignite.getOrCreateCache(ccfg);

        //Creating SQL table.
        cache.query(new SqlFieldsQuery(
                "CREATE TABLE person (id LONG, name VARCHAR, city_id LONG, PRIMARY KEY (id)) " +
                        "WITH \"backups=1\"")).getAll();

        cache.query(new SqlFieldsQuery("CREATE INDEX on Person (city_id)")).getAll();

        //Inserting some data to tables.
        SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO person (id, name, city_id) values (?, ?, ?)");

        cache.query(qry.setArgs(1L, "John Doe", 3L)).getAll();
        cache.query(qry.setArgs(2L, "Jane Roe", 2L)).getAll();
        cache.query(qry.setArgs(3L, "Mary Major", 1L)).getAll();
        cache.query(qry.setArgs(4L, "Richard Miles", 2L)).getAll();
    }
}
