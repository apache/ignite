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

package org.apache.ignite.internal.ducktest;

import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.ignite.spark.IgniteDataFrameSettings;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.ignite.IgniteSparkSession;

/**
 *
 */
public class SparkApplication extends IgniteAwareApplication {
    /** Home. */
    public static final String HOME = "/opt/ignite-dev";

    /** Version. */
    public static final String VER = "2.10.0-SNAPSHOT";

    /** Spring version. */
    public static final String SPRING_VER = "4.3.26.RELEASE";

    /**
     * @param masterUrl Master url.
     */
    private static void sparkSession(String cfgPath, String masterUrl) {
        //Creating spark session.
        try (SparkSession spark = SparkSession.builder()
            .appName("SparkApplication")
            .master(masterUrl)
            .getOrCreate()) {
            spark.sparkContext().addJar(HOME + "/modules/core/target/ignite-core-" + VER + ".jar");
            spark.sparkContext().addJar(HOME + "/modules/spring/target/ignite-spring-" + VER + ".jar");
            spark.sparkContext().addJar(HOME + "/modules/log4j/target/ignite-log4j-" + VER + ".jar");
            spark.sparkContext().addJar(HOME + "/modules/spark/target/ignite-spark-" + VER + ".jar");
            spark.sparkContext().addJar(HOME + "/modules/indexing/target/ignite-indexing-" + VER + ".jar");
            spark.sparkContext().addJar(HOME + "/modules/spring/target/libs/spring-beans-" + SPRING_VER + ".jar");
            spark.sparkContext().addJar(HOME + "/modules/spring/target/libs/spring-core-" + SPRING_VER + ".jar");
            spark.sparkContext().addJar(HOME + "/modules/spring/target/libs/spring-context-" + SPRING_VER + ".jar");
            spark.sparkContext().addJar(HOME + "/modules/spring/target/libs/spring-expression-" + SPRING_VER + ".jar");
            spark.sparkContext().addJar(HOME + "/modules/core/target/libs/cache-api-1.0.0.jar");
            spark.sparkContext().addJar(HOME + "/modules/indexing/target/libs/h2-1.4.197.jar");

            sparkDSLExample(cfgPath, spark);
        }
    }

    /**
     * @param masterUrl Master url.
     * @param cfgPath Config path.
     */
    private static void igniteSession(String cfgPath, String masterUrl) {
        //Creating spark session.
        try (IgniteSparkSession spark = IgniteSparkSession.builder()
            .appName("SparkApplication")
            .igniteConfig(cfgPath)
            .master(masterUrl)
            .getOrCreate()) {
            spark.sparkContext().addJar(HOME + "/modules/core/target/ignite-core-" + VER + ".jar");
            spark.sparkContext().addJar(HOME + "/modules/spring/target/ignite-spring-" + VER + ".jar");
            spark.sparkContext().addJar(HOME + "/modules/log4j/target/ignite-log4j-" + VER + ".jar");
            spark.sparkContext().addJar(HOME + "/modules/spark/target/ignite-spark-" + VER + ".jar");
            spark.sparkContext().addJar(HOME + "/modules/indexing/target/ignite-indexing-" + VER + ".jar");
            spark.sparkContext().addJar(HOME + "/modules/spring/target/libs/spring-beans-" + SPRING_VER + ".jar");
            spark.sparkContext().addJar(HOME + "/modules/spring/target/libs/spring-core-" + SPRING_VER + ".jar");
            spark.sparkContext().addJar(HOME + "/modules/spring/target/libs/spring-context-" + SPRING_VER + ".jar");
            spark.sparkContext().addJar(HOME + "/modules/spring/target/libs/spring-expression-" + SPRING_VER + ".jar");
            spark.sparkContext().addJar(HOME + "/modules/core/target/libs/cache-api-1.0.0.jar");
            spark.sparkContext().addJar(HOME + "/modules/indexing/target/libs/h2-1.4.197.jar");

            spark.catalog().listTables().show();

            sparkDSLExample(cfgPath, spark);
        }
    }

    /**
     * @param spark Spark.
     * @param cfgPath Config path.
     */
    private static void sparkDSLExample(String cfgPath, SparkSession spark) {
        System.out.println("Querying using Spark DSL.");

        Dataset<Row> igniteDF = spark.read()
            .format(IgniteDataFrameSettings.FORMAT_IGNITE()) //Data source type.
            .option(IgniteDataFrameSettings.OPTION_TABLE(), "person") //Table to read.
            .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), cfgPath) //Ignite config.
            .load();

        System.out.println("Data frame schema:");

        igniteDF.printSchema(); //Printing query schema to console.

        System.out.println("Data frame content:");

        igniteDF.show(); //Printing query results to console.
    }

    /** {@inheritDoc} */
    @Override protected void run(String[] args) throws Exception {
        System.out.println("SparkApplication.main - args");

        for (String arg : args)
            System.out.println("SparkApplication.main - " + arg);

        sparkSession(args[0], args[1]);

        igniteSession(args[0], args[1]);

        markSyncExecutionComplete();
    }
}
