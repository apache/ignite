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

package org.apache.ignite.spark.examples.java;

import org.apache.ignite.spark.*;
import org.apache.spark.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;

import scala.*;

import java.lang.Boolean;

/**
 * Ignite process example.
 */
public class IgniteProcessExample {
    /** Filter function. */
    private static final Function<Tuple2<Object, String>, Boolean> FILTER_F =
        new Function<Tuple2<Object, String>, Boolean>() {
            @Override public Boolean call(Tuple2<Object, String> t) throws Exception {
                System.out.println("Analyzing line: " + t._2());

                return t._2().contains("Ignite");
            }
        };

    /** To value function. */
    private static final Function<Tuple2<Object, String>, String> TO_VALUE_F =
        new Function<Tuple2<Object, String>, String>() {
            @Override public String call(Tuple2<Object, String> t) throws Exception {
                return t._2();
            }
        };

    /**
     * @param args Args.
     */
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();

        conf.setAppName("Ignite processing example");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaIgniteContext<Object, String> ignite = new JavaIgniteContext<>(sc, new ExampleConfiguration());

        // Search for lines containing "Ignite".
        JavaIgniteRDD<Object, String> scanRdd = ignite.fromCache("partitioned");

        JavaRDD<String> processedRdd = scanRdd.filter(FILTER_F).map(TO_VALUE_F);

        // Create a new cache for results.
        JavaIgniteRDD<Object, String> results = ignite.fromCache("results");

        results.saveValues(processedRdd);

        // SQL query
        ignite.fromCache("indexed").objectSql("Person", "age > ? and organizationId = ?", 20, 12).collect();

        // SQL fields query
        DataFrame df = ignite.fromCache("indexed").sql("select name, age from Person where age > ?", 20);
    }
}
