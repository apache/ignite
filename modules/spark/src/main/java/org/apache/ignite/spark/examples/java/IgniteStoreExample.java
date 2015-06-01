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
import org.apache.spark.api.java.function.*;

import scala.*;

import java.lang.Boolean;

/**
 * Ignite store example.
 */
public class IgniteStoreExample {
    /** Predicate. */
    private static final Function<String, Boolean> PREDICATE = new Function<String, Boolean>() {
        @Override public Boolean call(String s) throws Exception {
            System.out.println("Read line: " + s);

            return s.contains("Ignite");
        }
    };

    /** To pair function. */
    private static final PairFunction<String, String, String> TO_PAIR_F = new PairFunction<String, String, String>() {
        @Override public Tuple2<String, String> call(String s) throws Exception {
            return new Tuple2<>(s, s);
        }
    };

    /**
     * @param args Args.
     */
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();

        conf.setAppName("Ignite processing example");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaIgniteContext<String, String> ignite = new JavaIgniteContext<>(sc, new ExampleConfiguration());

        JavaRDD<String> lines = sc.textFile(args[0]).filter(PREDICATE);

        ignite.fromCache("partitioned").saveValues(lines);

        ignite.fromCache("partitioned").savePairs(lines.mapToPair(TO_PAIR_F));
    }
}
