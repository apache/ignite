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

import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spark.*;
import org.apache.spark.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import scala.Tuple2;

import java.util.*;

/**
 * Colocation test example.
 */
public class ColocationTest {
    /** Keys count. */
    private static final int KEYS_CNT = 10000;

    /** To pair function. */
    private static final IgniteClosure<Integer, Tuple2<Integer, Integer>> TO_PAIR_F =
        new IgniteClosure<Integer, Tuple2<Integer, Integer>>() {
            @Override public Tuple2<Integer, Integer> apply(Integer i) {
                return new Tuple2<>(i, i);
            }
        };

    /** To value function. */
    private static final Function<Tuple2<Integer, Integer>, Integer> TO_VALUE_F =
        new Function<Tuple2<Integer, Integer>, Integer>() {
            /** {@inheritDoc} */
            @Override public Integer call(Tuple2<Integer, Integer> t) throws Exception {
                return t._2();
            }
        };

    /** Sum function. */
    private static final Function2<Integer, Integer, Integer> SUM_F = new Function2<Integer, Integer, Integer>() {
        public Integer call(Integer x, Integer y) {
            return x + y;
        }
    };

    /**
     * @param args Args.
     */
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();

        conf.setAppName("Colocation test");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaIgniteContext<Integer, Integer> ignite = new JavaIgniteContext<>(sc, new ExampleConfiguration());

        JavaIgniteRDD<Integer, Integer> cache = ignite.fromCache("partitioned");

        List<Integer> seq = F.range(0, KEYS_CNT + 1);

        JavaPairRDD<Integer, Integer> rdd = sc.parallelizePairs(F.transformList(seq, TO_PAIR_F), 48);

        cache.savePairs(rdd);

        int sum = (KEYS_CNT * KEYS_CNT - KEYS_CNT) / 2;

        // Execute parallel sum.
        System.out.println("Local sum: " + sum);

        System.out.println("Distributed sum: " + cache.map(TO_VALUE_F).fold(0, SUM_F));
    }
}
