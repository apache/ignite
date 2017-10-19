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

package org.apache.ignite.examples.java8.spark;

import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import scala.Tuple2;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This example demonstrates how to create an JavaIgnitedRDD and share it with multiple spark workers. The goal of this
 * particular example is to provide the simplest code example of this logic.
 * <p>
 * This example will start Ignite in the embedded mode and will start an JavaIgniteContext on each Spark worker node.
 * <p>
 * The example can work in the standalone mode as well that can be enabled by setting JavaIgniteContext's
 * {@code standalone} property to {@code true} and running an Ignite node separately with
 * `examples/config/spark/example-shared-rdd.xml` config.
 */
public class SharedRDDExample {
    /**
     * Executes the example.
     * @param args Command line arguments, none required.
     */
    public static void main(String args[]) {
        // Spark Configuration.
        SparkConf sparkConf = new SparkConf()
            .setAppName("JavaIgniteRDDExample")
            .setMaster("local")
            .set("spark.executor.instances", "2");

        // Spark context.
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // Adjust the logger to exclude the logs of no interest.
        Logger.getRootLogger().setLevel(Level.ERROR);
        Logger.getLogger("org.apache.ignite").setLevel(Level.INFO);

        // Creates Ignite context with specific configuration and runs Ignite in the embedded mode.
        JavaIgniteContext<Integer, Integer> igniteContext = new JavaIgniteContext<Integer, Integer>(
            sparkContext,"examples/config/spark/example-shared-rdd.xml", false);

        // Create a Java Ignite RDD of Type (Int,Int) Integer Pair.
        JavaIgniteRDD<Integer, Integer> sharedRDD = igniteContext.<Integer, Integer>fromCache("sharedRDD");

        // Define data to be stored in the Ignite RDD (cache).
        List<Integer> data = IntStream.range(0, 20).boxed().collect(Collectors.toList());

        // Preparing a Java RDD.
        JavaRDD<Integer> javaRDD = sparkContext.<Integer>parallelize(data);

        // Fill the Ignite RDD in with Int pairs. Here Pairs are represented as Scala Tuple2.
        sharedRDD.savePairs(javaRDD.<Integer, Integer>mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override public Tuple2<Integer, Integer> call(Integer val) throws Exception {
                return new Tuple2<Integer, Integer>(val, val);
            }
        }));

        System.out.println(">>> Iterating over Ignite Shared RDD...");

        // Iterate over the Ignite RDD.
        sharedRDD.foreach((x) -> System.out.println("(" + x._1 + "," + x._2 + ")"));

        System.out.println(">>> Transforming values stored in Ignite Shared RDD...");

        // Filter out even values as a transformed RDD.
        JavaPairRDD<Integer, Integer> transformedValues =
            sharedRDD.filter((Tuple2<Integer, Integer> pair) -> pair._2() % 2 == 0);

        // Print out the transformed values.
        transformedValues.foreach((x) -> System.out.println("(" + x._1 + "," + x._2 + ")"));

        System.out.println(">>> Executing SQL query over Ignite Shared RDD...");

        // Execute SQL query over the Ignite RDD.
        DataFrame df = sharedRDD.sql("select _val from Integer where _key < 9");

        // Show the result of the execution.
        df.show();

        // Close IgniteContext on all the workers.
        igniteContext.close(true);
    }
}