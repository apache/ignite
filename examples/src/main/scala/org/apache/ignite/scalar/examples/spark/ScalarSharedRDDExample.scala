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

import org.apache.ignite.spark.{IgniteContext, IgniteRDD}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * This example demonstrates how to create an IgnitedRDD and share it with multiple spark workers.
  * The goal of this particular example is to provide the simplest code example of this logic.
  * <p>
  * This example will start Ignite in the embedded mode and will start an IgniteContext on each Spark worker node.
  * <p>
  * The example can work in the standalone mode as well that can be enabled by setting IgniteContext's {@code isClient}
  * property to {@code true} and running an Ignite node separately with `examples/config/spark/
  * example-shared-rdd.xml` config.
  * <p>
  */
object ScalarSharedRDDExample extends App {
    // Spark Configuration.
    private val conf = new SparkConf()
        .setAppName("IgniteRDDExample")
        .setMaster("local")
        .set("spark.executor.instances", "2")

    // Spark context.
    val sparkContext = new SparkContext(conf)

    // Adjust the logger to exclude the logs of no interest.
    Logger.getRootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.ignite").setLevel(Level.INFO)

    // Defines spring cache Configuration path.
    private val CONFIG = "examples/config/spark/example-shared-rdd.xml"

    // Creates Ignite context with above configuration.
    val igniteContext = new IgniteContext(sparkContext, CONFIG, false)

    // Creates an Ignite Shared RDD of Type (Int,Int) Integer Pair.
    val sharedRDD: IgniteRDD[Int, Int] = igniteContext.fromCache[Int, Int]("sharedRDD")

    // Fill the Ignite Shared RDD in with Int pairs.
    sharedRDD.savePairs(sparkContext.parallelize(1 to 100000, 10).map(i => (i, i)))

    // Transforming Pairs to contain their Squared value.
    sharedRDD.mapValues(x => (x * x))

    // Retrieve sharedRDD back from the Cache.
    val transformedValues: IgniteRDD[Int, Int] = igniteContext.fromCache("sharedRDD")

    // Perform some transformations on IgniteRDD and print.
    val squareAndRootPair = transformedValues.map { case (x, y) => (x, Math.sqrt(y.toDouble)) }

    println(">>> Transforming values stored in Ignite Shared RDD...")

    // Filter out pairs which square roots are less than 100 and
    // take the first five elements from the transformed IgniteRDD and print them.
    squareAndRootPair.filter(_._2 < 100.0).take(5).foreach(println)

    println(">>> Executing SQL query over Ignite Shared RDD...")

    // Execute a SQL query over the Ignite Shared RDD.
    val df = transformedValues.sql("select _val from Integer where _val < 100 and _val > 9 ")

    // Show ten rows from the result set.
    df.show(10)

    // Close IgniteContext on all workers.
    igniteContext.close(true)

    // Stop SparkContext.
    sparkContext.stop()
}
