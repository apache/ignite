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
import org.apache.spark.{SparkConf, SparkContext}

/**
  * This example demonstrates the simplest code that creates the IgnitedRDD and
  * shares it with multiple spark workers. The goal of this particular
  * example is to provide the simplest code example of this logic.
  * <p/>
  * This example will start Ignite in embedded mode and will start an
  * IgniteContext on each Spark worker node. It can also be
  * in standalone mode which can bet set while instantiating IgniteContext
  * (set isClient to true, it is currently set to false as last argument to
  * IgniteContext constructor) and running an Ignite node separately.
  * <p/>
  */
object ScalarSharedRDDExample extends App {
  /** Spark Configuration */
  private val conf = new SparkConf()
    .setAppName("IgniteRDDExample")
    .setMaster("local")
    .set("spark.executor.instances", "2")

  /** Spark context */
  val sparkContext = new SparkContext(conf)

  /** Defines spring cache Configuration path */
  private val CONFIG = "examples/config/example-shared-rdd.xml"

  /** Creates Ignite context with above configuration configuration */
  val igniteContext = new IgniteContext(sparkContext, CONFIG, false)

  /** Creates an Ignite RDD of Type (Int,Int) Integer Pair */
  val sharedRDD: IgniteRDD[Int, Int] = igniteContext.fromCache[Int, Int]("sharedRDD")

  /** Fill IgniteRDD with Int pairs */
  sharedRDD.savePairs(sparkContext.parallelize(1 to 100000, 10).map(i => (i, i)))

  /** Transforming Pairs to contain their Squared value */
  sharedRDD.mapValues(x => (x * x))

  /** Retrieve sharedRDD back from the Cache */
  val transformedValues: IgniteRDD[Int, Int] = igniteContext.fromCache("sharedRDD")

  /** Perform some transformations on IgniteRDD and print */
  val squareAndRootPair = transformedValues.map { case (x,y) => (x,Math.sqrt(y.toDouble)) }

  /** filters pairs whose square roots are less than 100 and
    * takes five elements from the transformed IgniteRDD and prints it*/
  squareAndRootPair.filter( _._2 < 100.0 ).take(5).foreach(println)

  /** Performing SQL query on existing cache and
    * collect result into a Spark Dataframe
    * */
  val df = transformedValues.sql("select _val from Integer where _val < 100 and _val > 9 ")

  /** Show DataFrame results (only 10) */
  df.show(10)

  /** Close IgniteContext on all workers */
  igniteContext.close(true)

  /**Stop SparkContext */
  sparkContext.stop()
}
