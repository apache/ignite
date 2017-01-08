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

import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.spark.{IgniteContext, IgniteRDD}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * This example demonstrates the simplest code that creates the IgnitedRDD and
  * shares it with multiple spark workers. The goal of this particular
  * example is to provide the simplest code example of this logic.
  * <p/>
  * Remote nodes should always be started with special configuration file which
  * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
  * <p/>
  * Alternatively you can run `ExampleNodeStartup` in another JVM which will
  * start node with `examples/config/example-ignite.xml` configuration.
  */
object ScalarSharedRDDExample extends App {

  /** Spark Configuration */
  private val conf = new SparkConf()
    .setAppName("IgniteRDDExample")
    .setMaster("local")
    .set("spark.executor.instances","2")

  /** Spark context */
  implicit val sparkContext = new SparkContext(conf)


  /** Creates Ignite context with default configuration */
  val igniteContext = new IgniteContext(sparkContext, () => new IgniteConfiguration(),false)

  /** Creates an Ignite RDD of Type (Int,Int) Integer Pair */
  val sharedRDD: IgniteRDD[Int, Int] = igniteContext.fromCache("partitioned")

  /** Fill IgniteRDD with data */
  sharedRDD.saveValues(sparkContext.parallelize(1 to 100000, 10))

  /** Transforming Pairs to contain their Squared value */
  sharedRDD.mapValues(x => (x * x))

  /** Retrieve RDD back from the Cache */
  val transformedValues = igniteContext.fromCache("partitioned")

  transformedValues.take(5).foreach(println)

  igniteContext.close(true)

  sparkContext.stop()
}
