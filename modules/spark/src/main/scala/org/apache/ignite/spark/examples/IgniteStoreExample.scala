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

package org.apache.ignite.spark.examples

import org.apache.ignite.spark.IgniteContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object IgniteStoreExample {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("Ignite store example")
        val sc = new SparkContext(conf)

        val ignite = new IgniteContext[String, String](sc, () ⇒ ExampleConfiguration.configuration())

        val lines: RDD[String] = sc.textFile(args(0)).filter(line ⇒ {
            println("Read line: " + line)

            line.contains("IGNITE")
        })

        ignite.fromCache("partitioned").saveValues(lines)
        ignite.fromCache("partitioned").savePairs(lines.map(l ⇒ (l, l)))
    }
}