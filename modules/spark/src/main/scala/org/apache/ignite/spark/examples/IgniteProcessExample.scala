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
import org.apache.spark.{SparkContext, SparkConf}

object IgniteProcessExample {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("Ignite processing example")
        val sc = new SparkContext(conf)

        val ignite = new IgniteContext[Object, String](sc, ExampleConfiguration.configuration _)

        // Search for lines containing "Ignite".
        val scanRdd = ignite.fromCache("partitioned")

        val processedRdd = scanRdd.filter(line => {
            println("Analyzing line: " + line)
            line._2.contains("Ignite")

            true
        }).map(_._2)

        // Create a new cache for results.
        val results = ignite.fromCache("results")

        results.saveValues(processedRdd)

        // SQL query
        ignite.fromCache("indexed").query("Person", "age > ?", 20).collect()

        // SQL fields query
        ignite.fromCache("indexed").queryFields("select name, age from Person where age > ?", 20).collect()
    }
}