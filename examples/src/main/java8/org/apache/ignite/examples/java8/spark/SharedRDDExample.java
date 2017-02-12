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

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import scala.Tuple2;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This example demonstrates the simplest code that creates the JavaIgnitedRDD and
 * shares it with multiple spark workers. The goal of this particular
 * example is to provide the simplest code example of this logic.
 * <p/>
 * This example will start Ignite in embedded mode and will start
 * JavaIgniteContext on each Spark worker node. It can also be
 * in standalone mode which can bet set while instantiating JavaIgniteContext
 * (set standalone to true, it is currently set to false as last argument to
 * JavaIgniteContext constructor) and running an Ignite node separately.
 * <p/>
 */

public class SharedRDDExample {

    public static void main(String args[]) {
        /** Spark Configuration */
        SparkConf sparkConf = new SparkConf()
                .setAppName("JavaIgniteRDDExample")
                .setMaster("local")
                .set("spark.executor.instances", "2");

        /** Spark context */
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        /** Creates Ignite context with above configuration */
        JavaIgniteContext igniteContext =
                new JavaIgniteContext(sparkContext, IgniteConfiguration::new, false);

        /** Creates Cache configuration */
        CacheConfiguration<Integer, Integer> cacheCfg = new CacheConfiguration<>();

        /** Sets a name for the Cache*/
        cacheCfg.setName("sharedRdd");

        /** Sets Indexing properties on cache this is
         * required in order to perform sql queries on caches
         * as Spark Dataframe
         **/
        cacheCfg.setIndexedTypes(Integer.class,Integer.class);

        /** Creates a Java Ignite RDD of Type (Int,Int) Integer Pair */
        JavaIgniteRDD<Integer,Integer> sharedRDD = igniteContext.fromCache(cacheCfg);

        /** Defines data to stored in the cache*/
        List<Integer> data = IntStream.range(0, 10).boxed().collect(Collectors.toList());

        /** Fill IgniteRDD with Int pairs. Here Pairs are represented as Scala Tuple2 */
        sharedRDD.savePairs(sparkContext.parallelize(data).mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                return new Tuple2(integer,integer);
            }
        }));

        /** Gets reference of sharedRDD from Cache */
        JavaIgniteRDD<Integer,Integer> values  = igniteContext.fromCache(cacheCfg);

        values.foreach((x) -> System.out.println("(" + x._1 + "," + x._2 + ")"));

        /** filters out  even values as transformed RDD */
        JavaPairRDD<Integer,Integer> transformedValues  =
                sharedRDD.filter( (Tuple2<Integer,Integer> pair) -> pair._2() % 2 == 0);

        /** Prints the transformed values */
        transformedValues.foreach((x) -> System.out.println("(" + x._1 + "," + x._2 + ")"));

        /** Performs SQL query on existing cache and collects
         * values into a DataFrame
         **/
        DataFrame df = values.sql("select _val from Integer where _val < 9");

        /** Show the Dataframe contents */
        df.show();

        /** Closes IgniteContext on all workers */
        igniteContext.close(true);
    }
}