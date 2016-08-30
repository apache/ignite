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

package org.apache.ignite.spark

import java.util

import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.{Partition, TaskContext}

import scala.annotation.varargs
import scala.collection.JavaConversions._
import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * Java-friendly Ignite RDD wrapper. Represents Ignite cache as Java Spark RDD abstraction.
 *
 * @param rdd Ignite RDD instance.
 * @tparam K Key type.
 * @tparam V Value type.
 */
class JavaIgniteRDD[K, V](override val rdd: IgniteRDD[K, V])
    extends JavaPairRDD[K, V](rdd)(JavaIgniteRDD.fakeClassTag, JavaIgniteRDD.fakeClassTag) {

    override def wrapRDD(rdd: RDD[(K, V)]): JavaPairRDD[K, V] = JavaPairRDD.fromRDD(rdd)

    override val classTag: ClassTag[(K, V)] = JavaIgniteRDD.fakeClassTag

    /**
     * Computes iterator based on given partition.
     *
     * @param part Partition to use.
     * @param context Task context.
     * @return Partition iterator.
     */
    def compute(part: Partition, context: TaskContext): Iterator[(K, V)] = {
        rdd.compute(part, context)
    }

    /**
     * Gets partitions for the given cache RDD.
     *
     * @return Partitions.
     */
    protected def getPartitions: java.util.List[Partition] = {
        new util.ArrayList[Partition](rdd.getPartitions.toSeq)
    }

    /**
     * Gets preferred locations for the given partition.
     *
     * @param split Split partition.
     * @return
     */
    protected def getPreferredLocations(split: Partition): Seq[String] = {
        rdd.getPreferredLocations(split)
    }

    @varargs def objectSql(typeName: String, sql: String, args: Any*): JavaPairRDD[K, V] =
        JavaPairRDD.fromRDD(rdd.objectSql(typeName, sql, args:_*))

    @varargs def sql(sql: String, args: Any*): DataFrame = rdd.sql(sql, args:_*)

    def saveValues(jrdd: JavaRDD[V]) = rdd.saveValues(JavaRDD.toRDD(jrdd))

    def saveValues[T](jrdd: JavaRDD[T], f: (T, IgniteContext) ⇒ V) = rdd.saveValues(JavaRDD.toRDD(jrdd), f)

    def savePairs(jrdd: JavaPairRDD[K, V], overwrite: Boolean) = {
        val rrdd: RDD[(K, V)] = JavaPairRDD.toRDD(jrdd)

        rdd.savePairs(rrdd, overwrite)
    }

    def savePairs(jrdd: JavaPairRDD[K, V]) : Unit = savePairs(jrdd, overwrite = false)

    def savePairs[T](jrdd: JavaRDD[T], f: (T, IgniteContext) ⇒ (K, V), overwrite: Boolean = false) = {
        rdd.savePairs(JavaRDD.toRDD(jrdd), f, overwrite)
    }

    def savePairs[T](jrdd: JavaRDD[T], f: (T, IgniteContext) ⇒ (K, V)): Unit =
        savePairs(jrdd, f, overwrite = false)

    def clear(): Unit = rdd.clear()

    def withKeepBinary[K1, V1](): JavaIgniteRDD[K1, V1] = new JavaIgniteRDD[K1, V1](rdd.withKeepBinary[K1, V1]())
}

object JavaIgniteRDD {
    implicit def fromIgniteRDD[K: ClassTag, V: ClassTag](rdd: IgniteRDD[K, V]): JavaIgniteRDD[K, V] =
        new JavaIgniteRDD[K, V](rdd)

    implicit def toIgniteRDD[K, V](rdd: JavaIgniteRDD[K, V]): IgniteRDD[K, V] = rdd.rdd

    def fakeClassTag[T]: ClassTag[T] = ClassTag.AnyRef.asInstanceOf[ClassTag[T]]
}
