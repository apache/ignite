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

package org.apache.spark.sql.ignite

import java.{lang, util}

import org.apache.ignite.Ignite
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.internal.{SessionState, SessionStateBuilder, SharedState}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.streaming.{DataStreamReader, StreamingQueryManager}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.ExecutionListenerManager
import org.apache.spark.sql._

import scala.reflect.runtime.universe

/**
  */
class IgniteSparkSession(proxy: SparkSession, ignite: Ignite) extends SparkSession(proxy.sparkContext) with Logging {
    self ⇒

    experimental.extraStrategies = experimental.extraStrategies :+ IgniteStrategy
    experimental.extraOptimizations = experimental.extraOptimizations :+ IgniteOptimization

    proxy.experimental.extraStrategies = Seq(IgniteStrategy)

    override lazy val catalog = new IgniteCatalog(ignite, proxy)

    override val sqlContext: SQLContext = new SQLContext(self)

    override lazy val sharedState: SharedState =
        new IgniteSharedState(ignite, sparkContext)

    override lazy val sessionState: SessionState = {
        val sessionState = new SessionStateBuilder(self, None).build()

        sessionState.experimentalMethods.extraStrategies =
            sessionState.experimentalMethods.extraStrategies :+ IgniteStrategy
        sessionState.experimentalMethods.extraOptimizations =
            sessionState.experimentalMethods.extraOptimizations :+ IgniteOptimization

        sessionState
    }

    override lazy val conf: RuntimeConfig = proxy.conf

    override lazy val emptyDataFrame: DataFrame = proxy.emptyDataFrame

    override def newSession(): SparkSession = new IgniteSparkSession(proxy.newSession(), ignite)

    override def version: String = proxy.version

    override def listenerManager: ExecutionListenerManager = proxy.listenerManager

    override def experimental: ExperimentalMethods =
        proxy.experimental

    override def udf: UDFRegistration = proxy.udf

    override def streams: StreamingQueryManager = proxy.streams

    override def emptyDataset[T](implicit evidence$1: Encoder[T]): Dataset[T] = proxy.emptyDataset

    override def createDataFrame[A <: Product](rdd: RDD[A])(implicit
        evidence$2: universe.TypeTag[A]): DataFrame = proxy.createDataFrame(rdd)

    override def createDataFrame[A <: Product](data: Seq[A])(implicit
        evidence$3: universe.TypeTag[A]): DataFrame = proxy.createDataFrame(data)

    override def createDataFrame(rowRDD: RDD[Row],
        schema: StructType): DataFrame = proxy.createDataFrame(rowRDD, schema)

    override def createDataFrame(rowRDD: JavaRDD[Row],
        schema: StructType): DataFrame = proxy.createDataFrame(rowRDD, schema)

    override def createDataFrame(rows: util.List[Row],
        schema: StructType): DataFrame = proxy.createDataFrame(rows, schema)

    override def createDataFrame(rdd: RDD[_],
        beanClass: Class[_]): DataFrame = proxy.createDataFrame(rdd, beanClass)

    override def createDataFrame(rdd: JavaRDD[_],
        beanClass: Class[_]): DataFrame = proxy.createDataFrame(rdd, beanClass)

    override def createDataFrame(data: util.List[_],
        beanClass: Class[_]): DataFrame = proxy.createDataFrame(data, beanClass)

    override def baseRelationToDataFrame(
        baseRelation: BaseRelation): DataFrame = proxy.baseRelationToDataFrame(baseRelation)

    override def createDataset[T](data: Seq[T])(implicit
        evidence$4: Encoder[T]): Dataset[T] = proxy.createDataset(data)

    override def createDataset[T](data: RDD[T])(implicit
        evidence$5: Encoder[T]): Dataset[T] = proxy.createDataset(data)

    override def createDataset[T](data: util.List[T])(implicit
        evidence$6: Encoder[T]): Dataset[T] = proxy.createDataset(data)

    override def range(end: Long): Dataset[lang.Long] = proxy.range(end)

    override def range(start: Long,
        end: Long): Dataset[lang.Long] = proxy.range(start, end)

    override def range(start: Long, end: Long,
        step: Long): Dataset[lang.Long] = proxy.range(start, end, step)

    override def range(start: Long, end: Long, step: Long,
        numPartitions: Int): Dataset[lang.Long] = proxy.range(start, end, step, numPartitions)

    override def table(
        tableName: String): DataFrame = proxy.table(tableName)

    override def sql(sqlText: String): DataFrame =
        Dataset.ofRows(self, sessionState.sqlParser.parsePlan(sqlText))

    override def read: DataFrameReader = proxy.read

    override def readStream: DataStreamReader = proxy.readStream

    override def time[T](f: ⇒ T): T = proxy.time(f)

    override def stop(): Unit = proxy.stop()

    override def close(): Unit = proxy.close()

    override protected[sql] def parseDataType(
        dataTypeString: String): DataType = proxy.parseDataType(dataTypeString)

    override private[sql] def applySchemaToPythonRDD(rdd: RDD[Array[Any]],
        schemaString: String) = proxy.applySchemaToPythonRDD(rdd, schemaString)

    override private[sql] def applySchemaToPythonRDD(rdd: RDD[Array[Any]],
        schema: StructType) = proxy.applySchemaToPythonRDD(rdd, schema)

    override private[sql] def cloneSession() = new IgniteSparkSession(proxy.cloneSession(), ignite)

    override private[sql] val extensions = proxy.extensions

    override private[sql] def internalCreateDataFrame(
        catalystRows: RDD[InternalRow],
        schema: StructType) = proxy.internalCreateDataFrame(catalystRows, schema)

    override private[sql] def createDataFrame(rowRDD: RDD[Row],
        schema: StructType,
        needsConversion: Boolean) = proxy.createDataFrame(rowRDD, schema, needsConversion)

    override private[sql] def table(
        tableIdent: TableIdentifier) = proxy.table(tableIdent)
}

object IgniteSparkSession {
    def builder(): Builder = new Builder
}
