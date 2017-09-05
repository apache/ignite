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

import org.apache.ignite.IgniteException
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.internal.IgnitionEx
import org.apache.ignite.spark.IgniteContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.internal.{CatalogImpl, SessionState, SessionStateBuilder, SharedState}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.streaming.{DataStreamReader, StreamingQueryManager}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.ExecutionListenerManager
import org.apache.spark.SparkConf

import scala.reflect.runtime.universe

/**
  * Implementation of Spark Session for Ignite.
  */
class IgniteSparkSession private(ic: IgniteContext, proxy: SparkSession) extends SparkSession(proxy.sparkContext)  {
    self ⇒

    def this(proxy: SparkSession) =
        this(new IgniteContext(proxy.sparkContext, IgnitionEx.DFLT_CFG), proxy)

    def this(proxy: SparkSession, configPath: String) =
        this(new IgniteContext(proxy.sparkContext, configPath), proxy)

    def this(proxy: SparkSession, cfgF: () => IgniteConfiguration) =
        this(new IgniteContext(proxy.sparkContext, cfgF), proxy)

    @transient override lazy val catalog = new CatalogImpl(self)

    @transient override val sqlContext: SQLContext = new SQLContext(self)

    @transient override lazy val sharedState: SharedState =
        new IgniteSharedState(ic, sparkContext)

    @transient override lazy val sessionState: SessionState =
        new SessionStateBuilder(self, None).build()

    @transient override lazy val conf: RuntimeConfig = proxy.conf

    @transient override lazy val emptyDataFrame: DataFrame = proxy.emptyDataFrame

    override def newSession(): SparkSession = new IgniteSparkSession(ic, proxy.newSession())

    override def version: String = proxy.version

    override def listenerManager: ExecutionListenerManager = proxy.listenerManager

    override def experimental: ExperimentalMethods = proxy.experimental

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

    override private[sql] def cloneSession() = new IgniteSparkSession(ic, proxy.cloneSession())

    @transient override private[sql] val extensions = proxy.extensions

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
    def builder(): IgniteBuilder = {
        new IgniteBuilder
    }

    class IgniteBuilder extends Builder {
        private var cfgF: () ⇒ IgniteConfiguration = _
        private var config: String = _

        override def getOrCreate(): IgniteSparkSession = synchronized {
            val sparkSession = super.getOrCreate()

            val ic = if (cfgF != null)
                new IgniteContext(sparkSession.sparkContext, cfgF)
            else if (config != null)
                new IgniteContext(sparkSession.sparkContext, config)
            else {
                logWarning("No `igniteConfig` or `igniteConfigProvider`. " +
                    "IgniteSparkSession will use DFLT_CFG for Ignite.")
                new IgniteContext(sparkSession.sparkContext)
            }

            new IgniteSparkSession(ic, sparkSession)
        }

        def igniteConfig(cfg: String): IgniteBuilder = {
            if (cfgF != null)
                throw new IgniteException("only one of config or configProvider should be provided")

            this.config = cfg

            this
        }

        def igniteConfigProvider(cfgF: () ⇒ IgniteConfiguration): IgniteBuilder = {
            if (config != null)
                throw new IgniteException("only one of config or configProvider should be provided")

            this.cfgF = cfgF

            this
        }

        /** @inheritdoc */
        override def appName(name: String): IgniteBuilder = {
            super.appName(name)

            this
        }

        /** @inheritdoc */
        override def config(key: String, value: String): IgniteBuilder = {
            super.config(key, value)

            this
        }

        /** @inheritdoc */
        override def config(key: String, value: Long): IgniteBuilder = {
            super.config(key, value)

            this
        }

        /** @inheritdoc */
        override def config(key: String, value: Double): IgniteBuilder = {
            super.config(key, value)

            this
        }

        /** @inheritdoc */
        override def config(key: String, value: Boolean): IgniteBuilder = {
            super.config(key, value)

            this
        }

        /** @inheritdoc */
        override def config(conf: SparkConf): IgniteBuilder = {
            super.config(conf)

            this
        }

        /** @inheritdoc */
        override def master(master: String): IgniteBuilder = {
            super.master(master)

            this
        }

        /**
          * This method will throw RuntimeException as long as we building '''IgniteSparkSession'''
          */
        override def enableHiveSupport(): IgniteBuilder =
            throw new IgniteException("This method doesn't supported by IgniteSparkSession")

        /** @inheritdoc */
        override def withExtensions(f: (SparkSessionExtensions) ⇒ Unit): IgniteBuilder = {
            super.withExtensions(f)
            this
        }
    }
}
