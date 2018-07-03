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

import org.apache.ignite.IgniteException
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.internal.IgnitionEx
import org.apache.ignite.spark.IgniteContext

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Range}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal._
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

/**
  * Implementation of Spark Session for Ignite.
  */
class IgniteSparkSession private(ic: IgniteContext, proxy: SparkSession) extends SparkSession(proxy.sparkContext) {
    self ⇒

    private def this(proxy: SparkSession) =
        this(new IgniteContext(proxy.sparkContext, IgnitionEx.DFLT_CFG), proxy)

    private def this(proxy: SparkSession, configPath: String) =
        this(new IgniteContext(proxy.sparkContext, configPath), proxy)

    private def this(proxy: SparkSession, cfgF: () => IgniteConfiguration) =
        this(new IgniteContext(proxy.sparkContext, cfgF), proxy)

    /** @inheritdoc */
    @transient override lazy val catalog = new CatalogImpl(self)

    /** @inheritdoc */
    @transient override val sqlContext: SQLContext = new SQLContext(self)

    /** @inheritdoc */
    @transient override lazy val sharedState: SharedState =
        new IgniteSharedState(ic, sparkContext)

    /** @inheritdoc */
    @transient override lazy val sessionState: SessionState = {
        val sessionState = new SessionStateBuilder(self, None).build()

        sessionState.experimentalMethods.extraOptimizations =
            sessionState.experimentalMethods.extraOptimizations :+ IgniteOptimization

        sessionState
    }

    /** @inheritdoc */
    @transient override lazy val conf: RuntimeConfig = proxy.conf

    /** @inheritdoc */
    @transient override lazy val emptyDataFrame: DataFrame = proxy.emptyDataFrame

    /** @inheritdoc */
    override def newSession(): SparkSession = new IgniteSparkSession(ic, proxy.newSession())

    /** @inheritdoc */
    override def version: String = proxy.version

    /** @inheritdoc */
    override def emptyDataset[T: Encoder]: Dataset[T] = {
        val encoder = implicitly[Encoder[T]]
        new Dataset(self, LocalRelation(encoder.schema.toAttributes), encoder)
    }

    /** @inheritdoc */
    override def createDataFrame(rows: java.util.List[Row], schema: StructType): DataFrame = {
        Dataset.ofRows(self, LocalRelation.fromExternalRows(schema.toAttributes, rows.asScala))
    }

    /** @inheritdoc */
    override def createDataFrame(rdd: RDD[_], beanClass: Class[_]): DataFrame = {
        val attributeSeq: Seq[AttributeReference] = getSchema(beanClass)
        val className = beanClass.getName
        val rowRdd = rdd.mapPartitions { iter =>
            SQLContext.beansToRows(iter, Utils.classForName(className), attributeSeq)
        }
        Dataset.ofRows(self, LogicalRDD(attributeSeq, rowRdd)(self))
    }

    /** @inheritdoc */
    override def createDataFrame(data: java.util.List[_], beanClass: Class[_]): DataFrame = {
        val attrSeq = getSchema(beanClass)
        val rows = SQLContext.beansToRows(data.asScala.iterator, beanClass, attrSeq)
        Dataset.ofRows(self, LocalRelation(attrSeq, rows.toSeq))
    }

    /** @inheritdoc */
    override def createDataFrame[A <: Product : TypeTag](rdd: RDD[A]): DataFrame = {
        SparkSession.setActiveSession(this)
        val encoder = Encoders.product[A]
        Dataset.ofRows(self, ExternalRDD(rdd, self)(encoder))
    }

    /** @inheritdoc */
    override def baseRelationToDataFrame(baseRelation: BaseRelation): DataFrame = {
        Dataset.ofRows(self, LogicalRelation(baseRelation))
    }

    /** @inheritdoc */
    override def createDataset[T: Encoder](data: Seq[T]): Dataset[T] = {
        val enc = encoderFor[T]
        val attributes = enc.schema.toAttributes
        val encoded = data.map(d => enc.toRow(d).copy())
        val plan = new LocalRelation(attributes, encoded)
        Dataset[T](self, plan)
    }

    /** @inheritdoc */
    override def createDataset[T: Encoder](data: RDD[T]): Dataset[T] = {
        Dataset[T](self, ExternalRDD(data, self))
    }

    /** @inheritdoc */
    override def range(start: Long, end: Long, step: Long, numPartitions: Int): Dataset[java.lang.Long] = {
        new Dataset(self, Range(start, end, step, numPartitions), Encoders.LONG)
    }

    /** @inheritdoc */
    override def table(tableName: String): DataFrame = {
        val tableIdent = sessionState.sqlParser.parseTableIdentifier(tableName)

        Dataset.ofRows(self, sessionState.catalog.lookupRelation(tableIdent))
    }

    /** @inheritdoc */
    override def sql(sqlText: String): DataFrame = Dataset.ofRows(self, sessionState.sqlParser.parsePlan(sqlText))

    /** @inheritdoc */
    override def read: DataFrameReader = new DataFrameReader(self)

    /** @inheritdoc */
    override def readStream: DataStreamReader = new DataStreamReader(self)

    /** @inheritdoc */
    override def stop(): Unit = proxy.stop()

    /** @inheritdoc */
    override private[sql] def applySchemaToPythonRDD(rdd: RDD[Array[Any]], schema: StructType) = {
        val rowRdd = rdd.map(r => python.EvaluatePython.makeFromJava(schema).asInstanceOf[InternalRow])
        Dataset.ofRows(self, LogicalRDD(schema.toAttributes, rowRdd)(self))
    }

    /** @inheritdoc */
    override private[sql] def cloneSession() = new IgniteSparkSession(ic, proxy.cloneSession())

    /** @inheritdoc */
    @transient override private[sql] val extensions =
        proxy.extensions

    /** @inheritdoc */
    override private[sql] def createDataFrame(rowRDD: RDD[Row],
        schema: StructType,
        needsConversion: Boolean) = {
        val catalystRows = if (needsConversion) {
            val encoder = RowEncoder(schema)
            rowRDD.map(encoder.toRow)
        } else {
            rowRDD.map{r: Row => InternalRow.fromSeq(r.toSeq)}
        }
        val logicalPlan = LogicalRDD(schema.toAttributes, catalystRows)(self)
        Dataset.ofRows(self, logicalPlan)
    }

    /** @inheritdoc */
    override private[sql] def table( tableIdent: TableIdentifier) =
        Dataset.ofRows(self, sessionState.catalog.lookupRelation(tableIdent))

    private def getSchema(beanClass: Class[_]): Seq[AttributeReference] = {
        val (dataType, _) = JavaTypeInference.inferDataType(beanClass)
        dataType.asInstanceOf[StructType].fields.map { f =>
            AttributeReference(f.name, f.dataType, f.nullable)()
        }
    }
}

object IgniteSparkSession {
    /**
      * @return New instance of <code>IgniteBuilder</code>
      */
    def builder(): IgniteBuilder = {
        new IgniteBuilder
    }

    /**
      * Builder for <code>IgniteSparkSession</code>.
      * Extends spark session builder with methods related to Ignite configuration.
      */
    class IgniteBuilder extends Builder {
        /**
          * Config provider.
          */
        private var cfgF: () ⇒ IgniteConfiguration = _

        /**
          * Path to config file.
          */
        private var config: String = _

        /** @inheritdoc */
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

        /**
          * Set path to Ignite config file.
          * User should use only one of <code>igniteConfig</code> and <code>igniteConfigProvider</code>.
          *
          * @param cfg Path to Ignite config file.
          * @return This for chaining.
          */
        def igniteConfig(cfg: String): IgniteBuilder = {
            if (cfgF != null)
                throw new IgniteException("only one of config or configProvider should be provided")

            this.config = cfg

            this
        }

        /**
          * Set Ignite config provider.
          * User should use only one of <code>igniteConfig</code> and <code>igniteConfigProvider</code>.
          *
          * @param cfgF Closure to provide <code>IgniteConfiguration</code>.
          * @return This for chaining.
          */
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
