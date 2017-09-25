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

import java.util.{Map ⇒ JMap}

import org.apache.ignite.{Ignite, IgniteState, Ignition}
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalog.{Catalog, Column, Database, Function, Table}
import org.apache.spark.sql.catalyst.DefinedByConstructorParams
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.TypeTag
import IgniteCatalog._
import org.apache.ignite.spark.IgniteRelation

/**
  * Implementation of Catalog for Apache Ignite
  */
class IgniteCatalog(var ignite: Ignite, sparkSession: SparkSession) extends Catalog {
    override def currentDatabase: String = ignite.name

    override def listDatabases(): Dataset[Database] = {
        val databases = Ignition.allGrids().asScala.map(g ⇒ database(g.name()))

        IgniteCatalog.makeDataset(databases, sparkSession)
    }

    override def databaseExists(dbName: String): Boolean =
        IgniteCatalog.igniteExists(dbName)

    @throws[AnalysisException]("If database does not exist.")
    override def setCurrentDatabase(dbName: String): Unit = {
        ensureIgnite(dbName)

        ignite = Ignition.ignite(dbName)
    }

    @throws[AnalysisException]("If database does not exist.")
    override def getDatabase(dbName: String): Database = {
        ensureIgnite(dbName)

        database(dbName)
    }

    override def listTables: Dataset[Table] = listTables(currentDatabase)

    @throws[AnalysisException]("If database does not exist.")
    override def listTables(dbName: String): Dataset[Table] = {
        ensureIgnite(dbName)

        val ignite = Ignition.ignite(dbName)

        IgniteCatalog.makeDataset(ignite.cacheNames.asScala.flatten { name =>
            val cache = ignite.cache[Any, Any](name)

            val ccfg = cache.getConfiguration(classOf[CacheConfiguration[Any, Any]])

            ccfg.getQueryEntities.asScala.map { queryEntity ⇒
                new Table(
                    name = queryEntity.getTableName,
                    database = ignite.name,
                    description = queryEntity.getTableName,
                    tableType = "TABLE",
                    isTemporary = false)
            }
        }.toSeq, sparkSession)
    }

    @throws[AnalysisException]("If table does not exist.")
    override def getTable(tableName: String): Table = getTable(currentDatabase, tableName)


    @throws[AnalysisException]("If table or database does not exist.")
    override def getTable(dbName: String, tableName: String): Table = {
        ensureIgnite(dbName)

        val ignite = Ignition.ignite(dbName)

        val tableIdent = sparkSession.sessionState.sqlParser.parseTableIdentifier(tableName)

        if (tableExists(dbName, tableName)) {
            new Table(
                name = tableIdent.table,
                database = ignite.name,
                description = "Ignite cache",
                tableType = "TABLE",
                isTemporary = false)
        }
        else
            throw new AnalysisException(s"Table or view '$tableName' not found")
    }

    override def tableExists(tabName: String): Boolean = tableExists(currentDatabase, tabName)

    override def tableExists(dbName: String, tabName: String): Boolean =
        if (igniteExists(dbName)) {
            val ignite = Ignition.ignite(dbName)

            IgniteCatalog.tabExists(ignite, tabName)
        } else
            false

    override def createExternalTable(tableName: String, path: String): DataFrame = ???

    override def createExternalTable(tableName: String, path: String, source: String): DataFrame = ???

    override def createExternalTable(tableName: String, source: String, options: JMap[String, String]): DataFrame = ???

    override def createExternalTable(tableName: String, source: String, options: Map[String, String]): DataFrame = ???

    override def createExternalTable(tableName: String, source: String, schema: StructType,
        options: JMap[String, String]): DataFrame = ???

    override def createExternalTable(tableName: String, source: String,
        schema: StructType,
        options: Map[String, String]): DataFrame = ???

    override def dropTempView(viewName: String): Boolean = ???

    override def dropGlobalTempView(viewName: String): Boolean = ???

    override def listColumns(tableName: String): Dataset[Column] =
        listColumns(ignite.name, tableName)

    override def listColumns(dbName: String, tabName: String): Dataset[Column] = {
        val tableName = tabName.toUpperCase

        val ignite = Ignition.ignite(dbName)

        val cache = ignite.cache[Any, Any](IgniteRelation.cacheName(tableName))

        val ccfg = cache.getConfiguration(classOf[CacheConfiguration[Any, Any]])

        val table = ccfg.getQueryEntities.asScala
            .find(_.getTableName == tableName)
            .getOrElse(throw new AnalysisException(s"Table '$tableName' not found"))

        IgniteCatalog.makeDataset(table.getFields.asScala.toSeq map { case (name, dataType) ⇒
            new Column(
                name = name,
                description = null,
                dataType = dataType,
                nullable = true,
                isPartition = table.getKeyFields.contains(name),
                isBucket = false)
        }, sparkSession)
    }

    override def isCached(tableName: String): Boolean = false

    override def cacheTable(tableName: String): Unit = ???

    override def uncacheTable(tableName: String): Unit = ???

    override def clearCache(): Unit = {}

    override def refreshTable(tableName: String): Unit = {}

    override def refreshByPath(path: String): Unit = {}

    override def createTable(tableName: String, path: String): DataFrame = ???

    override def createTable(tableName: String, path: String, source: String): DataFrame = ???

    override def createTable(tableName: String, source: String, options: Map[String, String]): DataFrame = ???

    override def createTable(tableName: String, source: String, schema: StructType,
        options: Map[String, String]): DataFrame = ???

    override def recoverPartitions(tableName: String): Unit = {}

    override def listFunctions: Dataset[Function] =
        IgniteCatalog.makeDataset(Seq.empty[Function], sparkSession)

    override def listFunctions(dbName: String): Dataset[Function] = listFunctions

    @throws[AnalysisException]("If function does not exist.")
    override def getFunction(functionName: String): Function =
        throw new AnalysisException(s"Function '$functionName' does not exist.")

    @throws[AnalysisException]("If function or database does not exist.")
    override def getFunction(dbName: String, functionName: String): Function = {
        ensureIgnite(dbName)

        getFunction(functionName)
    }

    override def functionExists(functionName: String): Boolean = false

    @throws[AnalysisException]("If database does not exist.")
    override def functionExists(dbName: String, functionName: String): Boolean = {
        ensureIgnite(dbName)

        false
    }

    private def database(name: String) =
        new Database(
            name = name,
            description = "Apache Ignite Database Instance - " + name,
            locationUri = name)
}

object IgniteCatalog {
    def apply(i: Ignite, ss: SparkSession) = new IgniteCatalog(i, ss)

    def makeDataset[T <: DefinedByConstructorParams : TypeTag](
        data: Seq[T],
        sparkSession: SparkSession): Dataset[T] = {
        val enc = ExpressionEncoder[T]()

        val encoded = data.map(d => enc.toRow(d).copy())

        val plan = new LocalRelation(enc.schema.toAttributes, encoded)

        val queryExecution = sparkSession.sessionState.executePlan(plan)

        new Dataset[T](sparkSession, queryExecution, enc)
    }

    def ensureIgnite(gridName: String): Unit =
        if (!igniteExists(gridName))
            throw new AnalysisException(s"Ignite grid with name '$gridName' does not exist.")

    def igniteExists(gridName: String): Boolean =
        Ignition.state(gridName) == IgniteState.STARTED

    def tabExists(ignite: Ignite, tabName: String): Boolean = {
        val tableName = tabName.toUpperCase

        val cache = ignite.cache[Any, Any](IgniteRelation.cacheName(tableName))

        val ccfg = cache.getConfiguration(classOf[CacheConfiguration[Any, Any]])

        ccfg.getQueryEntities.asScala
            .exists(_.getTableName == tableName)
    }
}
