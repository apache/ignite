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

import org.apache.ignite.cache.QueryEntity
import org.apache.ignite.cache.affinity.Affinity
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.spark.IgniteRelationProvider._
import org.apache.ignite.spark.{IgniteContext, IgniteSQLRelation}
import org.apache.ignite.{Ignite, IgniteState, Ignition}
import org.apache.spark.sql.{AnalysisException, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.{DefinedByConstructorParams, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import scala.reflect.runtime.universe.TypeTag
import org.apache.commons.lang.StringUtils.equalsIgnoreCase
import org.apache.ignite.internal.util.lang.GridFunc.contains

import IgniteExternalCatalog._

/**
  */
class IgniteExternalCatalog(defaultIgniteContext: Option[IgniteContext] = None) extends ExternalCatalog {
    @transient private var default: Ignite = defaultIgniteContext.map(_.ignite()).getOrElse(Ignition.ignite)

    override def getDatabase(db: String): CatalogDatabase =
        database(igniteName(igniteOrDefault(db, default)))

    override def databaseExists(db: String): Boolean =
        db == SessionCatalog.DEFAULT_DATABASE || igniteExists(db)

    override def listDatabases(): Seq[String] =
        Ignition.allGrids().map(igniteName)

    override def listDatabases(pattern: String): Seq[String] =
        StringUtils.filterPattern(listDatabases(), pattern)

    override def setCurrentDatabase(db: String): Unit = {
        ensureIgnite(db)

        default = ignite(db)
    }

    override def getTable(db: String, table: String): CatalogTable = getTableOption(db, table).get

    override def getTableOption(db: String, tabName: String): Option[CatalogTable] = {
        val ignite = igniteOrDefault(db, default)

        val gridName = igniteName(ignite)

        igniteSQLTable(ignite, tabName) match {
            case Some(table) ⇒
                val tableName = table.getTableName

                Some(new CatalogTable(
                    identifier = new TableIdentifier(tableName, Some(gridName)),
                    tableType = CatalogTableType.EXTERNAL,
                    storage = CatalogStorageFormat(
                        locationUri = None,
                        inputFormat = Some(IGNITE),
                        outputFormat = Some(IGNITE),
                        serde = None,
                        compressed = false,
                        properties = Map(
                            GRID → gridName,
                            TABLE → tableName)
                    ),
                    schema = IgniteSQLRelation.schema(table),
                    provider = Some(IGNITE),
                    partitionColumnNames =
                        if (table.getKeyFields != null)
                            table.getKeyFields.toSeq
                        else
                            Seq(table.getKeyFieldName),
                    bucketSpec = None))
            case None ⇒ None
        }
    }

    override def tableExists(db: String, table: String): Boolean =
        sqlTableExists(igniteOrDefault(db, default), table)

    override def listTables(db: String): Seq[String] = listTables(db, ".*")

    override def listTables(db: String, pattern: String): Seq[String] = {
        val ignite = igniteOrDefault(db, default)

        ignite.cacheNames.flatten { name =>
            val cache = ignite.cache[Any, Any](name)

            val ccfg = cache.getConfiguration(classOf[CacheConfiguration[Any, Any]])

            ccfg.getQueryEntities.map(_.getTableName)
        }.toSeq
    }

    override def loadTable(db: String, table: String,
        loadPath: String, isOverwrite: Boolean, isSrcLocal: Boolean): Unit = { /* no-op */ }

    override def getPartition(db: String, table: String,
        spec: TablePartitionSpec): CatalogTablePartition = {
        ???
    }

    override def getPartitionOption(db: String, table: String,
        spec: TablePartitionSpec): Option[CatalogTablePartition] = ???

    override def listPartitionNames(db: String, table: String,
        partialSpec: Option[TablePartitionSpec]): Seq[String] = {
        val ignite = igniteOrDefault(db, default)

        if (sqlTableExists(ignite, table)) {
            val parts = ignite.affinity(IgniteSQLRelation.sqlCacheName(table)).partitions()

            (0 until parts).map(_.toString)
        }
        else
            Seq.empty
    }

    override def listPartitions(db: String, table: String,
        partialSpec: Option[TablePartitionSpec]): Seq[CatalogTablePartition] = {
        val ignite = igniteOrDefault(db, default)

        val partitionNames = listPartitionNames(db, table, partialSpec)

        if (partitionNames.isEmpty)
            Seq.empty
        else {
            val tableName = IgniteSQLRelation.sqlCacheName(table)

            val aff = ignite.affinity[Any](tableName)

            partitionNames.map {
                name ⇒ partition(aff, name.toInt, db, table)
            }
        }
    }

    override def listPartitionsByFilter(db: String, table: String,
        predicates: Seq[Expression],
        defaultTimeZoneId: String): Seq[CatalogTablePartition] = listPartitions(db, table, None)

    override def loadPartition(db: String, table: String,
        loadPath: String,
        partition: TablePartitionSpec, isOverwrite: Boolean,
        inheritTableSpecs: Boolean, isSrcLocal: Boolean): Unit = { /* no-op */ }

    override def loadDynamicPartitions(db: String, table: String,
        loadPath: String,
        partition: TablePartitionSpec, replace: Boolean,
        numDP: Int): Unit = { /* no-op */ }

    override def getFunction(db: String, funcName: String): CatalogFunction =
        throw new UnsupportedOperationException("unsupported")

    override def functionExists(db: String, funcName: String): Boolean = false

    override def listFunctions(db: String, pattern: String): Seq[String] = Seq.empty[String]

    override def alterDatabase(dbDefinition: CatalogDatabase): Unit =
        throw new UnsupportedOperationException("unsupported")

    override def alterTable(tableDefinition: CatalogTable): Unit =
        throw new UnsupportedOperationException("unsupported")

    override def alterTableSchema(db: String, table: String, schema: StructType): Unit =
        throw new UnsupportedOperationException("unsupported")

    override protected def doCreateFunction(db: String, funcDefinition: CatalogFunction): Unit = { /* no-op */ }

    override protected def doDropFunction(db: String, funcName: String): Unit = { /* no-op */ }

    override protected def doRenameFunction(db: String, oldName: String, newName: String): Unit = { /* no-op */ }

    override protected def doCreateDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit =
        throw new UnsupportedOperationException("unsupported")

    override protected def doDropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit =
        throw new UnsupportedOperationException("unsupported")

    override protected def doCreateTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit =
        throw new UnsupportedOperationException("unsupported")

    override protected def doDropTable(db: String, table: String, ignoreIfNotExists: Boolean, purge: Boolean): Unit =
        throw new UnsupportedOperationException("unsupported")

    override protected def doRenameTable(db: String, oldName: String, newName: String): Unit =
        throw new UnsupportedOperationException("unsupported")

    override def createPartitions(db: String, table: String,
        parts: Seq[CatalogTablePartition],
        ignoreIfExists: Boolean): Unit =
        throw new UnsupportedOperationException("unsupported")

    override def dropPartitions(db: String, table: String,
        parts: Seq[TablePartitionSpec],
        ignoreIfNotExists: Boolean, purge: Boolean, retainData: Boolean): Unit =
        throw new UnsupportedOperationException("unsupported")

    override def renamePartitions(db: String, table: String,
        specs: Seq[TablePartitionSpec],
        newSpecs: Seq[TablePartitionSpec]): Unit =
        throw new UnsupportedOperationException("unsupported")

    override def alterPartitions(db: String, table: String,
        parts: Seq[CatalogTablePartition]): Unit =
        throw new UnsupportedOperationException("unsupported")

    private def database(name: String): CatalogDatabase =
        CatalogDatabase(name, name, null, Map.empty)

    private def partition(aff: Affinity[Any], partition: Int, grid: String, table: String) = {
        val nodes = aff.mapPartitionToPrimaryAndBackups(partition)

        if (nodes.isEmpty)
            throw new AnalysisException(s"Nodes for parition is empty [grid=$grid,table=$table,partition=$partition].")

        CatalogTablePartition (
            Map (
                "name" → partition.toString,
                GRID → grid,
                "primary" → nodes.head.id.toString,
                "backups" → nodes.tail.map(_.id.toString).mkString(",")
            ),
            CatalogStorageFormat.empty
        )
    }
}

object IgniteExternalCatalog {
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
        if (gridName == "")
            Ignition.state() == IgniteState.STARTED
        else
            Ignition.state(gridName) == IgniteState.STARTED

    def igniteName(g: Ignite): String =
        if(g.name() != null)
            g.name
        else
            ""

    def igniteOrDefault(name: String, default: Ignite): Ignite =
        if (name == SessionCatalog.DEFAULT_DATABASE) {
            if (igniteExists(name))
                ignite(name)
            else
                default
        }
        else
            ignite(name)

    def ignite(gridName: String): Ignite =
        if (gridName == "")
            Ignition.ignite()
        else
            Ignition.ignite(gridName)

    def sqlTableExists(ignite: Ignite, tabName: String): Boolean =
        igniteSQLTable(ignite, tabName).isDefined

    def cacheExists(ignite: Ignite, tabName: String): Boolean =
        igniteCache(ignite, tabName).isDefined

    def igniteSQLTable(ignite: Ignite, tabName: String): Option[QueryEntity] = {
        val tableName = tabName.toUpperCase

        val cacheName = IgniteSQLRelation.sqlCacheName(tableName)

        if (!ignite.cacheNames.contains(cacheName))
            None
        else {
            val cache = ignite.cache[Any, Any](cacheName)

            val ccfg = cache.getConfiguration(classOf[CacheConfiguration[Any, Any]])

            ccfg.getQueryEntities.find(_.getTableName == tableName)
        }
    }

    def igniteCache[K, V](ignite: Ignite, tabName: String): Option[CacheConfiguration[K, V]] =
        ignite.cacheNames().find(_.equalsIgnoreCase(tabName)).map { cacheName ⇒
            ignite.cache[K, V](cacheName).getConfiguration(classOf[CacheConfiguration[K, V]])
        }

    /**
      * @param table Table.
      * @param column Column name.
      * @return `True` if column is key.
      */
    def isKeyColumn(table: QueryEntity, column: String): Boolean =
        contains(table.getKeyFields, column) || equalsIgnoreCase(table.getKeyFieldName, column)
}
