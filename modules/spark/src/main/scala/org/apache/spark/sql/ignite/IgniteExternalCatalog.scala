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

import org.apache.ignite.{Ignite, Ignition}
import org.apache.ignite.cache.QueryEntity
import org.apache.ignite.cache.affinity.Affinity
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.spark.IgniteRelation
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._


/**
  */
class IgniteExternalCatalog(defaultIgnite: Option[Ignite] = None) extends ExternalCatalog {
    private var default: Ignite = defaultIgnite.getOrElse(Ignition.ignite)

    override def getDatabase(db: String): CatalogDatabase =
        database(IgniteExternalCatalog.getOrDefault(db, default).name)

    override def databaseExists(db: String): Boolean = IgniteCatalog.igniteExists(db)

    override def listDatabases(): Seq[String] =
        Ignition.allGrids().asScala.map(_.name)

    override def listDatabases(pattern: String): Seq[String] =
        StringUtils.filterPattern(listDatabases(), pattern)

    override def setCurrentDatabase(db: String): Unit = {
        IgniteCatalog.ensureIgnite(db)

        default = Ignition.ignite(db)
    }

    override def getTable(db: String, table: String): CatalogTable = getTableOption(db, table).get

    override def getTableOption(db: String, tab: String): Option[CatalogTable] = {
        val ignite = IgniteExternalCatalog.getOrDefault(db, default)

        val table = tab.toUpperCase

        if (IgniteCatalog.tabExists(ignite, table)) {
            val tableName = table.toUpperCase

            val cache = ignite.cache[Any, Any](IgniteRelation.cacheName(tableName))

            val ccfg = cache.getConfiguration(classOf[CacheConfiguration[Any, Any]])

            ccfg.getQueryEntities.asScala
                .find(_.getTableName == tableName).map(qe ⇒ catalogTable(qe, ignite.name()))
        } else
            None
    }

    override def tableExists(db: String, table: String): Boolean = {
        val ignite = IgniteExternalCatalog.getOrDefault(db, default)

        val tableName = table

        val cache = ignite.cache[Any, Any](IgniteRelation.cacheName(tableName))

        val ccfg = cache.getConfiguration(classOf[CacheConfiguration[Any, Any]])

        ccfg.getQueryEntities.asScala
            .exists(_.getTableName == tableName)
    }

    override def listTables(db: String): Seq[String] = listTables(db, ".*")

    override def listTables(db: String, pattern: String): Seq[String] = {
        val ignite = IgniteExternalCatalog.getOrDefault(db, default)

        ignite.cacheNames.asScala.flatten { name =>
            val cache = ignite.cache[Any, Any](name)

            val ccfg = cache.getConfiguration(classOf[CacheConfiguration[Any, Any]])

            ccfg.getQueryEntities.asScala.map(_.getTableName)
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
        val ignite = IgniteExternalCatalog.getOrDefault(db, default)

        if (IgniteCatalog.tabExists(ignite, table)) {
            val parts = ignite.affinity(IgniteRelation.cacheName(table)).partitions()

            (0 until parts).map(_.toString)
        }
        else
            Seq.empty
    }

    override def listPartitions(db: String, table: String,
        partialSpec: Option[TablePartitionSpec]): Seq[CatalogTablePartition] = {
        val ignite = IgniteExternalCatalog.getOrDefault(db, default)

        val aff = ignite.affinity[Any](IgniteRelation.cacheName(table))

        val partitionNames = listPartitionNames(db, table, partialSpec)

        partitionNames.map {
            name ⇒ partition(aff, name.toInt, db, table)
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

    override def getFunction(db: String, funcName: String): CatalogFunction = ???

    override def functionExists(db: String, funcName: String): Boolean = false

    override def listFunctions(db: String, pattern: String): Seq[String] = Seq.empty[String]

    override def alterDatabase(dbDefinition: CatalogDatabase): Unit = ???

    override def alterTable(tableDefinition: CatalogTable): Unit = ???

    override def alterTableSchema(db: String, table: String, schema: StructType): Unit = ???

    override protected def doCreateFunction(db: String, funcDefinition: CatalogFunction): Unit = { /* no-op */ }

    override protected def doDropFunction(db: String, funcName: String): Unit = { /* no-op */ }

    override protected def doRenameFunction(db: String, oldName: String, newName: String): Unit = { /* no-op */ }

    override protected def doCreateDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = ???

    override protected def doDropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = ???

    override protected def doCreateTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit = ???

    override protected def doDropTable(db: String, table: String, ignoreIfNotExists: Boolean, purge: Boolean): Unit = ???

    override protected def doRenameTable(db: String, oldName: String, newName: String): Unit = ???

    override def createPartitions(db: String, table: String,
        parts: Seq[CatalogTablePartition],
        ignoreIfExists: Boolean): Unit = ???

    override def dropPartitions(db: String, table: String,
        parts: Seq[TablePartitionSpec],
        ignoreIfNotExists: Boolean, purge: Boolean, retainData: Boolean): Unit = ???

    override def renamePartitions(db: String, table: String,
        specs: Seq[TablePartitionSpec],
        newSpecs: Seq[TablePartitionSpec]): Unit = ???

    override def alterPartitions(db: String, table: String,
        parts: Seq[CatalogTablePartition]): Unit = ???

    private def database(name: String): CatalogDatabase =
        CatalogDatabase(name, name, null, Map.empty)

    private def catalogTable(table: QueryEntity, gridName: String): CatalogTable =
        new CatalogTable(
            identifier = new TableIdentifier(table.getTableName, Some(gridName)),
            tableType = CatalogTableType.EXTERNAL,
            storage = CatalogStorageFormat(
                locationUri = None,
                inputFormat = None,
                outputFormat = None,
                serde = None,
                compressed = false,
                properties = Map("grid" → gridName, "table" → table.getTableName)
            ),
            schema = IgniteRelation.schema(table),
            provider = Some("ignite"),
            partitionColumnNames = table.getKeyFields.asScala.toList.sorted,
            bucketSpec = None //TODO
        )

    private def partition(aff: Affinity[Any], parition: Int, grid: String, table: String) = {
        val nodes = aff.mapPartitionToPrimaryAndBackups(parition).asScala

        if (nodes.isEmpty)
            throw new AnalysisException(s"Nodes for parition is empty [grid=$grid,table=$table,parition=$parition].")

        CatalogTablePartition (
            Map (
                "name" → parition.toString,
                "grid" → grid,
                "primary" → nodes.head.id.toString,
                "backups" → nodes.tail.map(_.id.toString).mkString(",")
            ),
            CatalogStorageFormat.empty
        )
    }
}

object IgniteExternalCatalog {
    def getOrDefault(name: String, default: Ignite): Ignite =
        if (name == SessionCatalog.DEFAULT_DATABASE) {
            if (IgniteCatalog.igniteExists(name))
                Ignition.ignite(name)
            else
                default
        }
        else
            Ignition.ignite(name)
}
