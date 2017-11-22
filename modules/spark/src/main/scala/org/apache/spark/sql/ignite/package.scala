package org.apache.spark.sql

import org.apache.spark.sql.catalyst.DefinedByConstructorParams

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.TypeTag
import org.apache.commons.lang.StringUtils.equalsIgnoreCase
import org.apache.ignite.cache.QueryEntity
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.{Ignite, IgniteState, Ignition}
import org.apache.ignite.internal.util.lang.GridFunc.contains
import org.apache.ignite.spark.IgniteSQLRelation
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation

/**
  * @author NIzhikov
  */
package object ignite {
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
