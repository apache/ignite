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

import org.apache.commons.lang.StringUtils.equalsIgnoreCase
import org.apache.ignite.{Ignite, IgniteException, IgniteState, Ignition}
import org.apache.ignite.cache.QueryEntity
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.internal.util.lang.GridFunc.contains
import org.apache.spark.sql.catalyst.catalog.SessionCatalog

import scala.collection.JavaConversions._

package object impl {
    /**
      * Checks named instance of Ignite exists.
      * Throws IgniteException if not.
      *
      * @param gridName Name of grid.
      */
    def ensureIgnite(gridName: String): Unit =
        if (!igniteExists(gridName))
            throw new IgniteException(s"Ignite grid with name '$gridName' does not exist.")

    /**
      * @param gridName Name of grid.
      * @return True if named instance of Ignite exists false otherwise.
      */
    def igniteExists(gridName: String): Boolean =
        if (gridName == "")
            Ignition.state() == IgniteState.STARTED
        else
            Ignition.state(gridName) == IgniteState.STARTED

    /**
      * @param g Ignite.
      * @return Name of Ignite. If name is null empty string returned.
      */
    def igniteName(g: Ignite): String =
        if(g.name() != null)
            g.name
        else
            ""

    /**
      * @param name Name of grid..
      * @param default Default instance.
      * @return Named grid instance if it exists. If not default instance returned.
      */
    def igniteOrDefault(name: String, default: Ignite): Ignite =
        if (name == SessionCatalog.DEFAULT_DATABASE) {
            if (igniteExists(name))
                ignite(name)
            else
                default
        }
        else
            ignite(name)

    /**
      * @param gridName Name of grid.
      * @return Named instance of grid. If 'gridName' is empty unnamed instance returned.
      */
    def ignite(gridName: String): Ignite =
        if (gridName == "")
            Ignition.ignite()
        else
            Ignition.ignite(gridName)

    /**
      * @param ignite Ignite instance.
      * @param tabName Table name.
      * @return True if table exists false otherwise.
      */
    def sqlTableExists(ignite: Ignite, tabName: String): Boolean =
        sqlTableInfo(ignite, tabName).isDefined

    /**
      * @param ignite Ignite instance.
      * @param cacheName Cache name.
      * @return True if cache exists false otherwise.
      */
    def cacheExists(ignite: Ignite, cacheName: String): Boolean =
        ignite.cacheNames().contains(cacheName)

    /**
      * @param ignite Ignite instance.
      * @param tabName Table name.
      * @return QueryEntity for a given table.
      */
    def igniteSQLTable(ignite: Ignite, tabName: String): Option[QueryEntity] =
        sqlTableInfo[Any, Any](ignite, tabName).map(_._2)

    /**
      * @param ignite Ignite instance.
      * @param tabName Table name.
      * @return Cache name for given table.
      */
    def sqlCacheName(ignite: Ignite, tabName: String): Option[String] =
        sqlTableInfo[Any, Any](ignite, tabName).map(_._1.getName)

    /**
      * @param ignite Ignite instance.
      * @param tabName Table name.
      * @tparam K Key class.
      * @tparam V Value class.
      * @return CacheConfiguration and QueryEntity for a given table.
      */
    private def sqlTableInfo[K, V](ignite: Ignite, tabName: String): Option[(CacheConfiguration[K, V], QueryEntity)] =
        ignite.cacheNames().map { cacheName ⇒
            val ccfg = ignite.cache[K, V](cacheName).getConfiguration(classOf[CacheConfiguration[K, V]])

            val queryEntities = ccfg.getQueryEntities

            queryEntities.find(_.getTableName.equalsIgnoreCase(tabName)).map(qe ⇒ (ccfg, qe))
        }.find(_.isDefined).flatten

    /**
      * @param table Table.
      * @param column Column name.
      * @return `True` if column is key.
      */
    def isKeyColumn(table: QueryEntity, column: String): Boolean =
        contains(table.getKeyFields, column) || equalsIgnoreCase(table.getKeyFieldName, column)

    /**
      * Enclose some closure, so it doesn't on outer object(default scala behaviour) while serializing.
      */
    def enclose[E, R](enclosed: E)(func: E => R): R = func(enclosed)

}
