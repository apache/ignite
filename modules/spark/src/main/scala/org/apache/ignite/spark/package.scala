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

package org.apache.ignite

import scala.collection.JavaConversions._
import org.apache.commons.lang.StringUtils.equalsIgnoreCase
import org.apache.ignite.cache.QueryEntity
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.internal.util.lang.GridFunc.contains
import org.apache.spark.sql.catalyst.catalog.SessionCatalog

package object spark {
    def ensureIgnite(gridName: String): Unit =
        if (!igniteExists(gridName))
            throw new IgniteException(s"Ignite grid with name '$gridName' does not exist.")

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

    /**
      * Enclose some closure, so it doesn't on outer object(default scala behaviour) while serializing.
      */
    def enclose[E, R](enclosed: E)(func: E => R): R = func(enclosed)

}
