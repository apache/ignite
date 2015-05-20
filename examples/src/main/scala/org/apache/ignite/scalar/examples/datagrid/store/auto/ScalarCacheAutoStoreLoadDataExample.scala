/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ignite.scalar.examples.datagrid.store.auto

import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStore
import org.apache.ignite.examples.datagrid.store.auto.DbH2ServerStartup
import org.apache.ignite.examples.{ExampleNodeStartup, ExamplesUtils}
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

import java.lang.{Long => JavaLong}

/**
 * Demonstrates how to load data from database.
 * <p>
 * This example uses [[CacheJdbcPojoStore]] as a persistent store.
 * <p>
 * To start the example, you should:
 * <ul>
 *  <li>Start H2 database TCP server using [[DbH2ServerStartup]].</li>
 *  <li>Start a few nodes using [[ExampleNodeStartup]] or by starting remote nodes as specified below.</li>
 *  <li>Start example using [[ScalarCacheAutoStoreLoadDataExample]].</li>
 * </ul>
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p>
 * Alternatively you can run [[ExampleNodeStartup]] in another JVM which will
 * start node with `examples/config/example-ignite.xml` configuration.
 */
object ScalarCacheAutoStoreLoadDataExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    /** Cache name. */
    private val CACHE_NAME = ScalarCacheAutoStoreLoadDataExample.getClass.getSimpleName
    
    /** Heap size required to run this example. */
    val MIN_MEMORY = 1024 * 1024 * 1024
    
    ExamplesUtils.checkMinMemory(MIN_MEMORY)
    
    scalar(CONFIG) {
        println()
        println(">>> Cache auto store load data example started.")

        val cacheCfg = CacheConfig.jdbcPojoStoreCache(CACHE_NAME)

        val cache = createCache$(cacheCfg)

        try {
            cache.loadCache(null, "java.lang.Long", "select * from PERSON where id <= 3")

            println("Loaded cache entries: " + cache.size())

            cache.clear()

            cache.loadCache(null)

            println("Loaded cache entries: " + cache.size())
        }
        finally {
            if (cache != null)
                cache.close()
        }
    }
}
