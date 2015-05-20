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

import org.apache.ignite.cache.CacheAtomicityMode._
import org.apache.ignite.cache.store.CacheStore
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStore
import org.apache.ignite.cache.{CacheTypeFieldMetadata, CacheTypeMetadata}
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.scalar.examples.datagrid.store.Person

import org.h2.jdbcx.JdbcConnectionPool

import javax.cache.configuration.Factory
import java.lang.{Long => JavaLong}
import java.sql.Types
import java.util.{Arrays, Collections}

/**
 * Predefined configuration for examples with [[CacheJdbcPojoStore]].
 */
private[auto] object CacheConfig {
    /**
     * Configure cache with store.
     */
    def jdbcPojoStoreCache(name: String): CacheConfiguration[JavaLong, Person] = {
        val cfg = new CacheConfiguration[JavaLong, Person](name)

        cfg.setAtomicityMode(TRANSACTIONAL)

        cfg.setCacheStoreFactory(new Factory[CacheStore[_ >: JavaLong, _ >: Person]] {
            @impl def create: CacheStore[_ >: JavaLong, _ >: Person] = {
                val store = new CacheJdbcPojoStore[JavaLong, Person]

                store.setDataSource(JdbcConnectionPool.create("jdbc:h2:tcp://localhost/mem:ExampleDb", "sa", ""))

                store
            }
        })

        val meta = new CacheTypeMetadata

        meta.setDatabaseTable("PERSON")
        meta.setKeyType("java.lang.Long")
        meta.setValueType("org.apache.ignite.scalar.examples.datagrid.store.Person")
        meta.setKeyFields(Collections.singletonList(new CacheTypeFieldMetadata("ID", Types.BIGINT, "id", classOf[JavaLong])))
        meta.setValueFields(Arrays.asList(new CacheTypeFieldMetadata("ID", Types.BIGINT, "id", classOf[JavaLong]),
            new CacheTypeFieldMetadata("FIRST_NAME", Types.VARCHAR, "firstName", classOf[String]),
            new CacheTypeFieldMetadata("LAST_NAME", Types.VARCHAR, "lastName", classOf[String])))

        cfg.setTypeMetadata(Collections.singletonList(meta))
        cfg.setWriteBehindEnabled(true)
        cfg.setReadThrough(true)
        cfg.setWriteThrough(true)

        cfg
    }
}

