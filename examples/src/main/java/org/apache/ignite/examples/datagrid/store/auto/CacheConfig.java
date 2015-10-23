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

package org.apache.ignite.examples.datagrid.store.auto;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import javax.cache.configuration.Factory;
import org.apache.ignite.cache.CacheTypeFieldMetadata;
import org.apache.ignite.cache.CacheTypeMetadata;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.datagrid.store.Person;
import org.h2.jdbcx.JdbcConnectionPool;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Predefined configuration for examples with {@link CacheJdbcPojoStore}.
 */
public class CacheConfig {
    /**
     * Configure cache with store.
     */
    public static CacheConfiguration<Long, Person> jdbcPojoStoreCache(String name) {
        CacheConfiguration<Long, Person> cfg = new CacheConfiguration<>(name);

        // Set atomicity as transaction, since we are showing transactions in the example.
        cfg.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheStoreFactory(new Factory<CacheStore<? super Long, ? super Person>>() {
            @Override public CacheStore<? super Long, ? super Person> create() {
                CacheJdbcPojoStore<Long, Person> store = new CacheJdbcPojoStore<>();

                store.setDataSource(JdbcConnectionPool.create("jdbc:h2:tcp://localhost/mem:ExampleDb", "sa", ""));

                return store;
            }
        });

        CacheTypeMetadata meta = new CacheTypeMetadata();

        meta.setDatabaseTable("PERSON");

        meta.setKeyType("java.lang.Long");
        meta.setValueType("org.apache.ignite.examples.datagrid.store.Person");

        meta.setKeyFields(Collections.singletonList(new CacheTypeFieldMetadata("ID", Types.BIGINT, "id", Long.class)));

        meta.setValueFields(Arrays.asList(
            new CacheTypeFieldMetadata("ID", Types.BIGINT, "id", long.class),
            new CacheTypeFieldMetadata("FIRST_NAME", Types.VARCHAR, "firstName", String.class),
            new CacheTypeFieldMetadata("LAST_NAME", Types.VARCHAR, "lastName", String.class)
        ));

        cfg.setTypeMetadata(Collections.singletonList(meta));

        cfg.setWriteBehindEnabled(true);

        cfg.setReadThrough(true);
        cfg.setWriteThrough(true);

        return cfg;
    }
}