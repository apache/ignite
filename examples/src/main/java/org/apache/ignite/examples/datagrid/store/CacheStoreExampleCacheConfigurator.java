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

package org.apache.ignite.examples.datagrid.store;

import org.apache.ignite.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.examples.datagrid.store.dummy.*;
import org.apache.ignite.examples.datagrid.store.hibernate.*;
import org.apache.ignite.examples.datagrid.store.jdbc.*;

import javax.cache.configuration.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;

/**
 * Starts up an empty node with example cache and store configuration.
 */
public class CacheStoreExampleCacheConfigurator {
    /** Use org.apache.ignite.examples.datagrid.store.dummy.CacheDummyPersonStore to run example. */
    public static final String DUMMY = "DUMMY";

    /** Use org.apache.ignite.examples.datagrid.store.jdbc.CacheJdbcPersonStore to run example. */
    public static final String SIMPLE_JDBC = "SIMPLE_JDBC";

    /** Use org.apache.ignite.examples.datagrid.store.hibernate.CacheHibernatePersonStore to run example. */
    public static final String HIBERNATE = "HIBERNATE";

    /** Store to use. */
    public static final String STORE = DUMMY;

    /**
     * Configure ignite.
     *
     * @return Ignite configuration.
     * @throws IgniteException If failed.
     */
    public static CacheConfiguration<Long, Person> cacheConfiguration() throws IgniteException {
        CacheConfiguration<Long, Person> cacheCfg = new CacheConfiguration<>();

        // Set atomicity as transaction, since we are showing transactions in example.
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        cacheCfg.setCacheStoreFactory(new Factory<CacheStore<? super Long, ? super Person>>() {
            @Override public CacheStore<? super Long, ? super Person> create() {
                CacheStore<Long, Person> store;

                switch (STORE) {
                    case DUMMY:
                        store = new CacheDummyPersonStore();
                        break;

                    case SIMPLE_JDBC:
                        store = new CacheJdbcPersonStore();
                        break;

                    case HIBERNATE:
                        store = new CacheHibernatePersonStore();
                        break;

                    default:
                        throw new IllegalStateException("Unexpected store configured: " + STORE);
                }

                return store;
            }
        });

        cacheCfg.setReadThrough(true);
        cacheCfg.setWriteThrough(true);

        return cacheCfg;
    }
}
