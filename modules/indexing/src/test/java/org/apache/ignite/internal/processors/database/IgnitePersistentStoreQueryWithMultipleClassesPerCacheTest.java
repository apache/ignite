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

package org.apache.ignite.internal.processors.database;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK;

/**
 *
 */
@WithSystemProperty(key = IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK, value = "true")
public class IgnitePersistentStoreQueryWithMultipleClassesPerCacheTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "test_cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(cacheCfg(CACHE_NAME));

        DataStorageConfiguration pCfg = new DataStorageConfiguration();

        pCfg.setCheckpointFrequency(1000);

        cfg.setDataStorageConfiguration(pCfg);

        return cfg;
    }

    /**
     * @param name Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheCfg(String name) {
        CacheConfiguration<?, ?> cfg = new CacheConfiguration<>();

        cfg.setName(name);

        cfg.setRebalanceMode(CacheRebalanceMode.NONE);

        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        cfg.setIndexedTypes(
                Integer.class, Country.class,
                Integer.class, Person.class
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSimple() throws Exception {
        IgniteEx ig0 = startGrid(0);

        ig0.active(true);

        ig0.cache(CACHE_NAME).put(0, new Person(0));

        ig0.cache(CACHE_NAME).put(1, new Country());

        List<List<?>> all = ig0.cache(CACHE_NAME)
                .query(new SqlFieldsQuery("select depId FROM \"" + CACHE_NAME + "\".Person")).getAll();

        assert all.size() == 1;

    }

    /**
     *
     */
    public static class Person implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @QuerySqlField
        protected int depId;

        /** */
        @QuerySqlField
        protected String name;

        /** */
        @QuerySqlField
        protected UUID id = UUID.randomUUID();


        /** */
        @SuppressWarnings("unused")
        private Person() {
            // No-op.
        }

        /**
         * @param depId Department ID.
         */
        private Person(int depId) {
            this.depId = depId;

            name = "Name-" + id + " " + depId;
        }
    }

    /**
     *
     */
    public static class Country {
        /** */
        @QuerySqlField(index = true)
        private int id;

        /** */
        @QuerySqlField
        private String name;

        /** */
        @QuerySqlField
        private UUID uuid = UUID.randomUUID();
    }
}
