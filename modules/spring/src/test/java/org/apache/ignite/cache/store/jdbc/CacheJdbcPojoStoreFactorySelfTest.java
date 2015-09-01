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

package org.apache.ignite.cache.store.jdbc;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.store.jdbc.dialect.H2Dialect;
import org.apache.ignite.cache.store.jdbc.dialect.JdbcDialect;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.h2.jdbcx.JdbcDataSource;

/**
 * Test for Cache jdbc blob store factory.
 */
public class CacheJdbcPojoStoreFactorySelfTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "test";

    /**
     * @throws Exception If failed.
     */
    public void testCacheConfiguration() throws Exception {
        try (Ignite ignite = Ignition.start("modules/spring/src/test/config/node.xml")) {
            try (Ignite ignite1 = Ignition.start("modules/spring/src/test/config/node1.xml")) {
                try (IgniteCache<Integer, String> cache = ignite.getOrCreateCache(cacheConfiguration())) {
                    try (IgniteCache<Integer, String> cache1 = ignite1.getOrCreateCache(cacheConfiguration())) {
                        checkStore(cache, JdbcDataSource.class);

                        checkStore(cache1, CacheJdbcBlobStoreFactorySelfTest.DummyDataSource.class);
                    }
                }
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSerializable() throws Exception {
        try (Ignite ignite = Ignition.start("modules/spring/src/test/config/node.xml")) {
            try (IgniteCache<Integer, String> cache = ignite.getOrCreateCache(cacheConfigurationH2Dialect())) {
                checkStore(cache, JdbcDataSource.class);
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testIncorrectBeanConfiguration() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                try(Ignite ignite = Ignition.start("modules/spring/src/test/config/pojo-incorrect-store-cache.xml")) {
                    ignite.cache(CACHE_NAME).getConfiguration(CacheConfiguration.class).
                        getCacheStoreFactory().create();
                }
                return null;
            }
        }, IgniteException.class, "Failed to load bean in application context");
    }

    /**
     * @return Cache configuration with store.
     */
    private CacheConfiguration<Integer, String> cacheConfiguration() {
        CacheConfiguration<Integer, String> cfg = new CacheConfiguration<>();

        CacheJdbcPojoStoreFactory<Integer, String> factory = new CacheJdbcPojoStoreFactory<>();

        factory.setDataSourceBean("simpleDataSource");

        factory.setDialect(new DummyDialect());

        cfg.setCacheStoreFactory(factory);

        return cfg;
    }

    /**
     * @return Cache configuration with store.
     */
    private CacheConfiguration<Integer, String> cacheConfigurationH2Dialect() {
        CacheConfiguration<Integer, String> cfg = new CacheConfiguration<>();

        CacheJdbcPojoStoreFactory<Integer, String> factory = new CacheJdbcPojoStoreFactory<>();

        factory.setDataSourceBean("simpleDataSource");

        factory.setDialect(new H2Dialect());

        cfg.setCacheStoreFactory(factory);

        return cfg;
    }

    /**
     * @param cache Ignite cache.
     * @param dataSrcClass Data source class.
     * @throws Exception If store parameters is not the same as in configuration xml.
     */
    private void checkStore(IgniteCache<Integer, String> cache, Class<?> dataSrcClass) throws Exception {
        CacheJdbcPojoStore store = (CacheJdbcPojoStore)cache.getConfiguration(CacheConfiguration.class).
            getCacheStoreFactory().create();

        assertEquals(dataSrcClass,
            GridTestUtils.getFieldValue(store, CacheAbstractJdbcStore.class, "dataSrc").getClass());
    }

    /**
     *
     */
    public static class DummyDialect implements JdbcDialect, Serializable {
        /** {@inheritDoc} */
        @Override public String loadCacheSelectRangeQuery(String fullTblName, Collection<String> keyCols) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public String loadCacheRangeQuery(String fullTblName, Collection<String> keyCols,
            Iterable<String> uniqCols, boolean appendLowerBound, boolean appendUpperBound) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public String loadCacheQuery(String fullTblName, Iterable<String> uniqCols) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public String loadQuery(String fullTblName, Collection<String> keyCols, Iterable<String> cols,
            int keyCnt) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public String insertQuery(String fullTblName, Collection<String> keyCols,
            Collection<String> valCols) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public String updateQuery(String fullTblName, Collection<String> keyCols, Iterable<String> valCols) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean hasMerge() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public String mergeQuery(String fullTblName, Collection<String> keyCols,
            Collection<String> uniqCols) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public String removeQuery(String fullTblName, Iterable<String> keyCols) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public int getMaxParameterCount() {
            return 0;
        }
    }
}