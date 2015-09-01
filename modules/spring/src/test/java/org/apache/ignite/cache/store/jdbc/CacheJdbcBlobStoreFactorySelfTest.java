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

import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.Callable;
import java.util.logging.Logger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.h2.jdbcx.JdbcDataSource;

/**
 * Test for Cache jdbc blob store factory.
 */
public class CacheJdbcBlobStoreFactorySelfTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "test";

    /** User name. */
    private static final String USER_NAME = "GridGain";

    /**
     * @throws Exception If failed.
     */
    public void testXmlConfiguration() throws Exception {
        try (Ignite ignite = Ignition.start("modules/spring/src/test/config/store-cache.xml")) {
            try(Ignite ignite1 = Ignition.start("modules/spring/src/test/config/store-cache1.xml")) {
                checkStore(ignite.<Integer, String>cache(CACHE_NAME), JdbcDataSource.class);

                checkStore(ignite1.<Integer, String>cache(CACHE_NAME), DummyDataSource.class);
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheConfiguration() throws Exception {
        try (Ignite ignite = Ignition.start("modules/spring/src/test/config/node.xml")) {
            try (Ignite ignite1 = Ignition.start("modules/spring/src/test/config/node1.xml")) {
                try (IgniteCache<Integer, String> cache = ignite.getOrCreateCache(cacheConfiguration())) {
                    try (IgniteCache<Integer, String> cache1 = ignite1.getOrCreateCache(cacheConfiguration())) {
                        checkStore(cache, JdbcDataSource.class);

                        checkStore(cache1, DummyDataSource.class);
                    }
                }
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testIncorrectBeanConfiguration() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                try(Ignite ignite = Ignition.start("modules/spring/src/test/config/incorrect-store-cache.xml")) {
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

        CacheJdbcBlobStoreFactory<Integer, String> factory = new CacheJdbcBlobStoreFactory();

        factory.setUser(USER_NAME);

        factory.setDataSourceBean("simpleDataSource");

        cfg.setCacheStoreFactory(factory);

        return cfg;
    }

    /**
     * @param cache Ignite cache.
     * @param dataSrcClass Data source class.
     * @throws Exception If store parameters is not the same as in configuration xml.
     */
    private void checkStore(IgniteCache<Integer, String> cache, Class<?> dataSrcClass) throws Exception {
        CacheJdbcBlobStore store = (CacheJdbcBlobStore) cache.getConfiguration(CacheConfiguration.class).
            getCacheStoreFactory().create();

        assertEquals(USER_NAME, GridTestUtils.getFieldValue(store, CacheJdbcBlobStore.class, "user"));

        assertEquals(dataSrcClass,
            GridTestUtils.getFieldValue(store, CacheJdbcBlobStore.class, "dataSrc").getClass());
    }

    /**
     *
     */
    public static class DummyDataSource implements javax.sql.DataSource, Serializable {
        /** */
        public DummyDataSource() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Connection getConnection() throws SQLException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Connection getConnection(String username, String password) throws SQLException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public PrintWriter getLogWriter() throws SQLException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void setLogWriter(PrintWriter out) throws SQLException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void setLoginTimeout(int seconds) throws SQLException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public int getLoginTimeout() throws SQLException {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public <T> T unwrap(Class<T> iface) throws SQLException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return false;
        }
    }
}