/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.cache.store.jdbc;

import java.util.Collection;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.store.jdbc.dialect.H2Dialect;
import org.apache.ignite.cache.store.jdbc.dialect.JdbcDialect;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test for Cache JDBC POJO store factory.
 */
@RunWith(JUnit4.class)
public class CacheJdbcPojoStoreFactorySelfTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "test";

    /**
     * @throws Exception If failed.
     */
    @Test
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
    @Test
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
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10723")
    @Test
    public void testIncorrectBeanConfiguration() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-10723");

        GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                try (Ignite ignored = Ignition.start("modules/spring/src/test/config/pojo-incorrect-store-cache.xml")) {
                    // No-op.
                }
                return null;
            }
        }, IgniteCheckedException.class, "Spring bean with provided name doesn't exist");
    }

    /**
     * @return Cache configuration with store.
     */
    private CacheConfiguration<Integer, String> cacheConfiguration() {
        CacheConfiguration<Integer, String> cfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

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
        CacheConfiguration<Integer, String> cfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        CacheJdbcPojoStoreFactory<Integer, String> factory = new CacheJdbcPojoStoreFactory<>();

        factory.setDataSourceBean("simpleDataSource");

        factory.setDialect(new H2Dialect());

        cfg.setCacheStoreFactory(factory);

        return cfg;
    }

    /**
     * @param cache Ignite cache.
     * @param dataSrcCls Data source class.
     * @throws Exception If store parameters is not the same as in configuration xml.
     */
    private void checkStore(IgniteCache<Integer, String> cache, Class<?> dataSrcCls) throws Exception {
        CacheJdbcPojoStore store = (CacheJdbcPojoStore)cache.getConfiguration(CacheConfiguration.class).
            getCacheStoreFactory().create();

        assertEquals(dataSrcCls,
            GridTestUtils.getFieldValue(store, CacheAbstractJdbcStore.class, "dataSrc").getClass());
    }

    /**
     * Dummy JDBC dialect that does nothing.
     */
    public static class DummyDialect implements JdbcDialect {
        /** {@inheritDoc} */
        @Override public String escape(String ident) {
            return null;
        }

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

        /** {@inheritDoc} */
        @Override public int getFetchSize() {
            return 0;
        }
    }
}
