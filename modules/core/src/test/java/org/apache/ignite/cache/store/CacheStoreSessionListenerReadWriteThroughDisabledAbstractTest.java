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

package org.apache.ignite.cache.store;

import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Logger;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.sql.DataSource;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.store.jdbc.CacheJdbcStoreSessionListener;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.junit.Test;

/**
 * This class tests that redundant calls of {@link CacheStoreSessionListener#onSessionStart(CacheStoreSession)}
 * and {@link CacheStoreSessionListener#onSessionEnd(CacheStoreSession, boolean)} are not executed.
 */
public abstract class CacheStoreSessionListenerReadWriteThroughDisabledAbstractTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /** */
    protected final int CNT = 100;

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration cacheCfg = super.cacheConfiguration(igniteInstanceName);

        cacheCfg.setCacheStoreFactory(FactoryBuilder.factoryOf(EmptyCacheStore.class));

        cacheCfg.setCacheStoreSessionListenerFactories(new CacheStoreSessionFactory());

        cacheCfg.setReadThrough(false);
        cacheCfg.setWriteThrough(false);

        cacheCfg.setBackups(0);

        return cacheCfg;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /**
     * Tests that there are no calls of {@link CacheStoreSessionListener#onSessionStart(CacheStoreSession)} and
     * {@link CacheStoreSessionListener#onSessionStart(CacheStoreSession)}
     * while {@link IgniteCache#get(Object)} performed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLookup() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        Random r = new Random();

        for (int i = 0; i < CNT; ++i)
            cache.get(r.nextInt());
    }

    /**
     * Tests that there are no calls of {@link CacheStoreSessionListener#onSessionStart(CacheStoreSession)} and
     * {@link CacheStoreSessionListener#onSessionStart(CacheStoreSession)}
     * while {@link IgniteCache#getAll(Set)} performed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBatchLookup() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        Random r = new Random();

        Set<Object> values = new HashSet<>();

        for (int i = 0; i < CNT; ++i)
            values.add(r.nextInt());

        cache.getAll(values);
    }

    /**
     * Tests that there are no calls of {@link CacheStoreSessionListener#onSessionStart(CacheStoreSession)} and
     * {@link CacheStoreSessionListener#onSessionStart(CacheStoreSession)}
     * while {@link IgniteCache#put(Object, Object)} performed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUpdate() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        Random r = new Random();

        for (int i = 0; i < CNT; ++i)
            cache.put(r.nextInt(), "test-value");
    }

    /**
     * Tests that there are no calls of {@link CacheStoreSessionListener#onSessionStart(CacheStoreSession)} and
     * {@link CacheStoreSessionListener#onSessionStart(CacheStoreSession)}
     * while {@link IgniteCache#putAll(Map)} performed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBatchUpdate() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        Random r = new Random();

        Map<Object, Object> values = new TreeMap<>();

        for (int i = 0; i < CNT; ++i)
            values.put(r.nextInt(), "test-value");

        cache.putAll(values);
    }

    /**
     * Tests that there are no calls of {@link CacheStoreSessionListener#onSessionStart(CacheStoreSession)} and
     * {@link CacheStoreSessionListener#onSessionStart(CacheStoreSession)}
     * while {@link IgniteCache#remove(Object)} performed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRemove() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        Random r = new Random();

        for (int i = 0; i < CNT; ++i) {
            int key = r.nextInt();

            cache.put(key, "test-value");

            cache.remove(key);
        }
    }

    /**
     * Tests that there are no calls of {@link CacheStoreSessionListener#onSessionStart(CacheStoreSession)} and
     * {@link CacheStoreSessionListener#onSessionStart(CacheStoreSession)}
     * while {@link IgniteCache#removeAll(Set)} performed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBatchRemove() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        Random r = new Random();

        Set<Object> values = new HashSet<>();

        for (int i = 0; i < CNT; ++i) {
            int key = r.nextInt();

            cache.put(key, "test-value");

            values.add(key);
        }

        cache.removeAll(values);
    }

    /**
     * Cache store session factory.
     */
    public static class CacheStoreSessionFactory implements Factory<TestCacheStoreSessionListener> {
        /** {@inheritDoc} */
        @Override public TestCacheStoreSessionListener create() {
            TestCacheStoreSessionListener lsnr = new TestCacheStoreSessionListener();
            lsnr.setDataSource(new DataSourceStub());
            return lsnr;
        }
    }

    /**
     * Test cache store session listener.
     */
    public static class TestCacheStoreSessionListener extends CacheJdbcStoreSessionListener {
        /** {@inheritDoc} */
        @Override public void onSessionStart(CacheStoreSession ses) {
            fail("TestCacheStoreSessionListener.onSessionStart(CacheStoreSession) should not be called.");
        }

        /** {@inheritDoc} */
        @Override public void onSessionEnd(CacheStoreSession ses, boolean commit) {
            fail("TestCacheStoreSessionListener.onSessionEnd(CacheStoreSession, boolean) should not be called.");
        }
    }

    /** Empty cache store implementation. All overridden methods should not be called while the test is running. */
    public static class EmptyCacheStore extends CacheStoreAdapter {
        /** {@inheritDoc} */
        @Override public Object load(Object key) throws CacheLoaderException {
            fail("EmptyCacheStore.load(Object) should not be called.");

            return null;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry entry) throws CacheWriterException {
            fail("EmptyCacheStore.write(Cache.Entry) should not be called.");
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            fail("EmptyCacheStore.delete(Object) should not be called.");
        }
    }

    /**
     * Data source stub which should not be called.
     */
    public static class DataSourceStub implements DataSource, Serializable {
        /** {@inheritDoc} */
        @Override public Connection getConnection() throws SQLException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public Connection getConnection(String username, String password) throws SQLException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public PrintWriter getLogWriter() throws SQLException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void setLogWriter(PrintWriter out) throws SQLException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void setLoginTimeout(int seconds) throws SQLException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public int getLoginTimeout() throws SQLException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new UnsupportedOperationException();
        }
    }
}
