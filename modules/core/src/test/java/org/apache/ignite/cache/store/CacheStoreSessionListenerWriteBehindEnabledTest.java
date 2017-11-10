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

package org.apache.ignite.cache.store;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.store.jdbc.CacheJdbcStoreSessionListener;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStore;
import org.apache.ignite.resources.IgniteInstanceResource;

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.sql.DataSource;
import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * This class tests that calls of {@link CacheStoreSessionListener#onSessionStart(CacheStoreSession)}
 * and {@link CacheStoreSessionListener#onSessionEnd(CacheStoreSession, boolean)} are executed from
 * {@link GridCacheWriteBehindStore} only.
 */
public class CacheStoreSessionListenerWriteBehindEnabledTest extends GridCacheAbstractSelfTest {
    /** */
    protected final static int CNT = 100;

    /** */
    private final static int WRITE_BEHIND_FLUSH_FREQUENCY = 1000;

    /** */
    private static final List<OperationType> operations = Collections.synchronizedList(new ArrayList<OperationType>());

    /** */
    private static final AtomicInteger entryCnt = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration cacheCfg = super.cacheConfiguration(igniteInstanceName);

        cacheCfg.setCacheStoreFactory(FactoryBuilder.factoryOf(EmptyCacheStore.class));

        cacheCfg.setCacheStoreSessionListenerFactories(new CacheStoreSessionFactory());

        cacheCfg.setReadThrough(true);
        cacheCfg.setWriteThrough(true);

        cacheCfg.setWriteBehindEnabled(true);
        cacheCfg.setWriteBehindBatchSize(CNT * 2);
        cacheCfg.setWriteBehindFlushFrequency(WRITE_BEHIND_FLUSH_FREQUENCY);

        cacheCfg.setBackups(0);

        return cacheCfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        operations.clear();

        entryCnt.set(0);
    }

    /**
     * Tests that there are no redundant calls of {@link CacheStoreSessionListener#onSessionStart(CacheStoreSession)}
     * while {@link IgniteCache#get(Object)} performed.
     */
    public void testLookup() {
        IgniteCache<Object, Object> cache = grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < CNT; ++i)
            cache.get(i);

        checkSessionCounters(CNT);
    }

    /**
     * Tests that there are no redundant calls of {@link CacheStoreSessionListener#onSessionStart(CacheStoreSession)}
     * while {@link IgniteCache#put(Object, Object)} performed.
     */
    public void testUpdate() {
        IgniteCache<Object, Object> cache = grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < CNT; ++i)
            cache.put(i, i);

        checkSessionCounters(1);
    }

    /**
     * Tests that there are no redundant calls of {@link CacheStoreSessionListener#onSessionStart(CacheStoreSession)}
     * while {@link IgniteCache#remove(Object)} performed.
     */
    public void testRemove() {
        IgniteCache<Object, Object> cache = grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < CNT; ++i) {
            cache.remove(i);
        }

        checkSessionCounters(1);
    }

    /**
     * @param startedSessions Number of expected sessions.
     */
    private void checkSessionCounters(int startedSessions) {
        try {
            // Wait for GridCacheWriteBehindStore
            Thread.sleep(WRITE_BEHIND_FLUSH_FREQUENCY * 4);

            assertEquals(CNT, entryCnt.get());

            checkOpCount(operations, OperationType.SESSION_START, startedSessions);

            checkOpCount(operations, OperationType.SESSION_END, startedSessions);
        }
        catch (InterruptedException e) {
            throw new IgniteException("Failed to wait for the GridCacheWriteBehindStore due to interruption.", e);
        }
    }

    /**
     * @param operations List of {@link OperationType}.
     * @param op Operation.
     * @param expected Expected number of operations for the given {@code op}.
     */
    private void checkOpCount(List<OperationType> operations, OperationType op, int expected) {
        int n = 0;

        for (OperationType o : operations) {
            if (op.equals(o))
                ++n;
        }

        assertEquals("Operation=" + op.name(), expected, n);
    }

    /**
     * Operation type.
     */
    public enum OperationType {
        /**
         * Cache store session started.
         */
        SESSION_START,

        /**
         * Cache store session ended.
         */
        SESSION_END,
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
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public void onSessionStart(CacheStoreSession ses) {
            operations.add(OperationType.SESSION_START);
        }

        /** {@inheritDoc} */
        @Override public void onSessionEnd(CacheStoreSession ses, boolean commit) {
            operations.add(OperationType.SESSION_END);
        }
    }

    /**
     * Test cache store.
     *
     * {@link EmptyCacheStore#writeAll(Collection)} and {@link EmptyCacheStore#deleteAll(Collection)} should be called
     * by {@link GridCacheWriteBehindStore}.
     */
    public static class EmptyCacheStore extends CacheStoreAdapter<Object, Object> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public Object load(Object key) throws CacheLoaderException {
            entryCnt.getAndIncrement();
            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeAll(Collection<Cache.Entry<?, ?>> entries) {
            entryCnt.addAndGet(entries.size());
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry entry) throws CacheWriterException {
        }

        /** {@inheritDoc} */
        @Override public void deleteAll(Collection<?> keys) {
            entryCnt.addAndGet(keys.size());
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
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
