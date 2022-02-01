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

import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.sql.DataSource;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.store.jdbc.CacheJdbcStoreSessionListener;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStore;
import org.apache.ignite.resources.CacheStoreSessionResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.junit.Before;
import org.junit.Test;

/**
 * This class tests that calls of {@link CacheStoreSessionListener#onSessionStart(CacheStoreSession)}
 * and {@link CacheStoreSessionListener#onSessionEnd(CacheStoreSession, boolean)} are executed from
 * {@link GridCacheWriteBehindStore} only.
 */
public class CacheStoreSessionListenerWriteBehindEnabledTest extends GridCacheAbstractSelfTest {
    /** */
    protected static final int CNT = 100;

    /** */
    private static final int WRITE_BEHIND_FLUSH_FREQUENCY = 1000;

    /** */
    private static final List<OperationType> operations = Collections.synchronizedList(new ArrayList<OperationType>());

    /** */
    private static final AtomicInteger entryCnt = new AtomicInteger();

    /** */
    private static final AtomicInteger uninitializedListenerCnt = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.CACHE_STORE);

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

    /** */
    @Before
    public void beforeCacheStoreSessionListenerWriteBehindEnabledTest() {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.CACHE_STORE);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        operations.clear();

        entryCnt.set(0);

        uninitializedListenerCnt.set(0);
    }

    /**
     * Tests that there are no redundant calls of {@link CacheStoreSessionListener#onSessionStart(CacheStoreSession)}
     * while {@link IgniteCache#get(Object)} performed.
     */
    @Test
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
    @Test
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
    @Test
    public void testRemove() {
        IgniteCache<Object, Object> cache = grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < CNT; ++i) {
            cache.remove(i);
        }

        checkSessionCounters(1);
    }

    /**
     * Tests that cache store session listeners are notified by write-behind store.
     */
    @Test
    public void testFlushSingleValue() throws Exception {
        CacheConfiguration cfg = cacheConfiguration(getTestIgniteInstanceName());

        cfg.setName("back-pressure-control");
        cfg.setWriteBehindBatchSize(2);
        cfg.setWriteBehindFlushSize(2);
        cfg.setWriteBehindFlushFrequency(1_000);
        cfg.setWriteBehindCoalescing(true);

        IgniteCache<Object, Object> cache = grid(0).getOrCreateCache(cfg);

        try {
            int nUploaders = 5;

            final CyclicBarrier barrier = new CyclicBarrier(nUploaders);

            IgniteInternalFuture[] uploaders = new IgniteInternalFuture[nUploaders];

            for (int i = 0; i < nUploaders; ++i) {
                uploaders[i] = GridTestUtils.runAsync(
                    new Uploader(cache, barrier, i * CNT),
                    "uploader-" + i);
            }

            for (int i = 0; i < nUploaders; ++i)
                uploaders[i].get();

            assertEquals("Uninitialized cache store session listener.", 0, uninitializedListenerCnt.get());
        }
        finally {
            cache.destroy();
        }
    }

    /**
     *
     */
    public static class Uploader implements Runnable {
        /** */
        private final int start;

        /** */
        private final CyclicBarrier barrier;

        /** */
        private final IgniteCache<Object, Object> cache;

        /**
         * @param cache Ignite cache.
         * @param barrier Cyclic barrier.
         * @param start Key index.
         */
        public Uploader(IgniteCache<Object, Object> cache, CyclicBarrier barrier, int start) {
            this.cache = cache;

            this.barrier = barrier;

            this.start = start;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                barrier.await();

                for (int i = start; i < start + CNT; ++i)
                    cache.put(i, i);
            }
            catch (Exception e) {
                fail("Unexpected exception [" + e + "]");
            }
        }
    }

    /**
     * @param startedSessions Number of expected sessions.
     */
    private void checkSessionCounters(int startedSessions) {
        try {
            // Wait for GridCacheWriteBehindStore
            Thread.sleep(WRITE_BEHIND_FLUSH_FREQUENCY * 4);

            assertEquals(CNT, entryCnt.get());

            assertEquals("Uninitialized cache store session listener.", 0, uninitializedListenerCnt.get());

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
        /** {@inheritDoc} */
        @Override public void onSessionStart(CacheStoreSession ses) {
            operations.add(OperationType.SESSION_START);

            if (ses.attachment() == null)
                ses.attach(new Object());
        }

        /** {@inheritDoc} */
        @Override public void onSessionEnd(CacheStoreSession ses, boolean commit) {
            operations.add(OperationType.SESSION_END);

            ses.attach(null);
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
        @CacheStoreSessionResource
        private CacheStoreSession ses;

        /** {@inheritDoc} */
        @Override public Object load(Object key) throws CacheLoaderException {
            entryCnt.getAndIncrement();

            if (ses.attachment() == null)
                uninitializedListenerCnt.incrementAndGet();

            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeAll(Collection<Cache.Entry<?, ?>> entries) {
            entryCnt.addAndGet(entries.size());

            if (ses.attachment() == null)
                uninitializedListenerCnt.incrementAndGet();
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry entry) throws CacheWriterException {
            if (ses.attachment() == null)
                uninitializedListenerCnt.incrementAndGet();
        }

        /** {@inheritDoc} */
        @Override public void deleteAll(Collection<?> keys) {
            entryCnt.addAndGet(keys.size());

            if (ses.attachment() == null)
                uninitializedListenerCnt.incrementAndGet();
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            if (ses.attachment() == null)
                uninitializedListenerCnt.incrementAndGet();
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
