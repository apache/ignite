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
import java.util.Random;
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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * This class tests that redundant calls of {@link CacheStoreSessionListener#onSessionStart(CacheStoreSession)}
 * and {@link CacheStoreSessionListener#onSessionEnd(CacheStoreSession, boolean)} are not executed.
 */
public class CacheStoreSessionListenerReadWriteThroughDisabled extends GridCacheAbstractSelfTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration cacheCfg = super.cacheConfiguration(igniteInstanceName);

        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        cacheCfg.setCacheStoreFactory(FactoryBuilder.factoryOf(EmptyCacheStore.class));

        cacheCfg.setCacheStoreSessionListenerFactories(new CacheStroreSessionFactory());

        cacheCfg.setReadThrough(false);
        cacheCfg.setWriteThrough(false);

        return cacheCfg;
    }

    public void testLookup() throws Exception {
        IgniteCache cache = grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.get(new Random().nextInt());
    }

    public void testUpdate() throws Exception {
        IgniteCache cache = grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        int key = new Random().nextInt();

        cache.put(key, key);
    }

    public void testRemove() throws Exception {
        IgniteCache cache = grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        int key = new Random().nextInt();

        cache.put(key, key);

        cache.remove(key);
    }

    public static class CacheStroreSessionFactory implements Factory<TestCacheStoreSessionListener> {
        /** {@inheritDoc} */
        @Override public TestCacheStoreSessionListener create() {
            TestCacheStoreSessionListener lsnr = new TestCacheStoreSessionListener();
            lsnr.setDataSource(new DataSourceStub());
            return lsnr;
        }
    }

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

        @Override public void write(Cache.Entry entry) throws CacheWriterException {
            fail("EmptyCacheStore.write(Cache.Entry) should not be called.");
        }

        @Override public void delete(Object key) throws CacheWriterException {
            fail("EmptyCacheStore.delete(Object) should not be called.");
        }
    }

    public static class DataSourceStub implements DataSource, Serializable {
        @Override public Connection getConnection() throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override public Connection getConnection(String username, String password) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override public PrintWriter getLogWriter() throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override public void setLogWriter(PrintWriter out) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override public void setLoginTimeout(int seconds) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override public int getLoginTimeout() throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new UnsupportedOperationException();
        }
    }
}
