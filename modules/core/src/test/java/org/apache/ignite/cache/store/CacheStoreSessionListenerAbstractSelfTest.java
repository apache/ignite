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

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

/**
 * Tests for store session listeners.
 */
public abstract class CacheStoreSessionListenerAbstractSelfTest extends GridCommonAbstractTest implements Serializable {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    protected static final String URL = "jdbc:h2:mem:example;DB_CLOSE_DELAY=-1";

    /** */
    protected static final AtomicInteger loadCacheCnt = new AtomicInteger();

    /** */
    protected static final AtomicInteger loadCnt = new AtomicInteger();

    /** */
    protected static final AtomicInteger writeCnt = new AtomicInteger();

    /** */
    protected static final AtomicInteger deleteCnt = new AtomicInteger();

    /** */
    protected static final AtomicInteger reuseCnt = new AtomicInteger();

    /** */
    protected static final AtomicBoolean write = new AtomicBoolean();

    /** */
    protected static final AtomicBoolean fail = new AtomicBoolean();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            conn.createStatement().executeUpdate("DROP TABLE IF EXISTS Table1");
            conn.createStatement().executeUpdate("DROP TABLE IF EXISTS Table2");

            conn.createStatement().executeUpdate("CREATE TABLE Table1 (id INT AUTO_INCREMENT, key INT, value INT)");
            conn.createStatement().executeUpdate("CREATE TABLE Table2 (id INT AUTO_INCREMENT, key INT, value INT)");
        }

        loadCacheCnt.set(0);
        loadCnt.set(0);
        writeCnt.set(0);
        deleteCnt.set(0);
        reuseCnt.set(0);

        write.set(false);
        fail.set(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicCache() throws Exception {
        CacheConfiguration<Integer, Integer> cfg = cacheConfiguration(null, CacheAtomicityMode.ATOMIC);

        IgniteCache<Integer, Integer> cache = ignite(0).createCache(cfg);

        try {
            cache.loadCache(null);
            cache.get(1);
            cache.put(1, 1);
            cache.remove(1);
        }
        finally {
            cache.destroy();
        }

        assertEquals(3, loadCacheCnt.get());
        assertEquals(1, loadCnt.get());
        assertEquals(1, writeCnt.get());
        assertEquals(1, deleteCnt.get());
        assertEquals(0, reuseCnt.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransactionalCache() throws Exception {
        CacheConfiguration<Integer, Integer> cfg = cacheConfiguration(null, CacheAtomicityMode.TRANSACTIONAL);

        IgniteCache<Integer, Integer> cache = ignite(0).createCache(cfg);

        try {
            cache.loadCache(null);
            cache.get(1);
            cache.put(1, 1);
            cache.remove(1);
        }
        finally {
            cache.destroy();
        }

        assertEquals(3, loadCacheCnt.get());
        assertEquals(1, loadCnt.get());
        assertEquals(1, writeCnt.get());
        assertEquals(1, deleteCnt.get());
        assertEquals(0, reuseCnt.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testExplicitTransaction() throws Exception {
        CacheConfiguration<Integer, Integer> cfg = cacheConfiguration(null, CacheAtomicityMode.TRANSACTIONAL);

        IgniteCache<Integer, Integer> cache = ignite(0).createCache(cfg);

        try (Transaction tx = ignite(0).transactions().txStart()) {
            cache.put(1, 1);
            cache.put(2, 2);
            cache.remove(3);
            cache.remove(4);

            tx.commit();
        }
        finally {
            cache.destroy();
        }

        assertEquals(2, writeCnt.get());
        assertEquals(2, deleteCnt.get());
        assertEquals(3, reuseCnt.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testCrossCacheTransaction() throws Exception {
        CacheConfiguration<Integer, Integer> cfg1 = cacheConfiguration("cache1", CacheAtomicityMode.TRANSACTIONAL);
        CacheConfiguration<Integer, Integer> cfg2 = cacheConfiguration("cache2", CacheAtomicityMode.TRANSACTIONAL);

        IgniteCache<Integer, Integer> cache1 = ignite(0).createCache(cfg1);
        IgniteCache<Integer, Integer> cache2 = ignite(0).createCache(cfg2);

        try (Transaction tx = ignite(0).transactions().txStart()) {
            cache1.put(1, 1);
            cache2.put(2, 2);
            cache1.remove(3);
            cache2.remove(4);

            tx.commit();
        }
        finally {
            cache1.destroy();
            cache2.destroy();
        }

        assertEquals(2, writeCnt.get());
        assertEquals(2, deleteCnt.get());
        assertEquals(3, reuseCnt.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testCommit() throws Exception {
        write.set(true);

        CacheConfiguration<Integer, Integer> cfg1 = cacheConfiguration("cache1", CacheAtomicityMode.TRANSACTIONAL);
        CacheConfiguration<Integer, Integer> cfg2 = cacheConfiguration("cache2", CacheAtomicityMode.TRANSACTIONAL);

        IgniteCache<Integer, Integer> cache1 = ignite(0).createCache(cfg1);
        IgniteCache<Integer, Integer> cache2 = ignite(0).createCache(cfg2);

        try (Transaction tx = ignite(0).transactions().txStart()) {
            cache1.put(1, 1);
            cache2.put(2, 2);

            tx.commit();
        }
        finally {
            cache1.destroy();
            cache2.destroy();
        }

        try (Connection conn = DriverManager.getConnection(URL)) {
            checkTable(conn, 1, false);
            checkTable(conn, 2, false);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRollback() throws Exception {
        write.set(true);
        fail.set(true);

        CacheConfiguration<Integer, Integer> cfg1 = cacheConfiguration("cache1", CacheAtomicityMode.TRANSACTIONAL);
        CacheConfiguration<Integer, Integer> cfg2 = cacheConfiguration("cache2", CacheAtomicityMode.TRANSACTIONAL);

        IgniteCache<Integer, Integer> cache1 = ignite(0).createCache(cfg1);
        IgniteCache<Integer, Integer> cache2 = ignite(0).createCache(cfg2);

        try (Transaction tx = ignite(0).transactions().txStart()) {
            cache1.put(1, 1);
            cache2.put(2, 2);

            tx.commit();

            assert false : "Exception was not thrown.";
        }
        catch (IgniteException e) {
            CacheWriterException we = X.cause(e, CacheWriterException.class);

            assertNotNull(we);

            assertEquals("Expected failure.", we.getMessage());
        }
        finally {
            cache1.destroy();
            cache2.destroy();
        }

        try (Connection conn = DriverManager.getConnection(URL)) {
            checkTable(conn, 1, true);
            checkTable(conn, 2, true);
        }
    }

    /**
     * @param conn Connection.
     * @param idx Table index.
     * @param empty If table expected to be empty.
     * @throws Exception In case of error.
     */
    private void checkTable(Connection conn, int idx, boolean empty) throws Exception {
        ResultSet rs = conn.createStatement().executeQuery("SELECT key, value FROM Table" + idx);

        int cnt = 0;

        while (rs.next()) {
            int key = rs.getInt(1);
            int val = rs.getInt(2);

            assertEquals(idx, key);
            assertEquals(idx, val);

            cnt++;
        }

        assertEquals(empty ? 0 : 1, cnt);
    }

    /**
     * @param name Cache name.
     * @param atomicity Atomicity mode.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(String name, CacheAtomicityMode atomicity) {
        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>();

        cfg.setName(name);
        cfg.setAtomicityMode(atomicity);
        cfg.setCacheStoreFactory(storeFactory());
        cfg.setCacheStoreSessionListenerFactories(sessionListenerFactory());
        cfg.setReadThrough(true);
        cfg.setWriteThrough(true);
        cfg.setLoadPreviousValue(true);

        return cfg;
    }

    /**
     * @return Store factory.
     */
    protected abstract Factory<? extends CacheStore<Integer, Integer>> storeFactory();

    /**
     * @return Session listener factory.
     */
    protected abstract Factory<CacheStoreSessionListener> sessionListenerFactory();
}