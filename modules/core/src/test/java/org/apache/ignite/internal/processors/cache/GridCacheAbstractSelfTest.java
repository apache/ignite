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

package org.apache.ignite.internal.processors.cache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.commons.io.IOUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import org.apache.ignite.cache.store.jdbc.CacheJdbcStoreSessionListener;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.util.lang.GridAbsPredicateX;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.R1;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.CacheStoreSessionResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.tools.RunScript;
import org.h2.tools.Server;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Abstract class for cache tests.
 */
public abstract class GridCacheAbstractSelfTest extends GridCommonAbstractTest {
    /** Test timeout */
    private static final long TEST_TIMEOUT = 30 * 1000;

    /** VM ip finder for TCP discovery. */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    protected static TestCacheStoreStrategy storeStrategy;

    /**
     * @return Grids count to start.
     */
    protected abstract int gridCount();

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        int cnt = gridCount();

        assert cnt >= 1 : "At least one grid must be started";

        initStoreStrategy();

        startGrids(cnt);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        storeStrategy.resetStore();
    }

    /** Initialize {@link #storeStrategy} with respect to the nature of the test */
    void initStoreStrategy() throws IgniteCheckedException {
        if (storeStrategy == null)
            storeStrategy = isMultiJvm() ? new H2CacheStoreStrategy() : new MapCacheStoreStrategy();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        assert jcache().unwrap(Ignite.class).transactions().tx() == null;
        assertEquals(0, jcache().localSize());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        Transaction tx = jcache().unwrap(Ignite.class).transactions().tx();

        if (tx != null) {
            tx.close();

            fail("Cache transaction remained after test completion: " + tx);
        }

        for (int i = 0; i < gridCount(); i++) {
            info("Checking grid: " + i);

            while (true) {
                try {
                    final int fi = i;

                    assertTrue(
                        "Cache is not empty: " + " localSize = " + jcache(fi).localSize(CachePeekMode.ALL)
                        + ", local entries " + entrySet(jcache(fi).localEntries()),
                        GridTestUtils.waitForCondition(
                            // Preloading may happen as nodes leave, so we need to wait.
                            new GridAbsPredicateX() {
                                @Override public boolean applyx() throws IgniteCheckedException {
                                    jcache(fi).removeAll();

                                    if (jcache(fi).size(CachePeekMode.ALL) > 0) {
                                        for (Cache.Entry<String, ?> k : jcache(fi).localEntries())
                                            jcache(fi).remove(k.getKey());
                                    }

                                    return jcache(fi).localSize(CachePeekMode.ALL) == 0;
                                }
                            },
                            getTestTimeout()));

                    int primaryKeySize = jcache(i).localSize(CachePeekMode.PRIMARY);
                    int keySize = jcache(i).localSize();
                    int size = jcache(i).localSize();
                    int globalSize = jcache(i).size();
                    int globalPrimarySize = jcache(i).size(CachePeekMode.PRIMARY);

                    info("Size after [idx=" + i +
                        ", size=" + size +
                        ", keySize=" + keySize +
                        ", primarySize=" + primaryKeySize +
                        ", globalSize=" + globalSize +
                        ", globalPrimarySize=" + globalPrimarySize +
                        ", entrySet=" + jcache(i).localEntries() + ']');

                    assertEquals("Cache is not empty [idx=" + i + ", entrySet=" + jcache(i).localEntries() + ']',
                        0, jcache(i).localSize(CachePeekMode.ALL));

                    break;
                }
                catch (Exception e) {
                    if (X.hasCause(e, ClusterTopologyCheckedException.class)) {
                        info("Got topology exception while tear down (will retry in 1000ms).");

                        U.sleep(1000);
                    }
                    else
                        throw e;
                }
            }

            for (Cache.Entry<String, Integer> entry : jcache(i).localEntries(CachePeekMode.SWAP))
                jcache(i).remove(entry.getKey());
        }

        assert jcache().unwrap(Ignite.class).transactions().tx() == null;
        assertEquals("Cache is not empty", 0, jcache().localSize(CachePeekMode.ALL));

        storeStrategy.resetStore();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setMaxMissedHeartbeats(Integer.MAX_VALUE);

        disco.setIpFinder(ipFinder);

        if (isDebug())
            disco.setAckTimeout(Integer.MAX_VALUE);

        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(cacheConfiguration(gridName));

        return cfg;
    }

    /**
     * @param gridName Grid name.
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    @SuppressWarnings("unchecked")
    protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = defaultCacheConfiguration();

        Factory<? extends CacheStore<Object, Object>> storeFactory = storeStrategy.getStoreFactory();
        CacheStore<?, ?> store = storeFactory.create();

        if (store != null) {
            cfg.setCacheStoreFactory(storeFactory);
            cfg.setReadThrough(true);
            cfg.setWriteThrough(true);
            cfg.setLoadPreviousValue(true);
            storeStrategy.updateCacheConfiguration(cfg);
        }

        cfg.setSwapEnabled(swapEnabled());
        cfg.setCacheMode(cacheMode());
        cfg.setAtomicityMode(atomicityMode());
        cfg.setWriteSynchronizationMode(writeSynchronization());
        cfg.setNearConfiguration(nearConfiguration());

        Class<?>[] idxTypes = indexedTypes();

        if (!F.isEmpty(idxTypes))
            cfg.setIndexedTypes(idxTypes);

        if (cacheMode() == PARTITIONED)
            cfg.setBackups(1);

        return cfg;
    }

    /**
     * Indexed types.
     */
    protected Class<?>[] indexedTypes() {
        return null;
    }

    /**
     * @return Default cache mode.
     */
    protected CacheMode cacheMode() {
        return CacheConfiguration.DFLT_CACHE_MODE;
    }

    /**
     * @return Cache atomicity mode.
     */
    protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /**
     * @return Partitioned mode.
     */
    protected NearCacheConfiguration nearConfiguration() {
        return new NearCacheConfiguration();
    }

    /**
     * @return Write synchronization.
     */
    protected CacheWriteSynchronizationMode writeSynchronization() {
        return FULL_SYNC;
    }

    /**
     * @return {@code true} if swap should be enabled.
     */
    protected boolean swapEnabled() {
        return true;
    }

    /**
     * @return {@code true} if near cache should be enabled.
     */
    protected boolean nearEnabled() {
        return nearConfiguration() != null;
    }

    /**
     * @return {@code True} if transactions are enabled.
     * @see #txShouldBeUsed()
     */
    protected boolean txEnabled() {
        return true;
    }

    /**
     * @return {@code True} if transactions should be used.
     */
    protected boolean txShouldBeUsed() {
        return txEnabled() && !isMultiJvm();
    }

    /**
     * @return {@code True} if locking is enabled.
     */
    protected boolean lockingEnabled() {
        return true;
    }

    /**
     * @return {@code True} for partitioned caches.
     */
    protected final boolean partitionedMode() {
        return cacheMode() == PARTITIONED;
    }

    /**
     * @return Default cache instance.
     */
    @SuppressWarnings({"unchecked"})
    @Override protected IgniteCache<String, Integer> jcache() {
        return jcache(0);
    }

    /**
     * @return Transactions instance.
     */
    protected IgniteTransactions transactions() {
        return grid(0).transactions();
    }

    /**
     * @param idx Index of grid.
     * @return Default cache.
     */
    @SuppressWarnings({"unchecked"})
    @Override protected IgniteCache<String, Integer> jcache(int idx) {
        return ignite(idx).cache(null);
    }

    /**
     * @param idx Index of grid.
     * @return Cache context.
     */
    protected GridCacheContext<String, Integer> context(final int idx) {
        if (isRemoteJvm(idx) && !isRemoteJvm())
            throw new UnsupportedOperationException("Operation can't be done automatically via proxy. " +
                "Send task with this logic on remote jvm instead.");

        return ((IgniteKernal)grid(idx)).<String, Integer>internalCache().context();
    }

    /**
     * @param key Key.
     * @param idx Node index.
     * @return {@code True} if key belongs to node with index idx.
     */
    protected boolean belongs(String key, int idx) {
        return context(idx).cache().affinity().isPrimaryOrBackup(context(idx).localNode(), key);
    }

    /**
     * @param cache Cache.
     * @return {@code True} if cache has OFFHEAP_TIERED memory mode.
     */
    protected <K, V> boolean offheapTiered(IgniteCache<K, V> cache) {
        return cache.getConfiguration(CacheConfiguration.class).getMemoryMode() == OFFHEAP_TIERED;
    }

    /**
     * Executes regular peek or peek from swap.
     *
     * @param cache Cache projection.
     * @param key Key.
     * @return Value.
     */
    @Nullable protected <K, V> V peek(IgniteCache<K, V> cache, K key) {
        return offheapTiered(cache) ? cache.localPeek(key, CachePeekMode.SWAP, CachePeekMode.OFFHEAP) :
            cache.localPeek(key, CachePeekMode.ONHEAP);
    }

    /**
     * @param cache Cache.
     * @param key Key.
     * @return {@code True} if cache contains given key.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    protected boolean containsKey(IgniteCache cache, Object key) throws Exception {
        return offheapTiered(cache) ? cache.localPeek(key, CachePeekMode.OFFHEAP) != null : cache.containsKey(key);
    }

    /**
     * Filters cache entry projections leaving only ones with keys containing 'key'.
     */
    protected static IgnitePredicate<Cache.Entry<String, Integer>> entryKeyFilter =
        new P1<Cache.Entry<String, Integer>>() {
        @Override public boolean apply(Cache.Entry<String, Integer> entry) {
            return entry.getKey().contains("key");
        }
    };

    /**
     * Filters cache entry projections leaving only ones with keys not containing 'key'.
     */
    protected static IgnitePredicate<Cache.Entry<String, Integer>> entryKeyFilterInv =
        new P1<Cache.Entry<String, Integer>>() {
        @Override public boolean apply(Cache.Entry<String, Integer> entry) {
            return !entry.getKey().contains("key");
        }
    };

    /**
     * Filters cache entry projections leaving only ones with values less than 50.
     */
    protected static final IgnitePredicate<Cache.Entry<String, Integer>> lt50 =
        new P1<Cache.Entry<String, Integer>>() {
            @Override public boolean apply(Cache.Entry<String, Integer> entry) {
                Integer i = entry.getValue();

                return i != null && i < 50;
            }
        };

    /**
     * Filters cache entry projections leaving only ones with values greater or equal than 100.
     */
    protected static final IgnitePredicate<Cache.Entry<String, Integer>> gte100 =
        new P1<Cache.Entry<String, Integer>>() {
            @Override public boolean apply(Cache.Entry<String, Integer> entry) {
                Integer i = entry.getValue();

                return i != null && i >= 100;
            }

            @Override public String toString() {
                return "gte100";
            }
        };

    /**
     * Filters cache entry projections leaving only ones with values greater or equal than 200.
     */
    protected static final IgnitePredicate<Cache.Entry<String, Integer>> gte200 =
        new P1<Cache.Entry<String, Integer>>() {
            @Override public boolean apply(Cache.Entry<String, Integer> entry) {
                Integer i = entry.getValue();

                return i != null && i >= 200;
            }

            @Override public String toString() {
                return "gte200";
            }
        };

    /**
     * {@link org.apache.ignite.lang.IgniteInClosure} for calculating sum.
     */
    @SuppressWarnings({"PublicConstructorInNonPublicClass"})
    protected static final class SumVisitor implements CI1<Cache.Entry<String, Integer>> {
        /** */
        private final AtomicInteger sum;

        /**
         * @param sum {@link AtomicInteger} instance for accumulating sum.
         */
        public SumVisitor(AtomicInteger sum) {
            this.sum = sum;
        }

        /** {@inheritDoc} */
        @Override public void apply(Cache.Entry<String, Integer> entry) {
            if (entry.getValue() != null) {
                Integer i = entry.getValue();

                assert i != null : "Value cannot be null for entry: " + entry;

                sum.addAndGet(i);
            }
        }
    }

    /**
     * {@link org.apache.ignite.lang.IgniteReducer} for calculating sum.
     */
    @SuppressWarnings({"PublicConstructorInNonPublicClass"})
    protected static final class SumReducer implements R1<Cache.Entry<String, Integer>, Integer> {
        /** */
        private int sum;

        /** */
        public SumReducer() {
            // no-op
        }

        /** {@inheritDoc} */
        @Override public boolean collect(Cache.Entry<String, Integer> entry) {
            if (entry.getValue() != null) {
                Integer i = entry.getValue();

                assert i != null;

                sum += i;
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public Integer reduce() {
            return sum;
        }
    }

    /** Interface for cache store backend manipulation and stats routines */
    protected interface TestCacheStoreStrategy {

        /** */
        void afterTestsStopped();

        /** */
        int getReads();

        /** */
        int getWrites();

        /** */
        int getRemoves();

        /** */
        int getStoreSize();

        /** */
        void resetStore();

        /**
         * Put entry to cache store
         *
         * @param key Key.
         * @param val Value.
         */
        void putToStore(Object key, Object val);

        /** */
        void putAllToStore(Map<?, ?> data);

        /** */
        Object getFromStore(Object key);

        /** */
        void removeFromStore(Object key);

        /** */
        boolean isInStore(Object key);

        /** */
        void updateCacheConfiguration(CacheConfiguration<Object, Object> configuration);

        /**
         * @return Factory for write-through storage emulator
         */
        Factory<? extends CacheStore<Object, Object>> getStoreFactory();
    }

    /** {@link TestCacheStoreStrategy} implemented as a wrapper around {@link #map} */
    protected static class MapCacheStoreStrategy implements TestCacheStoreStrategy {

        /** Removes counter. */
        private final static AtomicInteger removes = new AtomicInteger();

        /** Writes counter. */
        private final static AtomicInteger writes = new AtomicInteger();

        /** Reads counter. */
        private final static AtomicInteger reads = new AtomicInteger();

        /** Store map. */
        private final static Map<Object, Object> map = new ConcurrentHashMap8<>();

        /** {@inheritDoc} */
        @Override public void afterTestsStopped() {
            resetStore();
        }

        /** {@inheritDoc} */
        @Override public int getReads() {
            return reads.get();
        }

        /** {@inheritDoc} */
        @Override public int getWrites() {
            return writes.get();
        }

        /** {@inheritDoc} */
        @Override public int getRemoves() {
            return removes.get();
        }

        /** {@inheritDoc} */
        @Override public int getStoreSize() {
            return map.size();
        }

        /** {@inheritDoc} */
        @Override public void resetStore() {
            map.clear();

            reads.set(0);
            writes.set(0);
            removes.set(0);
        }

        /** {@inheritDoc} */
        @Override public void putToStore(Object key, Object val) {
            map.put(key, val);
        }

        /** {@inheritDoc} */
        @Override public void putAllToStore(Map<?, ?> data) {
            map.putAll(data);
        }

        /** {@inheritDoc} */
        @Override public Object getFromStore(Object key) {
            return map.get(key);
        }

        /** {@inheritDoc} */
        @Override public void removeFromStore(Object key) {
            map.remove(key);
        }

        /** {@inheritDoc} */
        @Override public boolean isInStore(Object key) {
            return map.containsKey(key);
        }

        @Override
        public void updateCacheConfiguration(CacheConfiguration<Object, Object> configuration) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Factory<? extends CacheStore<Object, Object>> getStoreFactory() {
            return FactoryBuilder.factoryOf(MapCacheStore.class);
        }

        /** Serializable {@link #map} backed cache store factory */
        private static class MapStoreFactory implements Factory<CacheStore<Object, Object>> {
            /** {@inheritDoc} */
            @Override public CacheStore<Object, Object> create() {
                return new MapCacheStore();
            }
        }

        /** {@link CacheStore} backed by {@link #map} */
        public static class MapCacheStore extends CacheStoreAdapter<Object, Object> {

            /** {@inheritDoc} */
            @Override public void loadCache(IgniteBiInClosure<Object, Object> clo, Object... args) {
                for (Map.Entry<Object, Object> e : map.entrySet())
                    clo.apply(e.getKey(), e.getValue());
            }

            /** {@inheritDoc} */
            @Override public Object load(Object key) {
                reads.incrementAndGet();
                return map.get(key);
            }

            /** {@inheritDoc} */
            @Override public void write(Cache.Entry<?, ?> e) {
                writes.incrementAndGet();
                map.put(e.getKey(), e.getValue());
            }

            /** {@inheritDoc} */
            @Override public void delete(Object key) {
                removes.incrementAndGet();
                map.remove(key);
            }
        }
    }

    protected static class H2CacheStoreStrategy implements TestCacheStoreStrategy {
        private final JdbcConnectionPool dataSrc;

        /** Create table script. */
        private static final String CREATE_CACHE_TABLE =
            "create table if not exists CACHE(k binary not null, v binary not null, PRIMARY KEY(k));";

        private static final String CREATE_STATS_TABLE =
            "create table if not exists STATS(id bigint not null, reads int not null, writes int not null, " +
                "removes int not null, PRIMARY KEY(id));";

        private static final String POPULATE_STATS_TABLE =
            "delete from STATS;\n" +
            "insert into STATS(id, reads, writes, removes) values(1, 0, 0, 0);";

        /** */
        public H2CacheStoreStrategy() throws IgniteCheckedException {
            try {
                Server.createTcpServer("-tcpDaemon").start();
                dataSrc = createDataSource();

                try (Connection conn = connection()) {
                    RunScript.execute(conn, new StringReader(CREATE_CACHE_TABLE));
                    RunScript.execute(conn, new StringReader(CREATE_STATS_TABLE));
                    RunScript.execute(conn, new StringReader(POPULATE_STATS_TABLE));
                }
            }
            catch (SQLException e) {
                throw new IgniteCheckedException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void afterTestsStopped() {

        }

        /** {@inheritDoc} */
        @Override public int getReads() {
            return querySingleInt("select reads from STATS limit 0,1;", "Failed to query number of reads from STATS table");
        }

        /** {@inheritDoc} */
        @Override public int getWrites() {
            return querySingleInt("select writes from STATS limit 0,1;", "Failed to query number of writes from STATS table");
        }

        /** {@inheritDoc} */
        @Override public int getRemoves() {
            return querySingleInt("select removes from STATS limit 0,1;", "Failed to query number of removals from STATS table");
        }

        /** {@inheritDoc} */
        @Override public int getStoreSize() {
            return querySingleInt("select count(*) from CACHE;", "Failed to query number of rows from CACHE table");
        }

        /** {@inheritDoc} */
        @Override public void resetStore() {
            try (Connection conn = connection()) {
                RunScript.execute(conn, new StringReader("delete from CACHE;"));
                RunScript.execute(conn, new StringReader(POPULATE_STATS_TABLE));
            }
            catch (SQLException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void putToStore(Object key, Object val) {
            try {
                H2CacheStore.putToDb(connection(), key, val);
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }

        /** {@inheritDoc} */
        @Override public void putAllToStore(Map<?, ?> data) {
            Connection conn = null;
            PreparedStatement stmt = null;
            try {
                conn = connection();
                stmt = conn.prepareStatement(H2CacheStore.INSERT);
                for (Map.Entry<?, ?> e : data.entrySet()) {
                    byte[] v = H2CacheStore.serialize(e.getValue());
                    stmt.setBinaryStream(1, new ByteArrayInputStream(H2CacheStore.serialize(e.getKey())));
                    stmt.setBinaryStream(2, new ByteArrayInputStream(v));
                    stmt.setBinaryStream(3, new ByteArrayInputStream(v));
                    stmt.addBatch();
                }
                stmt.executeBatch();
            }
            catch (SQLException e) {
                throw new IgniteException(e);
            }
            finally {
                H2CacheStore.end(stmt, conn);
            }
        }

        /** {@inheritDoc} */
        @Override public Object getFromStore(Object key) {
            try {
                return H2CacheStore.getFromDb(connection(), key);
            }
            catch (SQLException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void removeFromStore(Object key) {
            try {
                H2CacheStore.removeFromDb(connection(), key);
            }
            catch (SQLException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public boolean isInStore(Object key) {
            return getFromStore(key) != null;
        }

        private Connection connection() throws SQLException {
            return dataSrc.getConnection();
        }

        private int querySingleInt(String query, String errorMsg) {
            Connection conn = null;
            PreparedStatement stmt = null;
            try {
                conn = connection();
                stmt = conn.prepareStatement(query);
                ResultSet rs = stmt.executeQuery();
                if (rs.next())
                    return rs.getInt(1);
                else
                    throw new IgniteException(errorMsg);
            }
            catch (SQLException e) {
                throw new IgniteException(e);
            }
            finally {
                H2CacheStore.end(stmt, conn);
            }
        }

        /** */
        private static JdbcConnectionPool createDataSource() {
            return JdbcConnectionPool.create("jdbc:h2:tcp://localhost/mem:TestDb;mode=MySQL", "sa", "");
        }

        /** {@inheritDoc} */
        @Override public void updateCacheConfiguration(CacheConfiguration<Object, Object> configuration) {
            configuration.setCacheStoreSessionListenerFactories(new H2CacheStoreSessionListenerFactory());
        }

        /** {@inheritDoc} */
        @Override public Factory<? extends CacheStore<Object, Object>> getStoreFactory() {
            return new H2StoreFactory();
        }

    }

    /** Serializable H2 backed cache store factory */
    private static class H2StoreFactory implements Factory<CacheStore<Object, Object>> {
        /** {@inheritDoc} */
        @Override public CacheStore<Object, Object> create() {
            return new H2CacheStore();
        }
    }

    private static class H2CacheStoreSessionListenerFactory implements Factory<CacheStoreSessionListener> {
        @Override public CacheStoreSessionListener create() {
            CacheJdbcStoreSessionListener lsnr = new CacheJdbcStoreSessionListener();
            lsnr.setDataSource(JdbcConnectionPool.create("jdbc:h2:tcp://localhost/mem:TestDb;mode=MySQL;DB_CLOSE_DELAY=-1", "sa", ""));
            return lsnr;
        }
    }


    public static class H2CacheStore extends CacheStoreAdapter<Object, Object> {
        /** Store session */
        @CacheStoreSessionResource
        private CacheStoreSession ses;

        /** Template for an insert statement */
        private static final String INSERT =
            "insert into CACHE(k, v) values(?, ?) on duplicate key update v = ?;";

        /** {@inheritDoc} */
        @Override public Object load(Object key) throws CacheLoaderException {
            try {
                Object res = getFromDb(ses.attachment(), key);
                updateStats("reads");
                return res;
            }
            catch (SQLException e) {
                throw new CacheLoaderException("Failed to load object [key=" + key + ']', e);
            }
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<?, ?> entry) throws CacheWriterException {
            try {
                putToDb(ses.attachment(), entry.getKey(), entry.getValue());
                updateStats("writes");
            }
            catch (SQLException e) {
                throw new CacheWriterException("Failed to write object [key=" + entry.getKey() + ", " +
                    "val=" + entry.getValue() + ']', e);
            }
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            try {
                removeFromDb(ses.attachment(), key);
                updateStats("removes");
            }
            catch (SQLException e) {
                throw new CacheWriterException("Failed to delete object [key=" + key + ']', e);
            }
        }

        /**
         * Select from H2 and deserialize from bytes the value pointed at by <tt>key</tt>
         * @param conn {@link Connection} to use
         * @param key key to llok for the value by
         * @return Stored object or null if the key is missing from DB
         * @throws SQLException
         */
        static Object getFromDb(Connection conn, Object key) throws SQLException {
            PreparedStatement stmt = null;
            try {
                stmt = conn.prepareStatement("select v from CACHE where k = ?");
                stmt.setBinaryStream(1, new ByteArrayInputStream(H2CacheStore.serialize(key)));
                ResultSet rs = stmt.executeQuery();
                return rs.next() ? H2CacheStore.deserialize(IOUtils.toByteArray(rs.getBinaryStream(1))) : null;
            }
            catch (IOException e) {
                throw new IgniteException(e);
            }
            finally {
                end(stmt, conn);
            }
        }

        static void putToDb(Connection conn, Object key, Object val) throws SQLException {
            PreparedStatement stmt = null;
            try {
                stmt = conn.prepareStatement(H2CacheStore.INSERT);
                byte[] v = H2CacheStore.serialize(val);
                stmt.setBinaryStream(1, new ByteArrayInputStream(H2CacheStore.serialize(key)));
                stmt.setBinaryStream(2, new ByteArrayInputStream(v));
                stmt.setBinaryStream(3, new ByteArrayInputStream(v));
                stmt.executeUpdate();
            }
            finally {
                end(stmt, conn);
            }
        }

        static void removeFromDb(Connection conn, Object key) throws SQLException {
            PreparedStatement stmt = null;
            try {
                stmt = conn.prepareStatement("delete from CACHE where k = ?");
                stmt.setBinaryStream(1, new ByteArrayInputStream(H2CacheStore.serialize(key)));
                stmt.executeUpdate();
            }
            finally {
                end(stmt, conn);
            }
        }

        /**
         * Increment stored stats for given field
         * @param field field name
         */
        private void updateStats(String field) {
            Connection conn = ses.attachment();
            assert conn != null;
            Statement stmt = null;
            try {
                stmt = conn.createStatement();
                stmt.executeUpdate("update STATS set " + field + " = " + field + " + 1;");
            }
            catch (SQLException e) {
                throw new IgniteException("Failed to update H2 store usage stats", e);
            }
            finally {
                end(stmt, conn);
            }
        }

        /**
         * Quietly close statement and connection
         * @param stmt {@link Statement} to close
         * @param conn {@link Connection} to close
         */
        private static void end(Statement stmt, Connection conn) {
            U.closeQuiet(stmt);
            U.closeQuiet(conn);
        }

        /**
         * Turn given arbitrary {@link Object} to byte array
         * @param obj {@link Object} to serialize
         * @return bytes representation of given {@link Object}
         */
        static byte[] serialize(Object obj) {
            try (ByteArrayOutputStream b = new ByteArrayOutputStream()) {
                try (ObjectOutputStream o = new ObjectOutputStream(b)) {
                    o.writeObject(obj);
                }
                return b.toByteArray();
            }
            catch (Exception e) {
                throw new IgniteException("Failed to serialize object to byte array [obj=" + obj, e);
            }
        }

        /**
         * Deserialize an object from its byte array representation
         * @param bytes byte array representation of the {@link Object}
         * @return deserialized {@link Object}
         */
        public static Object deserialize(byte[] bytes) {
            try (ByteArrayInputStream b = new ByteArrayInputStream(bytes)) {
                try (ObjectInputStream o = new ObjectInputStream(b)) {
                    return o.readObject();
                }
            }
            catch (Exception e) {
                throw new IgniteException("Failed to deserialize object from byte array", e);
            }
        }
    }
}
