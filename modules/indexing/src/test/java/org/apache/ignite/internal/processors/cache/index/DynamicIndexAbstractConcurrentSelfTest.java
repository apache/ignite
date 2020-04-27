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

package org.apache.ignite.internal.processors.cache.index;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteClientReconnectAbstractTest;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.QueryIndexDescriptorImpl;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitor;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.apache.ignite.internal.IgniteClientReconnectAbstractTest.TestTcpDiscoverySpi;
import static org.apache.ignite.testframework.GridTestUtils.RunnableX;

/**
 * Concurrency tests for dynamic index create/drop.
 */
@SuppressWarnings("unchecked")
public abstract class DynamicIndexAbstractConcurrentSelfTest extends DynamicIndexAbstractSelfTest {
    /** Test duration. */
    private static final long TEST_DUR = 10_000L;

    /** Large cache size. */
    private static final int LARGE_CACHE_SIZE = 100_000;

    /** Latches to block certain index operations. */
    private static final ConcurrentHashMap<UUID, T3<CountDownLatch, AtomicBoolean, CountDownLatch>> BLOCKS =
        new ConcurrentHashMap<>();

    /** Cache mode. */
    private final CacheMode cacheMode;

    /** Atomicity mode. */
    private final CacheAtomicityMode atomicityMode;

    /**
     * Constructor.
     *
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     */
    DynamicIndexAbstractConcurrentSelfTest(CacheMode cacheMode, CacheAtomicityMode atomicityMode) {
        this.cacheMode = cacheMode;
        this.atomicityMode = atomicityMode;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ConstantConditions")
    @Override protected void afterTest() throws Exception {
        GridQueryProcessor.idxCls = null;

        for (T3<CountDownLatch, AtomicBoolean, CountDownLatch> block : BLOCKS.values())
            block.get1().countDown();

        BLOCKS.clear();

        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 5 * 60 * 1000L;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration<KeyClass, ValueClass> cacheConfiguration() {
        CacheConfiguration<KeyClass, ValueClass> ccfg = super.cacheConfiguration();

        return ccfg.setCacheMode(cacheMode).setAtomicityMode(atomicityMode);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration commonConfiguration(int idx) throws Exception {
        IgniteConfiguration cfg = super.commonConfiguration(idx);

        TestTcpDiscoverySpi testSpi = new TestTcpDiscoverySpi();

        if (cfg.getDiscoverySpi() instanceof TcpDiscoverySpi &&
            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).getIpFinder() != null)
            testSpi.setIpFinder(((TcpDiscoverySpi)cfg.getDiscoverySpi()).getIpFinder());

        return cfg.setDiscoverySpi(testSpi);
    }

    /**
     * Make sure that coordinator migrates correctly between nodes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCoordinatorChange() throws Exception {
        // Start servers.
        Ignite srv1 = ignitionStart(serverConfiguration(1));
        Ignite srv2 = ignitionStart(serverConfiguration(2));
        ignitionStart(serverConfiguration(3, true));
        ignitionStart(serverConfiguration(4));

        UUID srv1Id = srv1.cluster().localNode().id();
        UUID srv2Id = srv2.cluster().localNode().id();

        // Start client which will execute operations.
        Ignite cli = ignitionStart(clientConfiguration(5));

        createSqlCache(cli);

        put(srv1, 0, KEY_AFTER);

        // Test migration between normal servers.
        CountDownLatch idxLatch = blockIndexing(srv1Id);

        QueryIndex idx1 = index(IDX_NAME_1, field(FIELD_NAME_1));

        IgniteInternalFuture<?> idxFut1 = queryProcessor(cli).dynamicIndexCreate(CACHE_NAME, CACHE_NAME, TBL_NAME,
            idx1, false, 0);

        idxLatch.await();

        //srv1.close();
        Ignition.stop(srv1.name(), true);

        unblockIndexing(srv1Id);

        idxFut1.get();

        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1, QueryIndex.DFLT_INLINE_SIZE, field(FIELD_NAME_1));
        assertIndexUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_1, SQL_ARG_1);
        assertSqlSimpleData(SQL_SIMPLE_FIELD_1, KEY_AFTER - SQL_ARG_1);

        // Test migration from normal server to non-affinity server.
        idxLatch = blockIndexing(srv2Id);

        QueryIndex idx2 = index(IDX_NAME_2, field(aliasUnescaped(FIELD_NAME_2)));

        IgniteInternalFuture<?> idxFut2 =
            queryProcessor(cli).dynamicIndexCreate(CACHE_NAME, CACHE_NAME, TBL_NAME, idx2, false, 0);

        idxLatch.await();

        //srv2.close();
        Ignition.stop(srv2.name(), true);

        unblockIndexing(srv2Id);

        idxFut2.get();

        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_2, QueryIndex.DFLT_INLINE_SIZE, field(aliasUnescaped(FIELD_NAME_2)));
        assertIndexUsed(IDX_NAME_2, SQL_SIMPLE_FIELD_2, SQL_ARG_1);
        assertSqlSimpleData(SQL_SIMPLE_FIELD_2, KEY_AFTER - SQL_ARG_1);
    }

    /**
     * Test operations join.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testOperationChaining() throws Exception {
        Ignite srv1 = ignitionStart(serverConfiguration(1));

        ignitionStart(serverConfiguration(2));
        ignitionStart(serverConfiguration(3, true));
        ignitionStart(clientConfiguration(4));

        createSqlCache(srv1);

        CountDownLatch idxLatch = blockIndexing(srv1);

        QueryIndex idx1 = index(IDX_NAME_1, field(FIELD_NAME_1));
        QueryIndex idx2 = index(IDX_NAME_2, field(aliasUnescaped(FIELD_NAME_2)));

        IgniteInternalFuture<?> idxFut1 =
            queryProcessor(srv1).dynamicIndexCreate(CACHE_NAME, CACHE_NAME, TBL_NAME, idx1, false, 0);

        IgniteInternalFuture<?> idxFut2 =
            queryProcessor(srv1).dynamicIndexCreate(CACHE_NAME, CACHE_NAME, TBL_NAME, idx2, false, 0);

        idxLatch.await();

        // Start even more nodes of different flavors
        ignitionStart(serverConfiguration(5));
        ignitionStart(serverConfiguration(6, true));
        ignitionStart(clientConfiguration(7));

        assert !idxFut1.isDone();
        assert !idxFut2.isDone();

        unblockIndexing(srv1);

        idxFut1.get();
        idxFut2.get();

        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1, QueryIndex.DFLT_INLINE_SIZE, field(FIELD_NAME_1));
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_2, QueryIndex.DFLT_INLINE_SIZE, field(aliasUnescaped(FIELD_NAME_2)));

        put(srv1, 0, KEY_AFTER);

        assertIndexUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_1, SQL_ARG_1);
        assertIndexUsed(IDX_NAME_2, SQL_SIMPLE_FIELD_2, SQL_ARG_1);

        assertSqlSimpleData(SQL_SIMPLE_FIELD_1, KEY_AFTER - SQL_ARG_1);
        assertSqlSimpleData(SQL_SIMPLE_FIELD_2, KEY_AFTER - SQL_ARG_1);
    }

    /**
     * Test node join on pending operation.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNodeJoinOnPendingOperation() throws Exception {
        Ignite srv1 = ignitionStart(serverConfiguration(1));

        createSqlCache(srv1);

        CountDownLatch idxLatch = blockIndexing(srv1);

        QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_1));

        IgniteInternalFuture<?> idxFut =
            queryProcessor(srv1).dynamicIndexCreate(CACHE_NAME, CACHE_NAME, TBL_NAME, idx, false, 0);

        idxLatch.await();

        ignitionStart(serverConfiguration(2));
        ignitionStart(serverConfiguration(3, true));
        ignitionStart(clientConfiguration(4));

        assert !idxFut.isDone();

        unblockIndexing(srv1);

        idxFut.get();

        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1, QueryIndex.DFLT_INLINE_SIZE, field(FIELD_NAME_1));

        put(srv1, 0, KEY_AFTER);

        assertIndexUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_1, SQL_ARG_1);
        assertSqlSimpleData(SQL_SIMPLE_FIELD_1, KEY_AFTER - SQL_ARG_1);
    }

    /**
     * PUT/REMOVE data from cache and build index concurrently.
     *
     * @throws Exception If failed,
     */
    @Test
    public void testConcurrentPutRemove() throws Exception {
        // Start several nodes.
        Ignite srv1 = ignitionStart(serverConfiguration(1));
        ignitionStart(serverConfiguration(2));
        ignitionStart(serverConfiguration(3));
        ignitionStart(serverConfiguration(4));

        awaitPartitionMapExchange();

        IgniteCache<BinaryObject, BinaryObject> cache = createSqlCache(srv1).withKeepBinary();

        // Start data change operations from several threads.
        final AtomicBoolean stopped = new AtomicBoolean();

        IgniteInternalFuture updateFut = multithreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                while (!stopped.get()) {
                    Ignite node = grid(ThreadLocalRandom.current().nextInt(1, 5));

                    int key = ThreadLocalRandom.current().nextInt(0, LARGE_CACHE_SIZE);
                    int val = ThreadLocalRandom.current().nextInt();

                    BinaryObject keyObj = key(node, key);

                    if (ThreadLocalRandom.current().nextBoolean()) {
                        BinaryObject valObj = value(node, val);

                        node.cache(CACHE_NAME).put(keyObj, valObj);
                    }
                    else
                        node.cache(CACHE_NAME).remove(keyObj);
                }

                return null;
            }
        }, 4);

        // Let some to arrive.
        Thread.sleep(500L);

        // Create index.
        QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_1));

        queryProcessor(srv1).dynamicIndexCreate(CACHE_NAME, CACHE_NAME, TBL_NAME, idx, false, 0).get();

        // Stop updates once index is ready.
        stopped.set(true);

        updateFut.get();

        // Make sure index is there.
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1, QueryIndex.DFLT_INLINE_SIZE, field(FIELD_NAME_1));
        assertIndexUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_1, SQL_ARG_1);

        // Get expected values.
        Map<Long, Long> expKeys = new HashMap<>();

        for (int i = 0; i < LARGE_CACHE_SIZE; i++) {
            BinaryObject val = cache.get(key(srv1, i));

            if (val != null) {
                long fieldVal = val.field(FIELD_NAME_1);

                if (fieldVal >= SQL_ARG_1)
                    expKeys.put((long)i, fieldVal);
            }
        }

        // Validate query result.
        for (Ignite node : Ignition.allGrids()) {
            IgniteCache<BinaryObject, BinaryObject> nodeCache = node.cache(CACHE_NAME).withKeepBinary();

            SqlQuery qry = new SqlQuery(typeName(ValueClass.class), SQL_SIMPLE_FIELD_1).setArgs(SQL_ARG_1);

            List<Cache.Entry<BinaryObject, BinaryObject>> res = nodeCache.query(qry).getAll();

            assertEquals("Cache size mismatch [exp=" + expKeys.size() + ", actual=" + res.size() + ']',
                expKeys.size(), res.size());

            for (Cache.Entry<BinaryObject, BinaryObject> entry : res) {
                long key = entry.getKey().field(FIELD_KEY);
                Long fieldVal = entry.getValue().field(FIELD_NAME_1);

                assertTrue("Expected key is not in result set: " + key, expKeys.containsKey(key));

                assertEquals("Unexpected value [key=" + key + ", expVal=" + expKeys.get(key) +
                    ", actualVal=" + fieldVal + ']', expKeys.get(key), fieldVal);
            }

        }
    }

    /**
     * Test index consistency on re-balance.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentRebalance() throws Exception {
        // Start cache and populate it with data.
        Ignite srv1 = ignitionStart(serverConfiguration(1));
        Ignite srv2 = ignitionStart(serverConfiguration(2));

        createSqlCache(srv1);

        awaitPartitionMapExchange();

        put(srv1, 0, LARGE_CACHE_SIZE);

        // Start index operation in blocked state.
        CountDownLatch idxLatch1 = blockIndexing(srv1);
        CountDownLatch idxLatch2 = blockIndexing(srv2);

        QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_1));

        final IgniteInternalFuture<?> idxFut =
            queryProcessor(srv1).dynamicIndexCreate(CACHE_NAME, CACHE_NAME, TBL_NAME, idx, false, 0);

        idxLatch1.await();
        idxLatch2.await();

        // Start two more nodes and unblock index operation in the middle.
        ignitionStart(serverConfiguration(3));

        unblockIndexing(srv1);
        unblockIndexing(srv2);

        ignitionStart(serverConfiguration(4));

        awaitPartitionMapExchange();

        // Validate index state.
        idxFut.get();

        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1, QueryIndex.DFLT_INLINE_SIZE, field(FIELD_NAME_1));

        assertIndexUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_1, SQL_ARG_1);
        assertSqlSimpleData(SQL_SIMPLE_FIELD_1, LARGE_CACHE_SIZE - SQL_ARG_1);
    }

    /**
     * Check what happen in case cache is destroyed before operation is started.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentCacheDestroy() throws Exception {
        // Start complex topology.
        Ignite srv1 = ignitionStart(serverConfiguration(1));

        ignitionStart(serverConfiguration(2));
        ignitionStart(serverConfiguration(3, true));

        Ignite cli = ignitionStart(clientConfiguration(4));

        // Start cache and populate it with data.
        createSqlCache(cli);

        put(cli, KEY_AFTER);

        // Start index operation and block it on coordinator.
        CountDownLatch idxLatch = blockIndexing(srv1);

        QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_1));

        final IgniteInternalFuture<?> idxFut =
            queryProcessor(srv1).dynamicIndexCreate(CACHE_NAME, CACHE_NAME, TBL_NAME, idx, false, 0);

        idxLatch.await();

        // Destroy cache (drop table).
        destroySqlCache(cli);

        // Unblock indexing and see what happens.
        unblockIndexing(srv1);

        try {
            idxFut.get();

            fail("Exception has not been thrown.");
        }
        catch (SchemaOperationException e) {
            // No-op.
        }
    }

    /**
     * Make sure that contended operations on the same index from different nodes do not hang.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentOperationsMultithreaded() throws Exception {
        // Start complex topology.
        ignitionStart(serverConfiguration(1));
        ignitionStart(serverConfiguration(2));
        ignitionStart(serverConfiguration(3, true));

        Ignite cli = ignitionStart(clientConfiguration(4));

        createSqlCache(cli);

        final AtomicBoolean stopped = new AtomicBoolean();

        // Start several threads which will mess around indexes.
        final QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_1));

        IgniteInternalFuture idxFut = multithreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                boolean exists = false;

                while (!stopped.get()) {
                    Ignite node = grid(ThreadLocalRandom.current().nextInt(1, 5));

                    IgniteInternalFuture fut;

                    if (exists) {
                        fut = queryProcessor(node).dynamicIndexDrop(CACHE_NAME, CACHE_NAME, IDX_NAME_1, true);

                        exists = false;
                    }
                    else {
                        fut = queryProcessor(node).dynamicIndexCreate(CACHE_NAME, CACHE_NAME, TBL_NAME, idx, true, 0);

                        exists = true;
                    }

                    try {
                        fut.get();
                    }
                    catch (SchemaOperationException e) {
                        // No-op.
                    }
                    catch (Exception e) {
                        fail("Unexpected exception: " + e);
                    }
                }

                return null;
            }
        }, 8);

        Thread.sleep(TEST_DUR);

        stopped.set(true);

        // Make sure nothing hanged.
        idxFut.get();

        queryProcessor(cli).dynamicIndexDrop(CACHE_NAME, CACHE_NAME, IDX_NAME_1, true).get();
        queryProcessor(cli).dynamicIndexCreate(CACHE_NAME, CACHE_NAME, TBL_NAME, idx, true, 0).get();

        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1, QueryIndex.DFLT_INLINE_SIZE, field(FIELD_NAME_1));

        put(cli, 0, KEY_AFTER);

        assertIndexUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_1, SQL_ARG_1);
        assertSqlSimpleData(SQL_SIMPLE_FIELD_1, KEY_AFTER - SQL_ARG_1);
    }

    /**
     * Make sure that contended operations on the same index from different nodes do not hang when we issue both
     * CREATE/DROP and SELECT statements.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testQueryConsistencyMultithreaded() throws Exception {
        // Start complex topology.
        ignitionStart(serverConfiguration(1));
        ignitionStart(serverConfiguration(2));
        ignitionStart(serverConfiguration(3, true));

        Ignite cli = ignitionStart(clientConfiguration(4));

        createSqlCache(cli);

        put(cli, 0, KEY_AFTER);

        final AtomicBoolean stopped = new AtomicBoolean();

        // Thread which will mess around indexes.
        final QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_1));

        IgniteInternalFuture idxFut = multithreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                boolean exists = false;

                while (!stopped.get()) {
                    Ignite node = grid(ThreadLocalRandom.current().nextInt(1, 5));

                    IgniteInternalFuture fut;

                    if (exists) {
                        fut = queryProcessor(node).dynamicIndexDrop(CACHE_NAME, CACHE_NAME, IDX_NAME_1, true);

                        exists = false;
                    }
                    else {
                        fut = queryProcessor(node).dynamicIndexCreate(CACHE_NAME, CACHE_NAME, TBL_NAME, idx, true, 0);

                        exists = true;
                    }

                    try {
                        fut.get();
                    }
                    catch (SchemaOperationException e) {
                        // No-op.
                    }
                    catch (Exception e) {
                        fail("Unexpected exception: " + e);
                    }
                }

                return null;
            }
        }, 1);

        IgniteInternalFuture qryFut = multithreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                while (!stopped.get()) {
                    Ignite node = grid(ThreadLocalRandom.current().nextInt(1, 5));

                    try {
                        assertSqlSimpleData(node, SQL_SIMPLE_FIELD_1, KEY_AFTER - SQL_ARG_1);
                    }
                    catch (Exception e) {
                        awaitConcDestroyException(e);
                    }
                }

                return null;
            }
        }, 8);

        Thread.sleep(TEST_DUR);

        stopped.set(true);

        // Make sure nothing hanged.
        idxFut.get();
        qryFut.get();
    }

    /**
     * Make sure that client receives schema changes made while it was disconnected.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnect() throws Exception {
        checkClientReconnect(false);
    }

    /**
     * Make sure that client receives schema changes made while it was disconnected, even with cache recreation.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnectWithCacheRestart() throws Exception {
        checkClientReconnect(true);
    }

    /**
     * Make sure that client receives schema changes made while it was disconnected, optionally with cache restart
     * in the interim.
     *
     * @param restartCache Whether cache needs to be recreated during client's absence.
     * @throws Exception If failed.
     */
    private void checkClientReconnect(final boolean restartCache) throws Exception {
        // Start complex topology.
        final Ignite srv = ignitionStart(serverConfiguration(1));
        ignitionStart(serverConfiguration(2));
        ignitionStart(serverConfiguration(3, true));

        final Ignite cli = ignitionStart(clientConfiguration(4));

        createSqlCache(cli);

        // Check index create.
        reconnectClientNode(srv, cli, restartCache, new RunnableX() {
            @Override public void runx() throws Exception {
                final QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_1));

                queryProcessor(srv).dynamicIndexCreate(CACHE_NAME, CACHE_NAME, TBL_NAME, idx, false, 0).get();
            }
        });

        assertIndex(cli, CACHE_NAME, TBL_NAME, IDX_NAME_1,
            QueryIndex.DFLT_INLINE_SIZE, field(FIELD_NAME_1));
        assertIndexUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_1, SQL_ARG_1);

        // Check index drop.
        reconnectClientNode(srv, cli, restartCache, new RunnableX() {
            @Override public void runx() throws Exception {
                if (!restartCache)
                    queryProcessor(srv).dynamicIndexDrop(CACHE_NAME, CACHE_NAME, IDX_NAME_1, false).get();
            }
        });

        assertNoIndex(cli, CACHE_NAME, TBL_NAME, IDX_NAME_1);
        assertIndexNotUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_1, SQL_ARG_1);

        // Update existing index.
        QueryIndex idx = index(IDX_NAME_2, field(aliasUnescaped(FIELD_NAME_2)));

        queryProcessor(srv).dynamicIndexCreate(CACHE_NAME, CACHE_NAME, TBL_NAME, idx, false, 0).get();

        assertIndex(cli, CACHE_NAME, TBL_NAME, IDX_NAME_2, QueryIndex.DFLT_INLINE_SIZE,
            field(aliasUnescaped(FIELD_NAME_2)));

        assertIndexUsed(IDX_NAME_2, SQL_SIMPLE_FIELD_2, SQL_ARG_2);

        reconnectClientNode(srv, cli, restartCache, new RunnableX() {
            @Override public void runx() throws Exception {
                if (!restartCache)
                    queryProcessor(srv).dynamicIndexDrop(CACHE_NAME, CACHE_NAME, IDX_NAME_2, false).get();

                final QueryIndex idx = index(IDX_NAME_2, field(FIELD_NAME_1), field(aliasUnescaped(FIELD_NAME_2)));

                queryProcessor(srv).dynamicIndexCreate(CACHE_NAME, CACHE_NAME, TBL_NAME, idx, false, 0).get();
            }
        });

        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_2, QueryIndex.DFLT_INLINE_SIZE, field(FIELD_NAME_1),
            field(aliasUnescaped(FIELD_NAME_2)));
        assertIndexUsed(IDX_NAME_2, SQL_COMPOSITE, SQL_ARG_1, SQL_ARG_2);
    }

    /**
     * Reconnect the client and run specified actions while it's out.
     *
     * @param srvNode Server node.
     * @param cliNode Client node.
     * @param restart Whether cache has to be recreated prior to executing required actions.
     * @param clo Closure to run
     * @throws Exception If failed.
     */
    private void reconnectClientNode(final Ignite srvNode, final Ignite cliNode, final boolean restart,
        final Runnable clo) throws Exception {
        IgniteClientReconnectAbstractTest.reconnectClientNode(log, cliNode, srvNode, new Runnable() {
            @Override public void run() {
                if (restart) {
                    try {
                        destroySqlCache(srvNode);

                        createSqlCache(srvNode);
                    }
                    catch (IgniteCheckedException e) {
                        throw new AssertionError(e);
                    }
                }

                try {
                    clo.run();
                }
                catch (Exception e) {
                    throw new IgniteException("Test reconnect runnable failed.", e);
                }
            }
        });

        if (restart)
            cliNode.cache(CACHE_NAME);
    }

    /**
     * Test concurrent node start/stop along with index operations. Nothing should hang.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentOperationsAndNodeStartStopMultithreaded() throws Exception {
        // Start several stable nodes.
        ignitionStart(serverConfiguration(1));
        ignitionStart(serverConfiguration(2));
        ignitionStart(serverConfiguration(3, true));

        final Ignite cli = ignitionStart(clientConfiguration(4));

        createSqlCache(cli);

        final AtomicBoolean stopped = new AtomicBoolean();

        // Start node start/stop worker.
        final AtomicInteger nodeIdx = new AtomicInteger(4);

        IgniteInternalFuture startStopFut = multithreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                boolean exists = false;

                int lastIdx = 0;

                while (!stopped.get()) {
                    if (exists) {
                        stopGrid(lastIdx);

                        exists = false;
                    }
                    else {
                        lastIdx = nodeIdx.incrementAndGet();

                        IgniteConfiguration cfg;

                        switch (ThreadLocalRandom.current().nextInt(0, 3)) {
                            case 1:
                                cfg = serverConfiguration(lastIdx, false);

                                break;

                            case 2:

                                cfg = serverConfiguration(lastIdx, true);

                                break;

                            default:
                                cfg = clientConfiguration(lastIdx);
                        }

                        ignitionStart(cfg);

                        exists = true;
                    }

                    Thread.sleep(ThreadLocalRandom.current().nextLong(500L, 1500L));
                }

                return null;
            }
        }, 1);

        // Start several threads which will mess around indexes.
        final QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_1));

        IgniteInternalFuture idxFut = multithreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                boolean exists = false;

                while (!stopped.get()) {
                    Ignite node = grid(ThreadLocalRandom.current().nextInt(1, 5));

                    IgniteInternalFuture fut;

                    if (exists) {
                        fut = queryProcessor(node).dynamicIndexDrop(CACHE_NAME, CACHE_NAME, IDX_NAME_1, true);

                        exists = false;
                    }
                    else {
                        fut = queryProcessor(node).dynamicIndexCreate(CACHE_NAME, CACHE_NAME, TBL_NAME, idx, true, 0);

                        exists = true;
                    }

                    try {
                        fut.get();
                    }
                    catch (SchemaOperationException e) {
                        // No-op.
                    }
                    catch (Exception e) {
                        fail("Unexpected exception: " + e);
                    }
                }

                return null;
            }
        }, 1);

        Thread.sleep(TEST_DUR);

        stopped.set(true);

        // Make sure nothing hanged.
        startStopFut.get();
        idxFut.get();

        // Make sure cache is operational at this point.
        createSqlCache(cli);

        queryProcessor(cli).dynamicIndexDrop(CACHE_NAME, CACHE_NAME, IDX_NAME_1, true).get();
        queryProcessor(cli).dynamicIndexCreate(CACHE_NAME, CACHE_NAME, TBL_NAME, idx, true, 0).get();

        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1, QueryIndex.DFLT_INLINE_SIZE, field(FIELD_NAME_1));

        put(cli, 0, KEY_AFTER);

        assertIndexUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_1, SQL_ARG_1);
        assertSqlSimpleData(SQL_SIMPLE_FIELD_1, KEY_AFTER - SQL_ARG_1);
    }

    /**
     * Multithreaded cache start/stop along with index operations. Nothing should hang.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentOperationsAndCacheStartStopMultithreaded() throws Exception {
        // Start complex topology.
        ignitionStart(serverConfiguration(1));
        ignitionStart(serverConfiguration(2));
        ignitionStart(serverConfiguration(3, true));

        Ignite cli = ignitionStart(clientConfiguration(4));

        final AtomicBoolean stopped = new AtomicBoolean();

        // Start cache create/destroy worker.
        IgniteInternalFuture startStopFut = multithreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                boolean exists = false;

                while (!stopped.get()) {
                    Ignite node = grid(ThreadLocalRandom.current().nextInt(1, 5));

                    if (exists) {
                        destroySqlCache(node);

                        exists = false;
                    }
                    else {
                        createSqlCache(node);

                        exists = true;
                    }

                    Thread.sleep(ThreadLocalRandom.current().nextLong(200L, 400L));
                }

                return null;
            }
        }, 1);

        // Start several threads which will mess around indexes.
        final QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_1));

        IgniteInternalFuture idxFut = multithreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                boolean exists = false;

                while (!stopped.get()) {
                    Ignite node = grid(ThreadLocalRandom.current().nextInt(1, 5));

                    IgniteInternalFuture fut;

                    if (exists) {
                        fut = queryProcessor(node).dynamicIndexDrop(CACHE_NAME, CACHE_NAME, IDX_NAME_1, true);

                        exists = false;
                    }
                    else {
                        fut = queryProcessor(node).dynamicIndexCreate(CACHE_NAME, CACHE_NAME, TBL_NAME, idx, true, 0);

                        exists = true;
                    }

                    try {
                        fut.get();
                    }
                    catch (SchemaOperationException e) {
                        // No-op.
                    }
                    catch (Exception e) {
                        fail("Unexpected exception: " + e);
                    }
                }

                return null;
            }
        }, 8);

        Thread.sleep(TEST_DUR);

        stopped.set(true);

        // Make sure nothing hanged.
        startStopFut.get();
        idxFut.get();

        // Make sure cache is operational at this point.
        createSqlCache(cli);

        queryProcessor(cli).dynamicIndexDrop(CACHE_NAME, CACHE_NAME, IDX_NAME_1, true).get();
        queryProcessor(cli).dynamicIndexCreate(CACHE_NAME, CACHE_NAME, TBL_NAME, idx, true, 0).get();

        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1, QueryIndex.DFLT_INLINE_SIZE, field(FIELD_NAME_1));

        put(cli, 0, KEY_AFTER);

        assertIndexUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_1, SQL_ARG_1);
        assertSqlSimpleData(SQL_SIMPLE_FIELD_1, KEY_AFTER - SQL_ARG_1);
    }

    /**
     * Block indexing.
     *
     * @param node Node.
     */
    private static CountDownLatch blockIndexing(Ignite node) {
        UUID nodeId = ((IgniteEx)node).localNode().id();

        return blockIndexing(nodeId);
    }

    /**
     * Block indexing.
     *
     * @param nodeId Node.
     */
    @SuppressWarnings("SuspiciousMethodCalls")
    private static CountDownLatch blockIndexing(UUID nodeId) {
        assertFalse(BLOCKS.contains(nodeId));

        CountDownLatch idxLatch = new CountDownLatch(1);

        BLOCKS.put(nodeId, new T3<>(new CountDownLatch(1), new AtomicBoolean(), idxLatch));

        return idxLatch;
    }

    /**
     * Unblock indexing.
     *
     * @param node Node.
     */
    private static void unblockIndexing(Ignite node) {
        UUID nodeId = ((IgniteEx)node).localNode().id();

        unblockIndexing(nodeId);
    }

    /**
     * Unblock indexing.
     *
     * @param nodeId Node ID.
     */
    @SuppressWarnings("ConstantConditions")
    private static void unblockIndexing(UUID nodeId) {
        T3<CountDownLatch, AtomicBoolean, CountDownLatch> blocker = BLOCKS.remove(nodeId);

        assertNotNull(blocker);

        blocker.get1().countDown();
    }

    /**
     * Await indexing.
     *
     * @param nodeId Node ID.
     */
    @SuppressWarnings("ConstantConditions")
    private static void awaitIndexing(UUID nodeId) {
        T3<CountDownLatch, AtomicBoolean, CountDownLatch> blocker = BLOCKS.get(nodeId);

        if (blocker != null) {
            assertTrue(blocker.get2().compareAndSet(false, true));

            blocker.get3().countDown();

            while (true) {
                try {
                    blocker.get1().await();

                    break;
                }
                catch (InterruptedException e) {
                    // No-op.
                }
            }
        }
    }

    /**
     * Get unescaped field alias.
     *
     * @param field Field.
     * @return Alias.
     */
    private static String aliasUnescaped(String field) {
        return alias(field).toUpperCase();
    }

    /**
     * Blocking indexing processor.
     */
    private static class BlockingIndexing extends IgniteH2Indexing {
        /** {@inheritDoc} */
        @Override public void dynamicIndexCreate(@NotNull String schemaName, String tblName,
            QueryIndexDescriptorImpl idxDesc, boolean ifNotExists, SchemaIndexCacheVisitor cacheVisitor)
            throws IgniteCheckedException {
            awaitIndexing(ctx.localNodeId());

            super.dynamicIndexCreate(schemaName, tblName, idxDesc, ifNotExists, cacheVisitor);
        }

        /** {@inheritDoc} */
        @Override public void dynamicIndexDrop(@NotNull String schemaName, String idxName, boolean ifExists)
            throws IgniteCheckedException {
            awaitIndexing(ctx.localNodeId());

            super.dynamicIndexDrop(schemaName, idxName, ifExists);
        }
    }

    /**
     * Start SQL cache on given node.
     * @param node Node to create cache on.
     * @return Created cache.
     */
    private IgniteCache<?, ?> createSqlCache(Ignite node) throws IgniteCheckedException {
        return createSqlCache(node, cacheConfiguration());
    }

    /**
     * Start a node.
     *
     * @param cfg Configuration.
     * @return Ignite instance.
     */
    private static Ignite ignitionStart(IgniteConfiguration cfg) {
        GridQueryProcessor.idxCls = BlockingIndexing.class;

        return Ignition.start(cfg);
    }
}
