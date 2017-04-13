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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitor;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;
import org.apache.ignite.internal.util.typedef.T2;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Concurrency tests for dynamic indexes.
 */
@SuppressWarnings("unchecked")
public class DynamicIndexConcurrentSelfTest extends DynamicIndexAbstractSelfTest {
    /** Latches to block certain index operations. */
    private static final ConcurrentHashMap<UUID, T2<CountDownLatch, AtomicBoolean>> BLOCKS = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        GridQueryProcessor.idxCls = BlockingIndexing.class;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        GridQueryProcessor.idxCls = null;

        for (T2<CountDownLatch, AtomicBoolean> block : BLOCKS.values())
            block.get1().countDown();

        BLOCKS.clear();

        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 5 * 60 * 1000L;
    }

    /**
     * Make sure that coordinator migrates correctly between nodes.
     *
     * @throws Exception If failed.
     */
    public void testCoordinatorChange() throws Exception {
        // Start servers.
        Ignite srv1 = Ignition.start(serverConfiguration(1));
        Ignite srv2 = Ignition.start(serverConfiguration(2));
        Ignition.start(serverConfiguration(3, true));
        Ignition.start(serverConfiguration(4));

        UUID srv1Id = srv1.cluster().localNode().id();
        UUID srv2Id = srv2.cluster().localNode().id();

        // Start client which will execute operations.
        Ignite cli = Ignition.start(clientConfiguration(5));

        cli.getOrCreateCache(cacheConfiguration());

        put(srv1, 0, KEY_AFTER);

        // Test migration between normal servers.
        blockIndexing(srv1Id);

        QueryIndex idx1 = index(IDX_NAME_1, field(FIELD_NAME_1));

        IgniteInternalFuture<?> idxFut1 = queryProcessor(cli).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx1, false);

        Thread.sleep(100);

        //srv1.close();
        Ignition.stop(srv1.name(), true);

        unblockIndexing(srv1Id);

        idxFut1.get();

        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1, field(FIELD_NAME_1));
        assertIndexUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_1, SQL_SIMPLE_ARG);
        assertSqlSimpleData(SQL_SIMPLE_FIELD_1, KEY_AFTER - SQL_SIMPLE_ARG);

        // Test migration from normal server to non-affinity server.
        blockIndexing(srv2Id);

        QueryIndex idx2 = index(IDX_NAME_2, field(alias(FIELD_NAME_2)));

        IgniteInternalFuture<?> idxFut2 = queryProcessor(cli).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx2, false);

        Thread.sleep(100);

        //srv2.close();
        Ignition.stop(srv2.name(), true);

        unblockIndexing(srv2Id);

        idxFut2.get();

        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_2, field(alias(FIELD_NAME_2)));
        assertIndexUsed(IDX_NAME_2, SQL_SIMPLE_FIELD_2, SQL_SIMPLE_ARG);
        assertSqlSimpleData(SQL_SIMPLE_FIELD_2, KEY_AFTER - SQL_SIMPLE_ARG);
    }

    /**
     * Test operations join.
     *
     * @throws Exception If failed.
     */
    public void testOperationJoin() throws Exception {
        Ignite srv1 = Ignition.start(serverConfiguration(1));

        Ignition.start(serverConfiguration(2));
        Ignition.start(serverConfiguration(3, true));
        Ignition.start(clientConfiguration(4));

        srv1.getOrCreateCache(cacheConfiguration());

        blockIndexing(srv1);

        QueryIndex idx1 = index(IDX_NAME_1, field(FIELD_NAME_1));
        QueryIndex idx2 = index(IDX_NAME_2, field(alias(FIELD_NAME_2)));

        IgniteInternalFuture<?> idxFut1 = queryProcessor(srv1).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx1, false);
        IgniteInternalFuture<?> idxFut2 = queryProcessor(srv1).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx2, false);

        // Start even more nodes of different flavors
        Ignition.start(serverConfiguration(5));
        Ignition.start(serverConfiguration(6, true));
        Ignition.start(clientConfiguration(7));

        assert !idxFut1.isDone();
        assert !idxFut2.isDone();

        unblockIndexing(srv1);

        idxFut1.get();
        idxFut2.get();

        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1, field(FIELD_NAME_1));
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_2, field(alias(FIELD_NAME_2)));

        put(srv1, 0, KEY_AFTER);

        assertIndexUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_1, SQL_SIMPLE_ARG);
        assertIndexUsed(IDX_NAME_2, SQL_SIMPLE_FIELD_2, SQL_SIMPLE_ARG);

        assertSqlSimpleData(SQL_SIMPLE_FIELD_1, KEY_AFTER - SQL_SIMPLE_ARG);
        assertSqlSimpleData(SQL_SIMPLE_FIELD_2, KEY_AFTER - SQL_SIMPLE_ARG);
    }

    /**
     * Test node join on pending operation.
     *
     * @throws Exception If failed.
     */
    public void testNodeJoinOnPendingOperation() throws Exception {
        Ignite srv1 = Ignition.start(serverConfiguration(1));

        srv1.getOrCreateCache(cacheConfiguration());

        blockIndexing(srv1);

        QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_1));

        IgniteInternalFuture<?> idxFut = queryProcessor(srv1).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false);

        Ignition.start(serverConfiguration(2));
        Ignition.start(serverConfiguration(3, true));
        Ignition.start(clientConfiguration(4));

        assert !idxFut.isDone();

        unblockIndexing(srv1);

        idxFut.get();

        Thread.sleep(100L);

        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1, field(FIELD_NAME_1));

        put(srv1, 0, KEY_AFTER);

        assertIndexUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_1, SQL_SIMPLE_ARG);
        assertSqlSimpleData(SQL_SIMPLE_FIELD_1, KEY_AFTER - SQL_SIMPLE_ARG);
    }

    /**
     * Check what happen in case cache is destroyed before operation is started.
     *
     * @throws Exception If failed.
     */
    public void testConcurrentCacheDestroy() throws Exception {
        // Start complex topology.
        Ignite srv1 = Ignition.start(serverConfiguration(1));

        Ignition.start(serverConfiguration(2));
        Ignition.start(serverConfiguration(3, true));

        Ignite cli = Ignition.start(clientConfiguration(4));

        // Start cache and populate it with data.
        IgniteCache cache = cli.getOrCreateCache(cacheConfiguration());

        put(cli, KEY_AFTER);

        // Start index operation and block it on coordinator.
        blockIndexing(srv1);

        QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_1));

        final IgniteInternalFuture<?> idxFut =
            queryProcessor(srv1).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false);

        Thread.sleep(100);

        // Destroy cache.
        cache.destroy();

        // Unblock indexing and see what happens.
        unblockIndexing(srv1);

        try {
            idxFut.get();

            fail("Ëxception has not been thrown.");
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
    public void testConcurrentOperationsMultithreaded() throws Exception {
        // Start complex topology.
        Ignition.start(serverConfiguration(1));
        Ignition.start(serverConfiguration(2));
        Ignition.start(serverConfiguration(3, true));

        Ignite cli = Ignition.start(clientConfiguration(4));

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
                        fut = queryProcessor(node).dynamicIndexDrop(CACHE_NAME, IDX_NAME_1, true);

                        exists = false;
                    }
                    else {
                        fut = queryProcessor(node).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, true);

                        exists = true;
                    }

                    try {
                        fut.get();
                    }
                    catch (SchemaOperationException e) {
                        log.info("Got operation exception (expected): " + e);
                    }
                    catch (Exception e) {
                        fail("Unexpected exception: " + e);
                    }
                }

                return null;
            }
        }, 8);

        // Let them play for 30 seconds.
        Thread.sleep(30_000);

        stopped.set(true);

        // Make sure nothing hanged.
        idxFut.get();

        // Make sure cache is operational at this point.
        cli.getOrCreateCache(cacheConfiguration());

        queryProcessor(cli).dynamicIndexDrop(CACHE_NAME, IDX_NAME_1, true).get();
        queryProcessor(cli).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, true).get();

        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1, field(FIELD_NAME_1));

        put(cli, 0, KEY_AFTER);

        assertIndexUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_1, SQL_SIMPLE_ARG);
        assertSqlSimpleData(SQL_SIMPLE_FIELD_1, KEY_AFTER - SQL_SIMPLE_ARG);
    }

    /**
     * Test concurrent node start/stop along with index operations. Nothing should hang.
     *
     * @throws Exception If failed.
     */
    public void testConcurrentOperationsAndNodeStartStopMultithreaded() throws Exception {
        // Start several stable nodes.
        Ignition.start(serverConfiguration(1));
        Ignition.start(serverConfiguration(2));
        Ignition.start(serverConfiguration(3, true));

        final Ignite cli = Ignition.start(clientConfiguration(4));

        cli.createCache(cacheConfiguration());

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

                        Ignition.start(cfg);

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
                        fut = queryProcessor(node).dynamicIndexDrop(CACHE_NAME, IDX_NAME_1, true);

                        exists = false;
                    }
                    else {
                        fut = queryProcessor(node).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, true);

                        exists = true;
                    }

                    try {
                        fut.get();
                    }
                    catch (SchemaOperationException e) {
                        log.info("Got operation exception (expected): " + e);
                    }
                    catch (Exception e) {
                        fail("Unexpected exception: " + e);
                    }
                }

                return null;
            }
        }, 1);

        // Let them play for 30 seconds.
        Thread.sleep(30_000);

        stopped.set(true);

        // Make sure nothing hanged.
        startStopFut.get();
        idxFut.get();

        // Make sure cache is operational at this point.
        cli.getOrCreateCache(cacheConfiguration());

        queryProcessor(cli).dynamicIndexDrop(CACHE_NAME, IDX_NAME_1, true).get();
        queryProcessor(cli).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, true).get();

        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1, field(FIELD_NAME_1));

        put(cli, 0, KEY_AFTER);

        assertIndexUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_1, SQL_SIMPLE_ARG);
        assertSqlSimpleData(SQL_SIMPLE_FIELD_1, KEY_AFTER - SQL_SIMPLE_ARG);
    }

    /**
     * Multithreaded cache start/stop along with index operations. Nothing should hang.
     *
     * @throws Exception If failed.
     */
    public void testConcurrentOperationsAndCacheStartStopMultithreaded() throws Exception {
        // Start complex topology.
        Ignition.start(serverConfiguration(1));
        Ignition.start(serverConfiguration(2));
        Ignition.start(serverConfiguration(3, true));

        Ignite cli = Ignition.start(clientConfiguration(4));

        final AtomicBoolean stopped = new AtomicBoolean();

        // Start cache create/destroy worker.
        IgniteInternalFuture startStopFut = multithreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                boolean exists = false;

                while (!stopped.get()) {
                    Ignite node = grid(ThreadLocalRandom.current().nextInt(1, 5));

                    if (exists) {
                        node.destroyCache(CACHE_NAME);

                        exists = false;
                    }
                    else {
                        node.createCache(cacheConfiguration());

                        exists = true;
                    }
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
                        fut = queryProcessor(node).dynamicIndexDrop(CACHE_NAME, IDX_NAME_1, true);

                        exists = false;
                    }
                    else {
                        fut = queryProcessor(node).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, true);

                        exists = true;
                    }

                    try {
                        fut.get();
                    }
                    catch (SchemaOperationException e) {
                        log.info("Got operation exception (expected): " + e);
                    }
                    catch (Exception e) {
                        fail("Unexpected exception: " + e);
                    }
                }

                return null;
            }
        }, 8);

        // Let them play for 30 seconds.
        Thread.sleep(30_000);

        stopped.set(true);

        // Make sure nothing hanged.
        startStopFut.get();
        idxFut.get();

        // Make sure cache is operational at this point.
        cli.getOrCreateCache(cacheConfiguration());

        queryProcessor(cli).dynamicIndexDrop(CACHE_NAME, IDX_NAME_1, true).get();
        queryProcessor(cli).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, true).get();

        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1, field(FIELD_NAME_1));

        put(cli, 0, KEY_AFTER);

        assertIndexUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_1, SQL_SIMPLE_ARG);
        assertSqlSimpleData(SQL_SIMPLE_FIELD_1, KEY_AFTER - SQL_SIMPLE_ARG);
    }

    /**
     * Block indexing.
     *
     * @param node Node.
     */
    @SuppressWarnings("SuspiciousMethodCalls")
    private static void blockIndexing(Ignite node) {
        UUID nodeId = ((IgniteEx)node).localNode().id();

        blockIndexing(nodeId);
    }

    /**
     * Block indexing.
     *
     * @param nodeId Node.
     */
    @SuppressWarnings("SuspiciousMethodCalls")
    private static void blockIndexing(UUID nodeId) {
        assertFalse(BLOCKS.contains(nodeId));

        BLOCKS.put(nodeId, new T2<>(new CountDownLatch(1), new AtomicBoolean()));
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
    private static void unblockIndexing(UUID nodeId) {
        T2<CountDownLatch, AtomicBoolean> blocker = BLOCKS.remove(nodeId);

        assertNotNull(blocker);

        blocker.get1().countDown();
    }

    /**
     * Await indexing.
     *
     * @param nodeId Node ID.
     */
    private static void awaitIndexing(UUID nodeId) {
        T2<CountDownLatch, AtomicBoolean> blocker = BLOCKS.get(nodeId);

        if (blocker != null) {
            assertTrue(blocker.get2().compareAndSet(false, true));

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
     * Blocking indexing processor.
     */
    private static class BlockingIndexing extends IgniteH2Indexing {
        /** {@inheritDoc} */
        @Override public void createIndex(@Nullable String spaceName, String tblName, QueryIndex idx,
            boolean ifNotExists, SchemaIndexCacheVisitor cacheVisitor) throws IgniteCheckedException {
            awaitIndexing(ctx.localNodeId());

            super.createIndex(spaceName, tblName, idx, ifNotExists, cacheVisitor);
        }

        /** {@inheritDoc} */
        @Override public void dropIndex(@Nullable String spaceName, String idxName, boolean ifExists) {
            awaitIndexing(ctx.localNodeId());

            super.dropIndex(spaceName, idxName, ifExists);
        }
    }
}
