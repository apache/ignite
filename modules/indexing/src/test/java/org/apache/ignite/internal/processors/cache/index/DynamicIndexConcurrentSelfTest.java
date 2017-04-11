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
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitor;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.Nullable;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Concurrency tests for dynamic indexes.
 */
public class DynamicIndexConcurrentSelfTest extends DynamicIndexAbstractSelfTest {
    /** Blocker instances. */
    private static final ConcurrentHashMap<UUID, CountDownLatch> BLOCKERS = new ConcurrentHashMap<>();

    /** Latches to block certain index operations. */
    private static final ConcurrentHashMap<UUID, T2<CountDownLatch, AtomicBoolean>> BLOCKS = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        releaseBlockers();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration() {
        return super.cacheConfiguration();
    }

    /**
     * Test node join on pending operation.
     *
     * @throws Exception If failed.
     */
    public void testNodeJoinOnPendingOperation() throws Exception {
        Ignite srv1 = Ignition.start(serverConfiguration(1));
        Ignite srv2 = Ignition.start(serverConfiguration(2));

        srv1.getOrCreateCache(cacheConfiguration());

        put(srv1, 1);

        UUID blockerId = createBlocker();

        IgniteCache<BinaryObject, BinaryObject> cache = cache(srv1);

        IgniteFuture invokeFut = cache.invokeAsync(key(srv1, 1), new BlockerEntryProcessor(blockerId));

        QueryIndex idx = index(IDX_NAME, field(FIELD_NAME_1));

        queryProcessor(srv1).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false);

        System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    }

    /**
     * Create new blocker.
     *
     * @return Blocker ID.
     */
    public static UUID createBlocker() {
        UUID id = UUID.randomUUID();

        BLOCKERS.put(id, new CountDownLatch(1));

        return id;
    }

    /**
     * Release blockers.
     */
    public static void releaseBlockers() {
        Set<UUID> ids = new HashSet<>(BLOCKERS.keySet());

        for (UUID id : ids)
            unblock(id);
    }

    /**
     * Block the thread until unblock is called.
     *
     * @param blockerId Blocker ID.
     */
    public static void block(UUID blockerId) {
        CountDownLatch latch = BLOCKERS.get(blockerId);

        if (latch == null)
            throw new IllegalStateException("Latch doesn't exist: " + blockerId);

        try {
            latch.await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteException("Got interrupted!");
        }
    }

    /**
     * Unblock blocked thread.
     *
     * @param blockerId Blocker ID.
     */
    public static void unblock(UUID blockerId) {
        CountDownLatch latch = BLOCKERS.remove(blockerId);

        if (latch == null)
            throw new IllegalStateException("Latch doesn't exist: " + blockerId);

        latch.countDown();
    }

    /**
     * Blocker entry processor.
     */
    private static class BlockerEntryProcessor implements EntryProcessor<BinaryObject, BinaryObject, Void> {
        /** Blocker ID. */
        private final UUID blockerId;

        /**
         * Constructor.
         *
         * @param blockerId Blocker ID.
         */
        public BlockerEntryProcessor(UUID blockerId) {
            this.blockerId = blockerId;
        }

        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<BinaryObject, BinaryObject> entry, Object... args)
            throws EntryProcessorException {
            block(blockerId);

            return null;
        }
    }

    /**
     * Block indexing.
     *
     * @param node Node.
     */
    private static void blockIndexing(Ignite node) {
        UUID nodeId = ((IgniteEx)node).localNode().id();

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

            try {
                blocker.get1().await();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new IgniteException("Got interrupted!");
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
