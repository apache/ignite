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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import junit.framework.TestCase;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.QueryIndexDescriptorImpl;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitor;
import org.apache.ignite.internal.util.typedef.T3;
import org.jetbrains.annotations.NotNull;

/**
 * Blocking indexing processor.
 */
class BlockingIndexing extends IgniteH2Indexing {
    /** Latches to block certain index operations. */
    private static final Map<UUID, T3<CountDownLatch, AtomicBoolean, CountDownLatch>> BLOCKS = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public void dynamicIndexCreate(@NotNull String schemaName, String tblName,
        QueryIndexDescriptorImpl idxDesc, boolean ifNotExists, SchemaIndexCacheVisitor cacheVisitor)
        throws IgniteCheckedException {
        await(ctx.localNodeId());

        super.dynamicIndexCreate(schemaName, tblName, idxDesc, ifNotExists, cacheVisitor);
    }

    /** {@inheritDoc} */
    @Override public void dynamicIndexDrop(@NotNull String schemaName, String idxName, boolean ifExists)
        throws IgniteCheckedException{
        await(ctx.localNodeId());

        super.dynamicIndexDrop(schemaName, idxName, ifExists);
    }

    /**
     * Block indexing.
     *
     * @param node Node.
     */
    @SuppressWarnings("SuspiciousMethodCalls")
    public static CountDownLatch block(Ignite node) {
        UUID nodeId = ((IgniteEx)node).localNode().id();

        return block(nodeId);
    }

    /**
     * Block indexing.
     *
     * @param nodeId Node.
     */
    @SuppressWarnings("SuspiciousMethodCalls")
    public static CountDownLatch block(UUID nodeId) {
        TestCase.assertFalse(BLOCKS.containsValue(nodeId));

        CountDownLatch idxLatch = new CountDownLatch(1);

        BLOCKS.put(nodeId, new T3<>(new CountDownLatch(1), new AtomicBoolean(), idxLatch));

        return idxLatch;
    }

    /**
     * Unblock indexing.
     *
     * @param node Node.
     */
    public static void unblock(Ignite node) {
        UUID nodeId = ((IgniteEx)node).localNode().id();

        unblock(nodeId);
    }

    /**
     * Unblock indexing.
     *
     * @param nodeId Node ID.
     */
    @SuppressWarnings("ConstantConditions")
    public static void unblock(UUID nodeId) {
        T3<CountDownLatch, AtomicBoolean, CountDownLatch> blocker = BLOCKS.remove(nodeId);

        TestCase.assertNotNull(blocker);

        blocker.get1().countDown();
    }

    /**
     * Await indexing.
     *
     * @param nodeId Node ID.
     */
    @SuppressWarnings("ConstantConditions")
    private static void await(UUID nodeId) {
        T3<CountDownLatch, AtomicBoolean, CountDownLatch> blocker = BLOCKS.get(nodeId);

        if (blocker != null) {
            TestCase.assertTrue(blocker.get2().compareAndSet(false, true));

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
     * Unblocks all 1st blockers and clears {@code BLOCKS} map.
     * Must be invoked at {@code afterTest()} methods of test classes which used this indexing processor.
     */
    public static void clear() {
        for (T3<CountDownLatch, AtomicBoolean, CountDownLatch> block : BLOCKS.values())
            block.get1().countDown();

        BLOCKS.clear();
    }
}
