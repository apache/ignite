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

package org.apache.ignite.internal.processors.cache.consistentcut;

import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/** */
public class ConsistentCutUnstableTopologyTest extends AbstractConsistentCutTest {
    /** */
    private static volatile boolean asyncLoad;

    /** */
    private static volatile CountDownLatch consistentCutBlkLatch;

    /** */
    private static volatile CountDownLatch txBlkLatch;

    /** */
    private static volatile UUID cutBlkNodeId;

    /** */
    private static volatile UUID txBlkNodeId;

    /** */
    private final Map<IgniteUuid, Integer> txOrigNode = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setPluginProviders(
            new BlockingConsistentCutPluginProvider(),
            new BlockingWALPluginProvider());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        asyncLoad = true;
        consistentCutBlkLatch = null;
        txBlkLatch = null;

        TestConsistentCutManager.enableConsistentCutScheduling(grid(0));
    }

    /** */
    @Test
    public void testStopAndCutBlockServerNode() throws Exception {
        testWithTopologyChanged(0, 1, 2);
    }

    /** */
    @Test
    public void testStopServerNodeAndCutBlockCoordinatorNode() throws Exception {
        testWithTopologyChanged(1, 0, 2);
    }

    /** */
    @Test
    public void testStopClientNode() throws Exception {
        testWithTopologyChanged(0, 1, nodes());
    }

    /** */
    @Test
    public void testAddClientNode() throws Exception {
        startClientGrid();

        testWithTopologyChanged(0, 1, nodes());
    }

    /** */
    private void testWithTopologyChanged(int blkTxNode, int blkCutNode, int stopNode) throws Exception {
        IgniteInternalFuture<?> loadFut = asyncLoadData();

        try {
            // Some Consistent Cuts might finish before test comes here.
            long blkCutVer = 1 + awaitGlobalCutReady(1, false);

            txBlkNodeId = grid(blkTxNode).localNode().id();
            cutBlkNodeId = grid(blkCutNode).localNode().id();

            consistentCutBlkLatch = new CountDownLatch(1);
            txBlkLatch = new CountDownLatch(1);

            awaitGlobalCutVersionReceived(blkCutVer);

            stopGrid(stopNode);

            txBlkLatch.countDown();
            consistentCutBlkLatch.countDown();

            long lastCutVer = awaitGlobalCutReady(blkCutVer + 1, false);

            asyncLoad = false;

            loadFut.get();

            checkWalsConsistency(txOrigNode, (int)lastCutVer, nodeIds(stopNode), true);
        }
        finally {
            loadFut.cancel();
        }
    }

    /** {@inheritDoc} */
    @Override protected int nodes() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected int backups() {
        return 2;
    }

    /**
     * Starts creating transactions with concurrent load.
     *
     * @return Future that completes with full amount of transactions.
     */
    private IgniteInternalFuture<?> asyncLoadData() throws Exception {
        return multithreadedAsync(() -> {
            Random r = new Random();

            while (asyncLoad && !Thread.interrupted()) {
                try {
                    int n = r.nextInt(nodes() + 1); // +1 client

                    Ignite g = grid(n);

                    try (Transaction tx = g.transactions().txStart()) {
                        txOrigNode.put(tx.xid(), n);

                        for (int j = 0; j < nodes(); j++) {
                            int key = key(CACHE, grid(j).localNode(), grid((j + 1) % nodes()).localNode());

                            IgniteCache<Integer, Integer> cache = g.cache(CACHE);

                            cache.put(key, r.nextInt());
                        }

                        tx.commit();
                    }
                }
                catch (Throwable e) {
                    if (!expectedThrowable(e)) {
                        error("Unexpected throwable", e);

                        throw e;
                    }
                }
            }
        }, 2);
    }

    /** */
    private boolean expectedThrowable(Throwable e) {
        return X.hasCause(e, ClusterTopologyException.class, NodeStoppingException.class, IgniteIllegalStateException.class);
    }

    /** */
    private static class BlockingConsistentCutPluginProvider extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return "BlockingConsistentCutPluginProvider";
        }

        /** {@inheritDoc} */
        @Override public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
            if (ConsistentCutManager.class.equals(cls))
                return (T)new BlockingConsistentCutManager(((IgniteEx)ctx.grid()).context());

            return null;
        }
    }

    /** Blocks Consistent Cut procedure on writing ConsistentCutStartRecord or preparing and publishing Consistent Cut state. */
    protected static class BlockingConsistentCutManager extends TestConsistentCutManager {
        /** */
        private final GridKernalContext ctx;

        /** {@inheritDoc} */
        @Override protected ConsistentCut newConsistentCut() {
            return new BlockingConsistentCut(ctx.cache().context());
        }

        /** */
        public BlockingConsistentCutManager(GridKernalContext ctx) {
            this.ctx = ctx;
        }

        /** */
        private static class BlockingConsistentCut extends ConsistentCut {
            /** */
            private final UUID nodeId;

            /** */
            BlockingConsistentCut(GridCacheSharedContext<?, ?> cctx) {
                super(cctx);

                nodeId = cctx.localNodeId();
            }

            /** Blocks before or after ConsistentCut preparation. */
            @Override protected void init(ConsistentCutVersion ver) throws IgniteCheckedException {
                if (blkConsistentCutInit())
                    U.await(consistentCutBlkLatch, 60_000, TimeUnit.MILLISECONDS);

                super.init(ver);
            }

            /** */
            private boolean blkConsistentCutInit() {
                return consistentCutBlkLatch != null && nodeId.equals(cutBlkNodeId);
            }
        }
    }

    /** */
    private static class BlockingWALPluginProvider extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return "BlockingWALProvider";
        }

        /** {@inheritDoc} */
        @Override public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
            if (IgniteWriteAheadLogManager.class.equals(cls))
                return (T)new BlockingWALManager(((IgniteEx)ctx.grid()).context());

            return null;
        }
    }

    /** Blocks writing to WAL specific tx record, and awaits for Consistent Cut procedure starts on every node. */
    private static class BlockingWALManager extends FileWriteAheadLogManager {
        /** */
        private final UUID nodeId;

        /**
         * Constructor.
         *
         * @param ctx Kernal context.
         */
        public BlockingWALManager(GridKernalContext ctx) {
            super(ctx);

            nodeId = ctx.localNodeId();
        }

        /** {@inheritDoc} */
        @Override public WALPointer log(WALRecord record) throws IgniteCheckedException, StorageException {
            if (blkRecord(record))
                U.await(txBlkLatch, 60_000, TimeUnit.MILLISECONDS);

            return super.log(record);
        }

        /** */
        private boolean blkRecord(WALRecord record) {
            return txBlkLatch != null
                && nodeId.equals(txBlkNodeId)
                && record instanceof TxRecord;
        }
    }
}
