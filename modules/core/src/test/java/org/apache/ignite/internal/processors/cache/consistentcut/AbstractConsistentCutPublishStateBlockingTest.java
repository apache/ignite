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

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.consistentcut.AbstractConsistentCutBlockingTest.NodeType.NEAR;
import static org.apache.ignite.internal.processors.cache.consistentcut.AbstractConsistentCutBlockingTest.NodeType.PRIMARY;

/** */
public abstract class AbstractConsistentCutPublishStateBlockingTest extends AbstractConsistentCutMessagesBlockingTest {
    /** */
    private static UUID blkNodeId;

    /** */
    protected static NodeType blkNodeType;

    /** */
    protected static volatile CountDownLatch cutPblLatch;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setPluginProviders(new BlockingConsistentCutPluginProvider());

        return cfg;
    }

    /** */
    @Override protected void tx(int near, List<T2<Integer, Integer>> keys) {
        IgniteEx blkGrid;

        if (blkNodeType == NEAR)
            blkGrid = grid(near);
        else if (blkNodeType == PRIMARY)
            blkGrid = grid(keys.get(0).get1());
        else
            blkGrid = grid(keys.get(0).get2());

        blkNodeId = blkGrid.localNode().id();

        super.tx(near, keys);
    }

    /**
     * @param tx Function that performs transaction.
     * @param nearNodeId ID of near node.
     * @param c Test case - list of tuples (prim, backup) to be written.
     */
    @Override protected void runCase(Runnable tx, Integer nearNodeId, List<T2<Integer, Integer>> c) throws Exception {
        long prevVer = grid(0).context().cache().context().consistentCutMgr().latestCutVersion();

        cutLatch = new CountDownLatch(1);
        cutPblLatch = new CountDownLatch(1);
        txLatch = new CountDownLatch(1);

        log.info("START CASE " + caseNum + ". Data=" + c + ", nearNodeId=" + nearNodeId);

        int excl;

        if (blkNodeType == NEAR)
            excl = nearNodeId;
        else if (blkNodeType == PRIMARY)
            excl = c.get(0).get1();
        else
            excl = c.get(0).get2();

        if (excl == 0) {
            log.info("SKIP CASE (to avoid block coordinator) " + caseNum + ". Data=" + c + ", nearNodeId=" + nearNodeId);

            return;
        }

        caseNum++;

        Future<?> txFut = exec.submit(() -> {
            tx.run();

            // Some cases don't block on blkMsgCls. Then no need to await txLatch for such cases.
            txLatch.countDown();
        });

        txLatch.await(1_000, TimeUnit.MILLISECONDS);

        // TODO: escape cases where blocking cut on node0?
        grid(0).context().cache().context().consistentCutMgr().triggerConsistentCutOnCluster();

        awaitCutStarted(prevVer, excl);

        cutLatch.countDown();

        txFut.get(getTestTimeout(), TimeUnit.MILLISECONDS);

        cutPblLatch.countDown();

        // Await while all async operations completed.
        awaitGlobalCutReady(prevVer);
    }

    /** */
    private static class BlockingConsistentCutPluginProvider extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return "BlockingConsistentCutProvider";
        }

        /** {@inheritDoc} */
        @Override public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
            if (ConsistentCutManager.class.equals(cls))
                return (T)new BlockingConsistentCutManager(((IgniteEx)ctx.grid()).context());

            return null;
        }
    }

    /** */
    private static class BlockingConsistentCutManager extends ConsistentCutManager {
        /** */
        private final GridKernalContext ctx;

        /**
         * Constructor.
         *
         * @param ctx Kernal context.
         */
        public BlockingConsistentCutManager(GridKernalContext ctx) {
            this.ctx = ctx;
        }

        /** {@inheritDoc} */
        @Override protected ConsistentCutState consistentCut(UUID crdNodeId, long cutVer) {
            if (blkCut()) {
                try {
                    cutPblLatch.await(1_000, TimeUnit.MILLISECONDS);
                }
                catch (InterruptedException e) {
                    throw new IgniteException(e);
                }
            }

            return super.consistentCut(crdNodeId, cutVer);
        }

        /** */
        private boolean blkCut() {
            return ctx.localNodeId().equals(blkNodeId);
        }
    }
}
