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
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.consistentcut.AbstractConsistentCutBlockingTest.NodeType.NEAR;
import static org.apache.ignite.internal.processors.cache.consistentcut.AbstractConsistentCutBlockingTest.NodeType.PRIMARY;

/** */
public abstract class AbstractConsistentCutWALBlockingTest extends AbstractConsistentCutBlockingTest {
    /** */
    protected static NodeType blkNodeType;

    /** */
    protected static TransactionState blkTxState;

    /** */
    private static UUID blkNodeId;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setPluginProviders(new BlockingWALPluginProvider());

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

    /** */
    private static class BlockingWALManager extends FileWriteAheadLogManager {
        /** */
        private final GridKernalContext ctx;

        /**
         * Constructor.
         *
         * @param ctx Kernal context.
         */
        public BlockingWALManager(GridKernalContext ctx) {
            super(ctx);

            this.ctx = ctx;
        }

        /** {@inheritDoc} */
        @Override public WALPointer log(WALRecord record) throws IgniteCheckedException, StorageException {
            if (blkRecord(record)) {
                try {
                    txLatch.countDown();

                    cutLatch.await(1_000, TimeUnit.MILLISECONDS);
                }
                catch (InterruptedException e) {
                    throw new IgniteCheckedException(e);
                }
            }

            return super.log(record);
        }

        /** */
        private boolean blkRecord(WALRecord record) {
            return record instanceof TxRecord
                && ((TxRecord)record).state() == blkTxState
                && ctx.localNodeId().equals(blkNodeId);
        }
    }
}
