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

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractSnapshotSelfTest.snp;

/** */
public class SkipSameConsistentCutTest extends AbstractConsistentCutTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setPluginProviders(new BlockingConsistentCutPluginProvider());

        return cfg;
    }

    /** */
    @Test
    public void test() throws Exception {
        ((BlockingConsistentCutManager)cutMgr(grid(1))).block();

        snp(grid(0)).createIncrementalSnapshot(SNP);

        ((BlockingConsistentCutManager)cutMgr(grid(1))).awaitBlocked();

        GridTestUtils.waitForCondition(
            () -> cutMgr(grid(0)).consistentCut() == null,
            10);

        spi(grid(1)).record(ConsistentCutAwareMessage.class);

        try (Transaction tx = grid(1).transactions().txStart()) {
            for (int i = 0; i < 10; i++)
                grid(1).cache(CACHE).put(i, i);

            tx.commit();
        }

        assertFalse(spi(grid(1)).recordedMessages(true).isEmpty());

        ((BlockingConsistentCutManager)cutMgr(grid(1))).unblock();

        stopAllGrids();

        for (int i = 0; i < nodes(); i++) {
            int startRec = 0;
            int finishRec = 0;

            for (IgniteBiTuple<WALPointer, WALRecord> entry: walIter(i)) {
                WALRecord rec = entry.getValue();

                if (rec.type() == WALRecord.RecordType.CONSISTENT_CUT_START_RECORD) {
                    startRec++;

                    assertEquals(1, startRec);
                }

                if (rec.type() == WALRecord.RecordType.CONSISTENT_CUT_FINISH_RECORD) {
                    finishRec++;

                    assertEquals(1, finishRec);
                }
            }
        }
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
                return (T)new BlockingConsistentCutManager();

            return null;
        }
    }

    /** Blocks Consistent Cut before it is cleaned. */
    static class BlockingConsistentCutManager extends ConsistentCutManager {
        /** */
        private volatile CountDownLatch latch;

        /** */
        private final CountDownLatch blkLatch = new CountDownLatch(1);

        /** */
        static BlockingConsistentCutManager cutMgr(IgniteEx ign) {
            return (BlockingConsistentCutManager)ign.context().cache().context().consistentCutMgr();
        }

        /** {@inheritDoc} */
        @Override public void cleanConsistentCut() {
            if (latch != null) {
                blkLatch.countDown();

                U.awaitQuiet(latch);
            }

            super.cleanConsistentCut();
        }

        /** */
        public void block() {
            latch = new CountDownLatch(1);
        }

        /** */
        public void unblock() {
            latch.countDown();
        }

        /** */
        public void awaitBlocked() {
            U.awaitQuiet(blkLatch);
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
}
