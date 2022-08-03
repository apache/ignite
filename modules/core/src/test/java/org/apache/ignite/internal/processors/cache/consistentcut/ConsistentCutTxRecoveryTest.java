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

import java.util.Random;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

/***/
public class ConsistentCutTxRecoveryTest extends AbstractConsistentCutBlockingTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /** */
    @Test
    public void testSkipFinishRecordOnTxRecovery() throws Exception {
        IgniteInternalFuture<?> loadFut = asyncLoadData();

        try {
            BlockingConsistentCutManager srvCutMgr = BlockingConsistentCutManager.cutMgr(grid(0));

            srvCutMgr.scheduleConsistentCut();

            awaitGlobalCutReady(1, true);

            // Blocks finish message from client to server.
            TestRecordingCommunicationSpi clnComm = TestRecordingCommunicationSpi.spi(grid(nodes()));

            clnComm.blockMessages(GridNearTxFinishRequest.class, grid(0).name());
            clnComm.waitForBlocked();

            // Block Consistent Cut on server.
            srvCutMgr.block(BlkCutType.AFTER_VERSION_UPDATE);
            srvCutMgr.awaitBlocked();

            long blkCutVer = srvCutMgr.cutVersion().version();

            // Stop client node.
            stopGrid(nodes());

            loadFut.cancel();

            // Await this Consistent Cut and next one.
            srvCutMgr.unblock(BlkCutType.AFTER_VERSION_UPDATE);
            awaitGlobalCutReady(blkCutVer + 1, false);

            srvCutMgr.disableScheduling(false);

            // Stop cluster with flushing WALs.
            stopCluster();

            for (int i = 0; i < nodes(); i++)
                assertWalConsistentRecords(i, blkCutVer, i == 0);
        }
        finally {
            loadFut.cancel();
        }
    }

    /** */
    private void assertWalConsistentRecords(int nodeIdx, long blkVer, boolean skipBlkVer) throws Exception {
        WALIterator iter = walIter(nodeIdx);

        boolean expFinRec = false;

        boolean lastVerChecked = false;

        while (iter.hasNext()) {
            WALRecord rec = iter.next().getValue();

            if (rec.type() == WALRecord.RecordType.CONSISTENT_CUT_START_RECORD) {
                log.info("REC " + rec);

                ConsistentCutStartRecord startRec = (ConsistentCutStartRecord)rec;

                if (skipBlkVer)
                    expFinRec = startRec.version().version() != blkVer;
                else
                    expFinRec = true;

                lastVerChecked = startRec.version().version() == blkVer + 1;

            }
            else if (rec.type() == WALRecord.RecordType.CONSISTENT_CUT_FINISH_RECORD) {
                log.info("REC " + rec);

                assertTrue("Unexpect Finish Record. Blk ver " + blkVer, expFinRec);

                expFinRec = false;
            }
        }

        assertTrue("Should reach version after blk " + blkVer, lastVerChecked);
    }

    /** */
    private IgniteInternalFuture<?> asyncLoadData() throws Exception {
        return multithreadedAsync(() -> {
            Random r = new Random();

            while (!Thread.interrupted()) {
                Ignite g = grid(nodes());

                try (Transaction tx = g.transactions().txStart()) {
                    for (int j = 0; j < nodes(); j++) {
                        IgniteCache<Integer, Integer> cache = g.cache(CACHE);

                        int k = key(CACHE, grid(j).localNode(), grid((j + 1) % nodes()).localNode());

                        cache.put(k, r.nextInt());
                    }

                    tx.commit();
                }
            }
        }, 1);
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
