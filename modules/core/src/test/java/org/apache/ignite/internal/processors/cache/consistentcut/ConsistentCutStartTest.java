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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/** */
public class ConsistentCutStartTest extends AbstractConsistentCutBlockingTest {
    /** */
    @Test
    public void testSequentCutWithTheSameVersionFailed() throws Exception {
        AbstractConsistentCutBlockingTest.BlockingConsistentCutManager srvCutMgr =
            AbstractConsistentCutBlockingTest.BlockingConsistentCutManager.cutMgr(grid(0));

        assertTrue(srvCutMgr.triggerConsistentCutOnCluster(0).get(getTestTimeout()));

        for (int i = 0; i < nodes(); i++) {
            waitForCutIsFinishedOnAllNodes();

            AbstractConsistentCutBlockingTest.BlockingConsistentCutManager mgr =
                AbstractConsistentCutBlockingTest.BlockingConsistentCutManager.cutMgr(grid(i));

            GridTestUtils.assertThrows(
                log,
                () -> mgr.triggerConsistentCutOnCluster(0).get(getTestTimeout()),
                IgniteCheckedException.class, null);
        }

        waitForCutIsFinishedOnAllNodes();

        assertTrue(srvCutMgr.triggerConsistentCutOnCluster(1).get(getTestTimeout()));

        for (int i = 0; i < nodes(); i++)
            assertWalConsistentRecords(i, 1);
    }

    /** */
    @Test
    public void testConccurrentCutWithTheDifferentVersionFailed() throws Exception {
        AbstractConsistentCutBlockingTest.BlockingConsistentCutManager srvCutMgr =
            AbstractConsistentCutBlockingTest.BlockingConsistentCutManager.cutMgr(grid(0));

        srvCutMgr.block(BlkCutType.AFTER_VERSION_UPDATE);

        IgniteInternalFuture<Boolean> clusterCutFut = srvCutMgr.triggerConsistentCutOnCluster(0);

        waitForCutIsStartedOnAllNodes();

        for (int i = 0; i < nodes(); i++) {
            AbstractConsistentCutBlockingTest.BlockingConsistentCutManager mgr =
                AbstractConsistentCutBlockingTest.BlockingConsistentCutManager.cutMgr(grid(i));

            GridTestUtils.assertThrows(
                log,
                () -> mgr.triggerConsistentCutOnCluster(1).get(getTestTimeout()),
                IgniteCheckedException.class, null);
        }

        srvCutMgr.unblock(BlkCutType.AFTER_VERSION_UPDATE);

        assertTrue(clusterCutFut.get(getTestTimeout()));

        for (int i = 0; i < nodes(); i++)
            assertWalConsistentRecords(i, 0);
    }

    /** */
    @Test
    public void testConccurrentCutWithTheSameVersionFailed() throws Exception {
        AbstractConsistentCutBlockingTest.BlockingConsistentCutManager srv0CutMgr =
            AbstractConsistentCutBlockingTest.BlockingConsistentCutManager.cutMgr(grid(0));

        AbstractConsistentCutBlockingTest.BlockingConsistentCutManager srv1CutMgr =
            AbstractConsistentCutBlockingTest.BlockingConsistentCutManager.cutMgr(grid(1));

        srv1CutMgr.block(BlkCutType.AFTER_VERSION_UPDATE);

        IgniteInternalFuture<Boolean> clusterCutFut = srv0CutMgr.triggerConsistentCutOnCluster(0);

        srv1CutMgr.awaitBlockedOrFinishedCut(null);

        GridTestUtils.assertThrows(
            log,
            () -> srv1CutMgr.triggerConsistentCutOnCluster(0).get(getTestTimeout()),
            IgniteCheckedException.class, null);

        srv1CutMgr.unblock(BlkCutType.AFTER_VERSION_UPDATE);

        assertTrue(clusterCutFut.get(getTestTimeout()));

        for (int i = 0; i < nodes(); i++)
            assertWalConsistentRecords(i, 0);
    }

    /** */
    @Test
    public void testConccurrentCutWithTheSameVersionSucceed() throws Exception {
        AbstractConsistentCutBlockingTest.BlockingConsistentCutManager srv0CutMgr =
            AbstractConsistentCutBlockingTest.BlockingConsistentCutManager.cutMgr(grid(0));

        AbstractConsistentCutBlockingTest.BlockingConsistentCutManager srv1CutMgr =
            AbstractConsistentCutBlockingTest.BlockingConsistentCutManager.cutMgr(grid(1));

        srv1CutMgr.block(BlkCutType.BEFORE_VERSION_UPDATE);

        IgniteInternalFuture<Boolean> cluster0CutFut = srv0CutMgr.triggerConsistentCutOnCluster(0);

        srv1CutMgr.awaitBlockedOrFinishedCut(null);

        IgniteInternalFuture<Boolean> cluster1CutFut = srv1CutMgr.triggerConsistentCutOnCluster(0);

        srv1CutMgr.unblock(BlkCutType.BEFORE_VERSION_UPDATE);

        assertTrue(cluster0CutFut.get(getTestTimeout()));
        assertTrue(cluster1CutFut.get(getTestTimeout()));

        for (int i = 0; i < nodes(); i++)
            assertWalConsistentRecords(i, 0);
    }

    /** */
    private void assertWalConsistentRecords(int nodeIdx, int lastVer) throws Exception {
        WALIterator iter = walIter(nodeIdx);

        long lastVerChecked = -1;

        boolean expFinRec = false;

        while (iter.hasNext()) {
            WALRecord rec = iter.next().getValue();

            if (rec.type() == WALRecord.RecordType.CONSISTENT_CUT_START_RECORD) {
                ConsistentCutStartRecord startRec = (ConsistentCutStartRecord)rec;

                long ver = startRec.marker().version();

                assertTrue(ver == lastVerChecked + 1);

                lastVerChecked = ver;

                expFinRec = true;
            }
            else if (rec.type() == WALRecord.RecordType.CONSISTENT_CUT_FINISH_RECORD) {
                assertTrue("Unexpect Finish Record.", expFinRec);

                expFinRec = false;
            }
        }

        assertTrue(lastVerChecked == lastVer);
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
