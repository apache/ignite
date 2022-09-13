///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *      http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.ignite.internal.processors.cache.consistentcut;
//
//import org.apache.ignite.IgniteDataStreamer;
//import org.apache.ignite.cluster.ClusterState;
//import org.apache.ignite.internal.TestRecordingCommunicationSpi;
//import org.apache.ignite.internal.util.typedef.internal.U;
//import org.apache.ignite.testframework.GridTestUtils;
//import org.junit.Test;
//
///** */
//public class ConsistentCutActivationTest extends AbstractConsistentCutBlockingTest {
//    /** {@inheritDoc} */
//    @Override protected void beforeTest() throws Exception {
//        super.beforeTest();
//
//        try (IgniteDataStreamer<Integer, Integer> ds = grid(0).dataStreamer(CACHE)) {
//            for (int i = 0; i < 10_000; i++)
//                ds.addData(i, i);
//        }
//    }
//
//    /** */
//    @Test
//    public void testConsistentCutDisabledByDefault() {
//        assertConsistentCutDisabled(null);
//    }
//
//    /** */
//    @Test
//    public void testConsistentCutStartedOnSnapshotCreation() {
//        grid(1).context().cache().context().snapshotMgr().createSnapshot("snp").get();
//
//        assertConsistentCutEnabled();
//    }
//
//    /** */
//    @Test
//    public void testConsistentCutStartedOnSnapshotRestore() {
//        grid(1).context().cache().context().snapshotMgr().createSnapshot("snp").get();
//
//        assertConsistentCutDisabled(null);
//
//        grid(0).cache(CACHE).destroy();
//
//        grid(1).context().cache().context().snapshotMgr().restoreSnapshot("snp", null).get();
//
//        assertConsistentCutEnabled();
//    }
//
//    /** */
//    @Test
//    public void testConsistentCutStoppedOnServerFailed() {
//        TestConsistentCutManager.cutMgr(grid(0)).scheduleConsistentCut();
//
//        assertConsistentCutEnabled();
//
//        stopGrid(1);
//
//        assertConsistentCutDisabled(1);
//    }
//
//    /** */
//    @Test
//    public void testConsistentCutStoppedOnServerAdd() throws Exception {
//        TestConsistentCutManager.cutMgr(grid(0)).scheduleConsistentCut();
//
//        assertConsistentCutEnabled();
//
//        startGrid(nodes() + 1);
//
//        assertConsistentCutDisabled(null);
//    }
//
//    /** */
//    @Test
//    public void testConsistentCutClientNotAffect() throws Exception {
//        TestConsistentCutManager.cutMgr(grid(0)).scheduleConsistentCut();
//
//        assertConsistentCutEnabled();
//
//        startClientGrid(nodes() + 1);
//
//        assertConsistentCutEnabled();
//
//        stopGrid(nodes());
//
//        assertConsistentCutEnabled();
//    }
//
//    /** */
//    @Test
//    public void testClusterActivationNotAffect() throws Exception {
//        TestConsistentCutManager.cutMgr(grid(0)).scheduleConsistentCut();
//
//        assertConsistentCutEnabled();
//
//        grid(0).cluster().state(ClusterState.INACTIVE);
//
//        long actVer = awaitGlobalCutReady(2, false);
//
//        assertConsistentCutEnabled();
//
//        assertTrue(actVer >= 2);
//
//        grid(0).cluster().state(ClusterState.ACTIVE);
//
//        awaitGlobalCutReady(actVer + 1, false);
//
//        assertConsistentCutEnabled();
//    }
//
//    /**
//     * Checks that re-init scheduling after coordinator changed works correctly: it handles different Consistent Cut versions
//     * on different nodes (new coordinator has version less than other server nodes).
//     */
//    @Test
//    public void testReinitAfterCoordinatorChanged() throws Exception {
//        TestConsistentCutManager.cutMgr(grid(0)).scheduleConsistentCut();
//
//        awaitGlobalCutReady(1, false);
//
//        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(0));
//
//        // Block sending ConsistentCut to all server nodes except coordinator and last one
//        for (int i = 1; i < nodes() - 1; i++)
//            spi.blockMessages(ConsistentCutStartRequest.class, grid(i).name());
//
//        // Ensure that block sending message at least to single node.
//        spi.waitForBlocked(1, getTestTimeout());
//
//        long blkCutVer = TestConsistentCutManager.cutMgr(grid(0)).cutVersion().version();
//
//        // Await while last node received new Consistent Cut.
//        GridTestUtils.waitForCondition(() ->
//                TestConsistentCutManager.cutMgr(grid(nodes() - 1)).cutVersion().version() == blkCutVer,
//            getTestTimeout());
//
//        stopGrid(0);
//
//        awaitPartitionMapExchange();
//
//        // Ensure that new coordinator has old version of Consistent Cut.
//        assertEquals(blkCutVer - 1, TestConsistentCutManager.cutMgr(grid(1)).cutVersion().version());
//
//        assertTrue(U.isLocalNodeCoordinator(grid(1).context().discovery()));
//
//        // Return failed coordinator back as ordindary node.
//        startGrid(0);
//
//        awaitPartitionMapExchange();
//
//        assertTrue(U.isLocalNodeCoordinator(grid(1).context().discovery()));
//
//        TestConsistentCutManager.cutMgr(grid(1)).scheduleConsistentCut();
//
//        awaitGlobalCutReady(blkCutVer + 1, false);
//    }
//
//    /**
//     * Checks that it correctly handles changed coordinator with concurrent receiving message from previous coordinator.
//     */
//    @Test
//    public void testReceiveConcurrentReceiveSameCutVersionButDifferentTopologies() throws Exception {
//        TestConsistentCutManager.cutMgr(grid(0)).scheduleConsistentCut();
//
//        awaitGlobalCutReady(1, false);
//
//        // Last node received version from coordinator but did not handle it.
//        BlockingConsistentCutManager blkCutMgr = BlockingConsistentCutManager.cutMgr(grid(nodes() - 1));
//        blkCutMgr.block(BlkCutType.BEFORE_VERSION_UPDATE);
//
//        // Block sending ConsistentCut to all server nodes except coordinator and last one
//        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(0));
//
//        for (int i = 1; i < nodes() - 1; i++)
//            spi.blockMessages(ConsistentCutStartRequest.class, grid(i).name());
//
//        // Await sending new version blocked.
//        blkCutMgr.awaitBlocked();
//        spi.waitForBlocked(nodes() - 2, getTestTimeout());
//
//        long blkCutVer = TestConsistentCutManager.cutMgr(grid(0)).cutVersion().version();
//
//        // Stop coordinator and ensure that all nodes have previous version of Consistent Cut.
//        stopGrid(0);
//
//        awaitPartitionMapExchange();
//
//        for (int i = 1; i < nodes(); i++)
//            assertEquals(blkCutVer - 1, TestConsistentCutManager.cutMgr(grid(i)).cutVersion().version());
//
//        assertTrue(U.isLocalNodeCoordinator(grid(1).context().discovery()));
//
//        // Return failed coordinator back as ordindary node.
//        startGrid(0);
//
//        awaitPartitionMapExchange();
//
//        assertTrue(U.isLocalNodeCoordinator(grid(1).context().discovery()));
//
//        // Schedule new Consistent Cuts, but block sending it to last node.
//        spi = TestRecordingCommunicationSpi.spi(grid(1));
//        spi.blockMessages(ConsistentCutStartRequest.class, grid(nodes() - 1).name());
//
//        TestConsistentCutManager.cutMgr(grid(1)).scheduleConsistentCut();
//
//        spi.waitForBlocked();
//
//        // Force old Consistent Cut version to be handled by node, but not finish.
//        blkCutMgr.block(BlkCutType.BEFORE_FINISH);
//        blkCutMgr.unblock(BlkCutType.BEFORE_VERSION_UPDATE);
//        blkCutMgr.awaitBlocked();
//
//        // Force new Consistent Cut version to be updated by node.
//        blkCutMgr.block(BlkCutType.AFTER_VERSION_UPDATE);
//        spi.stopBlock();
//        blkCutMgr.awaitBlocked();
//
//        blkCutMgr.unblock(BlkCutType.BEFORE_FINISH);
//        blkCutMgr.unblock(BlkCutType.AFTER_VERSION_UPDATE);
//
//        // Await that new version will be finished on coordinator node.
//        awaitGlobalCutReady(blkCutVer + 1, false);
//    }
//
//    /** {@inheritDoc} */
//    @Override protected int nodes() {
//        return 3;
//    }
//
//    /** {@inheritDoc} */
//    @Override protected int backups() {
//        return 2;
//    }
//}
