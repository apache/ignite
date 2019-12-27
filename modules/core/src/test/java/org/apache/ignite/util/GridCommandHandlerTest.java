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

package org.apache.ignite.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.GridJobExecuteResponse;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.client.util.GridConcurrentHashSet;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.dumpprocessors.ToFileDumpProcessor;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.tx.VisorTxInfo;
import org.apache.ignite.internal.visor.tx.VisorTxTaskResult;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static java.io.File.separatorChar;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_CLUSTER_NAME;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.PartitionLossPolicy.READ_ONLY_SAFE;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.commandline.CommandHandler.CONFIRM_MSG;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.internal.commandline.CommandList.DEACTIVATE;
import static org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor.DEFAULT_TARGET_FOLDER;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/**
 * Command line handler test.
 * You can use this class if you need create nodes for each test.
 * If you not necessary create nodes for each test you can try use {@link GridCommandHandlerClusterByClassTest}
 */
public class GridCommandHandlerTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** */
    protected static File defaultDiagnosticDir;

    /** */
    protected static File customDiagnosticDir;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        initDiagnosticDir();

        cleanDiagnosticDir();
    }

    /** {@inheritDoc} */
    @Override protected void cleanPersistenceDir() throws Exception {
        super.cleanPersistenceDir();

        cleanDiagnosticDir();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    protected void initDiagnosticDir() throws IgniteCheckedException {
        defaultDiagnosticDir = new File(U.defaultWorkDirectory()
            + separatorChar + DEFAULT_TARGET_FOLDER + separatorChar);

        customDiagnosticDir = new File(U.defaultWorkDirectory()
            + separatorChar + "diagnostic_test_dir" + separatorChar);
    }

    /**
     * Clean diagnostic directories.
     */
    protected void cleanDiagnosticDir() {
        U.delete(defaultDiagnosticDir);
        U.delete(customDiagnosticDir);
    }

    /**
     * Test activation works via control.sh
     *
     * @throws Exception If failed.
     */
    @Test
    public void testActivate() throws Exception {
        Ignite ignite = startGrids(1);

        assertFalse(ignite.cluster().active());

        assertEquals(EXIT_CODE_OK, execute("--activate"));

        assertTrue(ignite.cluster().active());
        assertFalse(ignite.cluster().readOnly());
    }

    /**
     * Test enabling/disabling read-only mode works via control.sh
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReadOnlyEnableDisable() throws Exception {
        Ignite ignite = startGrids(1);

        ignite.cluster().active(true);

        assertFalse(ignite.cluster().readOnly());

        assertEquals(EXIT_CODE_OK, execute("--read-only-on"));

        assertTrue(ignite.cluster().readOnly());

        assertEquals(EXIT_CODE_OK, execute("--read-only-off"));

        assertFalse(ignite.cluster().readOnly());
    }

    /**
     * Test deactivation works via control.sh
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivate() throws Exception {
        Ignite ignite = startGrids(1);

        assertFalse(ignite.cluster().active());

        ignite.cluster().active(true);

        assertTrue(ignite.cluster().active());

        assertEquals(EXIT_CODE_OK, execute("--deactivate"));

        assertFalse(ignite.cluster().active());
    }

    /**
     * Test the deactivation command on the active and no cluster with checking
     * the cluster name(which is set through the system property) in
     * confirmation.
     *
     * @throws Exception If failed.
     * */
    @Test
    @WithSystemProperty(key = IGNITE_CLUSTER_NAME, value = "TEST_CLUSTER_NAME")
    public void testDeactivateWithCheckClusterNameInConfirmationBySystemProperty() throws Exception {
        IgniteEx igniteEx = startGrid(0);
        assertFalse(igniteEx.cluster().active());

        deactivateActiveOrNotClusterWithCheckClusterNameInConfirmation(igniteEx, "TEST_CLUSTER_NAME");
    }

    /**
     * Test the deactivation command on the active and no cluster with checking
     * the cluster name(default) in confirmation.
     *
     * @throws Exception If failed.
     * */
    @Test
    public void testDeactivateWithCheckClusterNameInConfirmationByDefault() throws Exception {
        IgniteEx igniteEx = startGrid(0);
        assertFalse(igniteEx.cluster().active());

        deactivateActiveOrNotClusterWithCheckClusterNameInConfirmation(
            igniteEx,
            igniteEx.context().cache().utilityCache().context().dynamicDeploymentId().toString()
        );
    }

    /**
     * Deactivating the cluster(active and not) with checking the cluster name
     * in the confirmation.
     *
     * @param igniteEx Node.
     * @param clusterName Cluster name to check in the confirmation message.
     * */
    private void deactivateActiveOrNotClusterWithCheckClusterNameInConfirmation(
        IgniteEx igniteEx,
        String clusterName
    ) {
        deactivateWithCheckClusterNameInConfirmation(igniteEx, clusterName);

        igniteEx.cluster().active(true);
        assertTrue(igniteEx.cluster().active());

        deactivateWithCheckClusterNameInConfirmation(igniteEx, clusterName);
    }

    /**
     * Deactivating the cluster with checking the cluster name in the
     * confirmation.
     *
     * @param igniteEx Node.
     * @param clusterName Cluster name to check in the confirmation message.
     * */
    private void deactivateWithCheckClusterNameInConfirmation(IgniteEx igniteEx, String clusterName) {
        autoConfirmation = false;
        injectTestSystemOut();
        injectTestSystemIn(CONFIRM_MSG);

        assertEquals(EXIT_CODE_OK, execute(DEACTIVATE.text()));
        assertFalse(igniteEx.cluster().active());

        assertContains(
            log,
            testOut.toString(),
            "Warning: the command will deactivate a cluster \"" + clusterName + "\"."
        );
    }

    /**
     * Test cluster active state works via control.sh
     *
     * @throws Exception If failed.
     */
    @Test
    public void testState() throws Exception {
        Ignite ignite = startGrids(1);

        assertFalse(ignite.cluster().active());

        assertEquals(EXIT_CODE_OK, execute("--state"));

        ignite.cluster().active(true);

        assertEquals(EXIT_CODE_OK, execute("--state"));
    }

    /**
     * Test baseline collect works via control.sh
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBaselineCollect() throws Exception {
        Ignite ignite = startGrids(1);

        assertFalse(ignite.cluster().active());

        ignite.cluster().active(true);

        assertEquals(EXIT_CODE_OK, execute("--baseline"));

        assertEquals(1, ignite.cluster().currentBaselineTopology().size());
    }

    /**
     * Test baseline collect works via control.sh
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBaselineCollectCrd() throws Exception {
        Ignite ignite = startGrids(2);

        assertFalse(ignite.cluster().active());

        ignite.cluster().active(true);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--baseline", "--port", "11212"));

        String crdStr = findCrdInfo();

        assertEquals("(Coordinator: ConsistentId=" +
            grid(0).cluster().localNode().consistentId() + ", Order=1)", crdStr);

        stopGrid(0);

        assertEquals(EXIT_CODE_OK, execute("--baseline", "--port", "11212"));

        crdStr = findCrdInfo();

        assertEquals("(Coordinator: ConsistentId=" +
            grid(1).cluster().localNode().consistentId() + ", Order=2)", crdStr);

        startGrid(0);

        assertEquals(EXIT_CODE_OK, execute("--baseline", "--port", "11212"));

        crdStr = findCrdInfo();

        assertEquals("(Coordinator: ConsistentId=" +
            grid(1).cluster().localNode().consistentId() + ", Order=2)", crdStr);

        stopGrid(1);

        assertEquals(EXIT_CODE_OK, execute("--baseline", "--port", "11211"));

        crdStr = findCrdInfo();

        assertEquals("(Coordinator: ConsistentId=" +
            grid(0).cluster().localNode().consistentId() + ", Order=4)", crdStr);
    }

    /**
     * @return utility information about coordinator
     */
    private String findCrdInfo() {
        String outStr = testOut.toString();

        int i = outStr.indexOf("(Coordinator: ConsistentId=");

        assertTrue(i != -1);

        String crdStr = outStr.substring(i).trim();

        return crdStr.substring(0, crdStr.indexOf('\n')).trim();
    }

    /**
     * @param ignites Ignites.
     * @return Local node consistent ID.
     */
    private String consistentIds(Ignite... ignites) {
        StringBuilder res = new StringBuilder();

        for (Ignite ignite : ignites) {
            String consistentId = ignite.cluster().localNode().consistentId().toString();

            if (res.length() != 0)
                res.append(", ");

            res.append(consistentId);
        }

        return res.toString();
    }

    /**
     * Test baseline add items works via control.sh
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBaselineAdd() throws Exception {
        Ignite ignite = startGrids(1);

        assertFalse(ignite.cluster().active());

        ignite.cluster().active(true);

        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute("--baseline", "add"));

        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute("--baseline", "add", "non-existent-id"));

        Ignite other = startGrid(2);

        assertEquals(EXIT_CODE_OK, execute("--baseline", "add", consistentIds(other)));

        assertEquals(2, ignite.cluster().currentBaselineTopology().size());
    }

    /**
     * Test baseline remove works via control.sh
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBaselineRemove() throws Exception {
        Ignite ignite = startGrids(1);
        Ignite other = startGrid("nodeToStop");

        assertFalse(ignite.cluster().active());

        ignite.cluster().active(true);

        String offlineNodeConsId = consistentIds(other);

        stopGrid("nodeToStop");

        assertEquals(EXIT_CODE_OK, execute("--baseline"));
        assertEquals(EXIT_CODE_OK, execute("--baseline", "remove", offlineNodeConsId));

        assertEquals(1, ignite.cluster().currentBaselineTopology().size());
    }

    /**
     * Test baseline remove node on not active cluster via control.sh
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBaselineRemoveOnNotActiveCluster() throws Exception {
        Ignite ignite = startGrids(1);
        Ignite other = startGrid("nodeToStop");

        assertFalse(ignite.cluster().active());

        String offlineNodeConsId = consistentIds(other);

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute("--baseline", "remove", offlineNodeConsId));

        ignite.cluster().active(true);

        stopGrid("nodeToStop");

        assertEquals(2, ignite.cluster().currentBaselineTopology().size());

        ignite.cluster().active(false);

        assertFalse(ignite.cluster().active());

        injectTestSystemOut();

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute("--baseline", "remove", offlineNodeConsId));

        assertContains(log, testOut.toString(), "Changing BaselineTopology on inactive cluster is not allowed.");
    }

    /**
     * Test baseline set works via control.sh
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBaselineSet() throws Exception {
        Ignite ignite = startGrids(1);

        assertFalse(ignite.cluster().active());

        ignite.cluster().active(true);

        Ignite other = startGrid(2);

        assertEquals(EXIT_CODE_OK, execute("--baseline", "set", consistentIds(ignite, other)));

        assertEquals(2, ignite.cluster().currentBaselineTopology().size());

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute("--baseline", "set", "invalidConsistentId"));
    }

    /**
     * Test baseline set by topology version works via control.sh
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBaselineVersion() throws Exception {
        Ignite ignite = startGrids(1);

        assertFalse(ignite.cluster().active());

        ignite.cluster().active(true);

        startGrid(2);

        assertEquals(EXIT_CODE_OK, execute("--baseline"));

        assertEquals(EXIT_CODE_OK, execute("--baseline", "version", String.valueOf(ignite.cluster().topologyVersion())));

        assertEquals(2, ignite.cluster().currentBaselineTopology().size());
    }

    /**
     * Test that updating of baseline auto_adjustment settings via control.sh actually influence cluster's baseline.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBaselineAutoAdjustmentAutoRemoveNode() throws Exception {
        Ignite ignite = startGrids(3);

        ignite.cluster().active(true);

        int timeout = 2000;

        assertEquals(EXIT_CODE_OK,
            execute("--baseline", "auto_adjust", "enable", "timeout", String.valueOf(timeout)));

        assertEquals(3, ignite.cluster().currentBaselineTopology().size());

        CountDownLatch nodeLeftLatch = new CountDownLatch(1);

        AtomicLong nodeLeftTime = new AtomicLong();

        ignite.events().localListen(event -> {
            nodeLeftTime.set(event.timestamp());

            nodeLeftLatch.countDown();

            return false;
        }, EVT_NODE_LEFT, EVT_NODE_FAILED);

        runAsync(() -> stopGrid(2));

        nodeLeftLatch.await();

        while (true) {
            int bltSize = ignite.cluster().currentBaselineTopology().size();

            if (System.currentTimeMillis() >= nodeLeftTime.get() + timeout)
                break;

            assertEquals(3, bltSize);

            U.sleep(100);
        }

        assertTrue(waitForCondition(() -> ignite.cluster().currentBaselineTopology().size() == 2, 10000));

        Collection<BaselineNode> baselineNodesAfter = ignite.cluster().currentBaselineTopology();

        assertEquals(EXIT_CODE_OK, execute("--baseline", "auto_adjust", "disable"));

        stopGrid(1);

        Thread.sleep(3000L);

        Collection<BaselineNode> baselineNodesFinal = ignite.cluster().currentBaselineTopology();

        assertEquals(
            baselineNodesAfter.stream().map(BaselineNode::consistentId).collect(Collectors.toList()),
            baselineNodesFinal.stream().map(BaselineNode::consistentId).collect(Collectors.toList())
        );
    }

    /**
     * Test that updating of baseline auto_adjustment settings via control.sh actually influence cluster's baseline.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBaselineAutoAdjustmentAutoAddNode() throws Exception {
        Ignite ignite = startGrids(1);

        ignite.cluster().active(true);

        assertEquals(EXIT_CODE_OK, execute("--baseline", "auto_adjust", "enable", "timeout", "2000"));

        assertEquals(1, ignite.cluster().currentBaselineTopology().size());

        startGrid(1);

        assertEquals(1, ignite.cluster().currentBaselineTopology().size());

        assertEquals(EXIT_CODE_OK, execute("--baseline"));

        assertTrue(waitForCondition(() -> ignite.cluster().currentBaselineTopology().size() == 2, 10000));

        Collection<BaselineNode> baselineNodesAfter = ignite.cluster().currentBaselineTopology();

        assertEquals(EXIT_CODE_OK, execute("--baseline", "auto_adjust", "disable"));

        startGrid(2);

        Thread.sleep(3000L);

        Collection<BaselineNode> baselineNodesFinal = ignite.cluster().currentBaselineTopology();

        assertEquals(
            baselineNodesAfter.stream().map(BaselineNode::consistentId).collect(Collectors.toList()),
            baselineNodesFinal.stream().map(BaselineNode::consistentId).collect(Collectors.toList())
        );
    }

    /**
     * Test active transactions.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testActiveTransactions() throws Exception {
        Ignite ignite = startGridsMultiThreaded(2);

        ignite.cluster().active(true);

        Ignite client = startGrid("client");

        client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL).setWriteSynchronizationMode(FULL_SYNC));

        for (Ignite ig : G.allGrids())
            assertNotNull(ig.cache(DEFAULT_CACHE_NAME));

        CountDownLatch lockLatch = new CountDownLatch(1);
        CountDownLatch unlockLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut = startTransactions("testActiveTransactions", lockLatch, unlockLatch, true);

        U.awaitQuiet(lockLatch);

        doSleep(5000);

        CommandHandler h = new CommandHandler();

        final VisorTxInfo[] toKill = {null};

        // Basic test.
        validate(h, map -> {
            VisorTxTaskResult res = map.get(grid(0).cluster().localNode());

            for (VisorTxInfo info : res.getInfos()) {
                if (info.getSize() == 100) {
                    toKill[0] = info; // Store for further use.

                    break;
                }
            }

            assertEquals(3, map.size());
        }, "--tx");

        assertNotNull(toKill[0]);

        // Test filter by label.
        validate(h, map -> {
            ClusterNode node = grid(0).cluster().localNode();

            for (Map.Entry<ClusterNode, VisorTxTaskResult> entry : map.entrySet())
                assertEquals(entry.getKey().equals(node) ? 1 : 0, entry.getValue().getInfos().size());
        }, "--tx", "--label", "label1");

        // Test filter by label regex.
        validate(h, map -> {
            ClusterNode node1 = grid(0).cluster().localNode();
            ClusterNode node2 = grid("client").cluster().localNode();

            for (Map.Entry<ClusterNode, VisorTxTaskResult> entry : map.entrySet()) {
                if (entry.getKey().equals(node1)) {
                    assertEquals(1, entry.getValue().getInfos().size());

                    assertEquals("label1", entry.getValue().getInfos().get(0).getLabel());
                }
                else if (entry.getKey().equals(node2)) {
                    assertEquals(1, entry.getValue().getInfos().size());

                    assertEquals("label2", entry.getValue().getInfos().get(0).getLabel());
                }
                else
                    assertTrue(entry.getValue().getInfos().isEmpty());

            }
        }, "--tx", "--label", "^label[0-9]");

        // Test filter by empty label.
        validate(h, map -> {
            VisorTxTaskResult res = map.get(grid(0).localNode());

            for (VisorTxInfo info : res.getInfos())
                assertNull(info.getLabel());

        }, "--tx", "--label", "null");

        // test check minSize
        int minSize = 10;

        validate(h, map -> {
            VisorTxTaskResult res = map.get(grid(0).localNode());

            assertNotNull(res);

            for (VisorTxInfo txInfo : res.getInfos())
                assertTrue(txInfo.getSize() >= minSize);
        }, "--tx", "--min-size", Integer.toString(minSize));

        // test order by size.
        validate(h, map -> {
            VisorTxTaskResult res = map.get(grid(0).localNode());

            assertTrue(res.getInfos().get(0).getSize() >= res.getInfos().get(1).getSize());
        }, "--tx", "--order", "SIZE");

        // test order by duration.
        validate(h, map -> {
            VisorTxTaskResult res = map.get(grid(0).localNode());

            assertTrue(res.getInfos().get(0).getDuration() >= res.getInfos().get(1).getDuration());
        }, "--tx", "--order", "DURATION");

        // test order by start_time.
        validate(h, map -> {
            VisorTxTaskResult res = map.get(grid(0).localNode());

            for (int i = res.getInfos().size() - 1; i > 1; i--)
                assertTrue(res.getInfos().get(i - 1).getStartTime() >= res.getInfos().get(i).getStartTime());
        }, "--tx", "--order", "START_TIME");

        // Trigger topology change and test connection.
        IgniteInternalFuture<?> startFut = multithreadedAsync(() -> {
            try {
                startGrid(2);
            }
            catch (Exception e) {
                fail();
            }
        }, 1, "start-node-thread");

        doSleep(5000); // Give enough time to reach exchange future.

        assertEquals(EXIT_CODE_OK, execute(h, "--tx"));

        // Test kill by xid.
        validate(h, map -> {
                assertEquals(1, map.size());

                Map.Entry<ClusterNode, VisorTxTaskResult> killedEntry = map.entrySet().iterator().next();

                VisorTxInfo info = killedEntry.getValue().getInfos().get(0);

                assertEquals(toKill[0].getXid(), info.getXid());
            }, "--tx", "--kill",
            "--xid", toKill[0].getXid().toString(), // Use saved on first run value.
            "--nodes", grid(0).localNode().consistentId().toString());

        unlockLatch.countDown();

        startFut.get();

        fut.get();

        awaitPartitionMapExchange();

        checkUserFutures();
    }

    /**
     * Simulate uncommitted backup transactions and test rolling back using utility.
     */
    @Test
    public void testKillHangingRemoteTransactions() throws Exception {
        final int cnt = 3;

        startGridsMultiThreaded(cnt);

        Ignite[] clients = new Ignite[] {
            startGrid("client1"),
            startGrid("client2"),
            startGrid("client3"),
            startGrid("client4")
        };

        clients[0].getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME).
            setBackups(2).
            setAtomicityMode(TRANSACTIONAL).
            setWriteSynchronizationMode(FULL_SYNC).
            setAffinity(new RendezvousAffinityFunction(false, 64)));

        for (Ignite client : clients) {
            assertTrue(client.configuration().isClientMode());

            assertNotNull(client.cache(DEFAULT_CACHE_NAME));
        }

        LongAdder progress = new LongAdder();

        AtomicInteger idx = new AtomicInteger();

        int tc = clients.length;

        CountDownLatch lockLatch = new CountDownLatch(1);
        CountDownLatch commitLatch = new CountDownLatch(1);

        Ignite prim = primaryNode(0L, DEFAULT_CACHE_NAME);

        TestRecordingCommunicationSpi primSpi = TestRecordingCommunicationSpi.spi(prim);

        primSpi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message message) {
                return message instanceof GridDhtTxFinishRequest;
            }
        });

        Set<IgniteUuid> xidSet = new GridConcurrentHashSet<>();

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                int id = idx.getAndIncrement();

                Ignite client = clients[id];

                try (Transaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED, 0, 1)) {
                    xidSet.add(tx.xid());

                    IgniteCache<Long, Long> cache = client.cache(DEFAULT_CACHE_NAME);

                    if (id != 0)
                        U.awaitQuiet(lockLatch);

                    cache.invoke(0L, new IncrementClosure(), null);

                    if (id == 0) {
                        lockLatch.countDown();

                        U.awaitQuiet(commitLatch);

                        doSleep(500); // Wait until candidates will enqueue.
                    }

                    tx.commit();
                }
                catch (Exception e) {
                    assertTrue(X.hasCause(e, TransactionTimeoutException.class));
                }

                progress.increment();

            }
        }, tc, "invoke-thread");

        U.awaitQuiet(lockLatch);

        commitLatch.countDown();

        primSpi.waitForBlocked(clients.length);

        // Unblock only finish messages from clients from 2 to 4.
        primSpi.stopBlock(true, new IgnitePredicate<T2<ClusterNode, GridIoMessage>>() {
            @Override public boolean apply(T2<ClusterNode, GridIoMessage> objects) {
                GridIoMessage iom = objects.get2();

                Message m = iom.message();

                if (m instanceof GridDhtTxFinishRequest) {
                    GridDhtTxFinishRequest r = (GridDhtTxFinishRequest)m;

                    return !r.nearNodeId().equals(clients[0].cluster().localNode().id());
                }

                return true;
            }
        });

        // Wait until queue is stable
        for (Ignite ignite : G.allGrids()) {
            if (ignite.configuration().isClientMode())
                continue;

            Collection<IgniteInternalTx> txs = ((IgniteEx)ignite).context().cache().context().tm().activeTransactions();

            waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    for (IgniteInternalTx tx : txs)
                        if (!tx.local()) {
                            IgniteTxEntry entry = tx.writeEntries().iterator().next();

                            GridCacheEntryEx cached = entry.cached();

                            Collection<GridCacheMvccCandidate> candidates = cached.remoteMvccSnapshot();

                            if (candidates.size() != clients.length)
                                return false;
                        }

                    return true;
                }
            }, 10_000);
        }

        CommandHandler h = new CommandHandler();

        // Check listing.
        validate(h, map -> {
            for (int i = 0; i < cnt; i++) {
                IgniteEx grid = grid(i);

                // Skip primary.
                if (grid.localNode().id().equals(prim.cluster().localNode().id()))
                    continue;

                VisorTxTaskResult res = map.get(grid.localNode());

                List<VisorTxInfo> infos = res.getInfos()
                    .stream()
                    .filter(info -> xidSet.contains(info.getNearXid()))
                    .collect(Collectors.toList());

                // Validate queue length on backups.
                assertEquals(clients.length, infos.size());
            }
        }, "--tx");

        // Check kill.
        validate(h, map -> {
            // No-op.
        }, "--tx", "--kill");

        // Wait for all remote txs to finish.
        for (Ignite ignite : G.allGrids()) {
            if (ignite.configuration().isClientMode())
                continue;

            Collection<IgniteInternalTx> txs = ((IgniteEx)ignite).context().cache().context().tm().activeTransactions();

            for (IgniteInternalTx tx : txs)
                if (!tx.local())
                    tx.finishFuture().get();
        }

        // Unblock finish message from client1.
        primSpi.stopBlock(true);

        fut.get();

        Long cur = (Long)clients[0].cache(DEFAULT_CACHE_NAME).get(0L);

        assertEquals(tc - 1, cur.longValue());

        checkUserFutures();
    }

    /**
     * Test baseline add items works via control.sh
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBaselineAddOnNotActiveCluster() throws Exception {
        Ignite ignite = startGrid(1);

        assertFalse(ignite.cluster().active());

        String consistentIDs = getTestIgniteInstanceName(1);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute("--baseline", "add", consistentIDs));

        assertContains(log, testOut.toString(), "Changing BaselineTopology on inactive cluster is not allowed.");

        consistentIDs =
            getTestIgniteInstanceName(1) + ", " +
                getTestIgniteInstanceName(2) + "," +
                getTestIgniteInstanceName(3);

        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute("--baseline", "add", consistentIDs));

        String testOutStr = testOut.toString();

        // Ignite instase 1 can be logged only in arguments list.
        boolean isInstanse1Found = Arrays.stream(testOutStr.split("\n"))
                                        .filter(s -> s.contains("Arguments:"))
                                        .noneMatch(s -> s.contains(getTestIgniteInstanceName() + "1"));

        assertTrue(testOutStr, testOutStr.contains("Node not found for consistent ID:"));
        assertFalse(testOutStr, isInstanse1Found);
    }

    /** */
    @Test
    public void testIdleVerifyCheckCrcFailsOnNotIdleCluster() throws Exception {
        checkpointFreq = 100L;

        IgniteEx node = startGrids(2);

        node.cluster().active(true);

        IgniteCache cache = node.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setBackups(1)
            .setName(DEFAULT_CACHE_NAME)
        );

        AtomicBoolean stopFlag = new AtomicBoolean();

        Thread loadThread = new Thread(() -> {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            while (!stopFlag.get()) {
                cache.put(rnd.nextInt(), rnd.nextInt());

                if (Thread.interrupted())
                    break;
            }
        });

        try {
            loadThread.start();

            doSleep(checkpointFreq);

            injectTestSystemOut();

            assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "--check-crc"));
        }
        finally {
            stopFlag.set(true);

            loadThread.join();
        }

        String out = testOut.toString();

        assertContains(log, out, "idle_verify failed");
        assertContains(log, out, "See log for additional information.");

        String logFileName = (out.split("See log for additional information. ")[1]).split(".txt")[0];

        String logFile = new String(Files.readAllBytes(new File(logFileName + ".txt").toPath()));

        assertContains(log, logFile, "Checkpoint with dirty pages started! Cluster not idle!");
    }

    /**
     * Tests that idle verify print partitions info when node failing.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIdleVerifyDumpWhenNodeFailing() throws Exception {
        Ignite ignite = startGrids(3);

        Ignite unstable = startGrid("unstable");

        ignite.cluster().active(true);

        createCacheAndPreload(ignite, 100);

        for (int i = 0; i < 3; i++) {
            TestRecordingCommunicationSpi.spi(unstable).blockMessages(GridJobExecuteResponse.class,
                getTestIgniteInstanceName(i));
        }

        injectTestSystemOut();

        IgniteInternalFuture fut = runAsync(() ->
            assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "--dump")));

        TestRecordingCommunicationSpi.spi(unstable).waitForBlocked();

        UUID unstableNodeId = unstable.cluster().localNode().id();

        unstable.close();

        fut.get();

        checkExceptionMessageOnReport(unstableNodeId);
    }

    /**
     * Tests that idle verify print partitions info when several nodes failing at same time.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIdleVerifyDumpWhenSeveralNodesFailing() throws Exception {
        int nodes = 6;

        Ignite ignite = startGrids(nodes);

        List<Ignite> unstableNodes = new ArrayList<>(nodes / 2);

        for (int i = 0; i < nodes; i++) {
            if (i % 2 == 1)
                unstableNodes.add(ignite(i));
        }

        ignite.cluster().active(true);

        createCacheAndPreload(ignite, 100);

        for (Ignite unstable : unstableNodes) {
            for (int i = 0; i < nodes; i++) {
                TestRecordingCommunicationSpi.spi(unstable).blockMessages(GridJobExecuteResponse.class,
                    getTestIgniteInstanceName(i));
            }
        }

        injectTestSystemOut();

        IgniteInternalFuture fut = runAsync(
            () -> assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "--dump"))
        );

        List<UUID> unstableNodeIds = new ArrayList<>(nodes / 2);

        for (Ignite unstable : unstableNodes) {
            TestRecordingCommunicationSpi.spi(unstable).waitForBlocked();

            unstableNodeIds.add(unstable.cluster().localNode().id());

            unstable.close();
        }

        fut.get();

        for (UUID unstableId : unstableNodeIds)
            checkExceptionMessageOnReport(unstableId);
    }

    /** */
    @Test
    public void testCacheIdleVerifyCrcWithCorruptedPartition() throws Exception {
        testCacheIdleVerifyWithCorruptedPartition("--cache", "idle_verify", "--check-crc");

        String out = testOut.toString();

        assertContains(log, out, "idle_verify failed on 1 node.");
        assertContains(log, out, "See log for additional information.");
    }

    /** */
    @Test
    public void testCacheIdleVerifyDumpCrcWithCorruptedPartition() throws Exception {
        testCacheIdleVerifyWithCorruptedPartition("--cache", "idle_verify", "--dump", "--check-crc");

        String parts[] = testOut.toString().split("VisorIdleVerifyDumpTask successfully written output to '");

        assertEquals(2, parts.length);

        String dumpFile = parts[1].split("\\.")[0] + ".txt";

        for (String line : Files.readAllLines(new File(dumpFile).toPath()))
            testOut.write(line.getBytes());

        String outputStr = testOut.toString();

        assertContains(log,outputStr, "idle_verify failed on 1 node.");
        assertContains(log, outputStr, "idle_verify check has finished, no conflicts have been found.");
    }

    /** */
    private void corruptPartition(File partitionsDir) throws IOException {
        ThreadLocalRandom rand = ThreadLocalRandom.current();

        for (File partFile : partitionsDir.listFiles((d, n) -> n.startsWith("part"))) {
            try (RandomAccessFile raf = new RandomAccessFile(partFile, "rw")) {
                byte[] buf = new byte[1024];

                rand.nextBytes(buf);

                raf.seek(4096 * 2 + 1);

                raf.write(buf);
            }
        }
    }

    /** */
    private void testCacheIdleVerifyWithCorruptedPartition(String... args) throws Exception {
        Ignite ignite = startGrids(2);

        ignite.cluster().active(true);

        createCacheAndPreload(ignite, 1000);

        Serializable consistId = ignite.configuration().getConsistentId();

        File partitionsDir = U.resolveWorkDirectory(
            ignite.configuration().getWorkDirectory(),
            "db/" + consistId + "/cache-" + DEFAULT_CACHE_NAME,
            false
        );

        stopGrid(0);

        corruptPartition(partitionsDir);

        startGrid(0);

        awaitPartitionMapExchange();

        forceCheckpoint();

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute(args));
    }

    /**
     * Try to finds node failed exception message on output report.
     *
     * @param unstableNodeId Unstable node id.
     */
    private void checkExceptionMessageOnReport(UUID unstableNodeId) throws IOException {
        Matcher fileNameMatcher = dumpFileNameMatcher();

        if (fileNameMatcher.find()) {
            String dumpWithConflicts = new String(Files.readAllBytes(Paths.get(fileNameMatcher.group(1))));

            assertContains(log, dumpWithConflicts, "Idle verify failed on nodes:");

            assertContains(log, dumpWithConflicts, "Node ID: " + unstableNodeId);
        }
        else
            fail("Should be found dump with conflicts");
    }

    /**
     * Tests that idle verify print partitions info over none-persistence client caches.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIdleVerifyDumpForCorruptedDataOnNonePersistenceClientCache() throws Exception {
        int parts = 32;

        dataRegionConfiguration = new DataRegionConfiguration()
            .setName("none-persistence-region");

        IgniteEx ignite = startGrids(3);

        ignite.cluster().active(true);

        IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, parts))
            .setBackups(2)
            .setName(DEFAULT_CACHE_NAME)
            .setDataRegionName("none-persistence-region"));

        // Adding some assignments without deployments.
        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        injectTestSystemOut();

        GridCacheContext<Object, Object> cacheCtx = ignite.cachex(DEFAULT_CACHE_NAME).context();

        corruptDataEntry(cacheCtx, 0, true, false);

        corruptDataEntry(cacheCtx, parts / 2, false, true);

        assertEquals(
            EXIT_CODE_OK,
            execute("--cache", "idle_verify", "--dump", "--cache-filter", "NOT_PERSISTENT")
        );

        Matcher fileNameMatcher = dumpFileNameMatcher();

        if (fileNameMatcher.find()) {
            String dumpWithConflicts = new String(Files.readAllBytes(Paths.get(fileNameMatcher.group(1))));

            assertContains(log, dumpWithConflicts, "found 1 conflict partitions: [counterConflicts=0, " +
                "hashConflicts=1]");
        }
        else
            fail("Should be found dump with conflicts");
    }

    /**
     * @return Build matcher for dump file name.
     */
    @NotNull private Matcher dumpFileNameMatcher() {
        Pattern fileNamePattern = Pattern.compile(".*VisorIdleVerifyDumpTask successfully written output to '(.*)'");

        return fileNamePattern.matcher(testOut.toString());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIdleVerifyMovingParts() throws Exception {
        IgniteEx ignite = startGrids(2);

        ignite.cluster().active(true);

        int parts = 32;

        IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, parts))
            .setBackups(1)
            .setName(DEFAULT_CACHE_NAME)
            .setRebalanceDelay(10_000));

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));

        assertContains(log, testOut.toString(), "no conflicts have been found");

        startGrid(2);

        resetBaselineTopology();

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));

        assertContains(log, testOut.toString(), "MOVING partitions");
    }

    /** */
    @Test
    public void testCacheSequence() throws Exception {
        Ignite ignite = startGrid();

        ignite.cluster().active(true);

        Ignite client = startGrid("client");

        final IgniteAtomicSequence seq1 = client.atomicSequence("testSeq", 1, true);
        seq1.get();

        final IgniteAtomicSequence seq2 = client.atomicSequence("testSeq2", 10, true);
        seq2.get();

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "list", "testSeq.*", "--seq"));

        String out = testOut.toString();

        assertContains(log, out, "testSeq");
        assertContains(log, out, "testSeq2");
    }

    /**
     * @param h Handler.
     * @param validateClo Validate clo.
     * @param args Args.
     */
    private void validate(CommandHandler h, IgniteInClosure<Map<ClusterNode, VisorTxTaskResult>> validateClo,
        String... args) {
        assertEquals(EXIT_CODE_OK, execute(h, args));

        validateClo.apply(h.getLastOperationResult());
    }

    /**
     * @param from From.
     * @param cnt Count.
     */
    private Map<Object, Object> generate(int from, int cnt) {
        Map<Object, Object> map = new TreeMap<>();

        for (int i = 0; i < cnt; i++)
            map.put(i + from, i + from);

        return map;
    }

    /**
     * Test execution of --diagnostic command.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testDiagnosticPageLocksTracker() throws Exception {
        Ignite ignite = startGrids(4);

        Collection<ClusterNode> nodes = ignite.cluster().nodes();

        List<ClusterNode> nodes0 = new ArrayList<>(nodes);

        ClusterNode node0 = nodes0.get(0);
        ClusterNode node1 = nodes0.get(1);
        ClusterNode node2 = nodes0.get(2);
        ClusterNode node3 = nodes0.get(3);

        ignite.cluster().active(true);

        assertEquals(
            EXIT_CODE_OK,
            execute("--diagnostic")
        );

        assertEquals(
            EXIT_CODE_OK,
            execute("--diagnostic", "help")
        );

        // Dump locks only on connected node to default path.
        assertEquals(
            EXIT_CODE_OK,
            execute("--diagnostic", "pageLocks", "dump")
        );

        // Check file dump in default path.
        checkNumberFiles(defaultDiagnosticDir, 1);

        assertEquals(
            EXIT_CODE_OK,
            execute("--diagnostic", "pageLocks", "dump_log")
        );

        // Dump locks only on connected node to specific path.
        assertEquals(
            EXIT_CODE_OK,
            execute("--diagnostic", "pageLocks", "dump", "--path", customDiagnosticDir.getAbsolutePath())
        );

        // Check file dump in specific path.
        checkNumberFiles(customDiagnosticDir, 1);

        // Dump locks only all nodes.
        assertEquals(
            EXIT_CODE_OK,
            execute("--diagnostic", "pageLocks", "dump", "--all")
        );

        // Current cluster 4 nodes -> 4 files + 1 from previous operation.
        checkNumberFiles(defaultDiagnosticDir, 5);

        assertEquals(
            EXIT_CODE_OK,
            execute("--diagnostic", "pageLocks", "dump_log", "--all")
        );

        assertEquals(
            EXIT_CODE_OK,
            execute("--diagnostic", "pageLocks", "dump",
                "--path", customDiagnosticDir.getAbsolutePath(), "--all")
        );

        // Current cluster 4 nodes -> 4 files + 1 from previous operation.
        checkNumberFiles(customDiagnosticDir, 5);

        // Dump locks only 2 nodes use nodeIds as arg.
        assertEquals(
            EXIT_CODE_OK,
            execute("--diagnostic", "pageLocks", "dump",
                "--nodes", node0.id().toString() + "," + node2.id().toString())
        );

        // Dump locks only for 2 nodes -> 2 files + 5 from previous operation.
        checkNumberFiles(defaultDiagnosticDir, 7);

        // Dump locks only for 2 nodes use constIds as arg.
        assertEquals(
            EXIT_CODE_OK,
            execute("--diagnostic", "pageLocks", "dump",
                "--nodes", node0.consistentId().toString() + "," + node2.consistentId().toString())
        );

        assertEquals(
            EXIT_CODE_OK,
            execute("--diagnostic", "pageLocks", "dump_log",
                "--nodes", node1.id().toString() + "," + node3.id().toString())
        );

        assertEquals(
            EXIT_CODE_OK,
            execute(
                "--diagnostic", "pageLocks", "dump",
                "--path", customDiagnosticDir.getAbsolutePath(),
                "--nodes", node1.consistentId().toString() + "," + node3.consistentId().toString())
        );

        // Dump locks only for 2 nodes -> 2 files + 5 from previous operation.
        checkNumberFiles(customDiagnosticDir, 7);
    }

    /**
     * @param dir Directory.
     * @param numberFiles Number of files.
     */
    private void checkNumberFiles(File dir, int numberFiles) {
        File[] files = dir.listFiles((d, name) -> name.startsWith(ToFileDumpProcessor.PREFIX_NAME));

        assertEquals(numberFiles, files.length);

        for (int i = 0; i < files.length; i++)
            assertTrue(files[i].length() > 0);
    }

    /**
     * Starts several long transactions in order to test --tx command. Transactions will last until unlock latch is
     * released: first transaction will wait for unlock latch directly, some others will wait for key lock acquisition.
     *
     * @param lockLatch Lock latch. Will be released inside body of the first transaction.
     * @param unlockLatch Unlock latch. Should be released externally. First transaction won't be finished until unlock
     * latch is released.
     * @param topChangeBeforeUnlock <code>true</code> should be passed if cluster topology is expected to change between
     * method call and unlock latch release. Commit of the first transaction will be asserted to fail in such case.
     * @return Future to be completed after finish of all started transactions.
     */
    private IgniteInternalFuture<?> startTransactions(
        String testName,
        CountDownLatch lockLatch,
        CountDownLatch unlockLatch,
        boolean topChangeBeforeUnlock
    ) throws Exception {
        IgniteEx client = grid("client");

        AtomicInteger idx = new AtomicInteger();

        return multithreadedAsync(new Runnable() {
            @Override public void run() {
                int id = idx.getAndIncrement();

                switch (id) {
                    case 0:
                        try (Transaction tx = grid(0).transactions().txStart()) {
                            grid(0).cache(DEFAULT_CACHE_NAME).putAll(generate(0, 100));

                            lockLatch.countDown();

                            U.awaitQuiet(unlockLatch);

                            tx.commit();

                            if (topChangeBeforeUnlock)
                                fail("Commit must fail");
                        }
                        catch (Exception e) {
                            if (topChangeBeforeUnlock)
                                assertTrue(X.hasCause(e, TransactionRollbackException.class));
                            else
                                throw e;
                        }

                        break;
                    case 1:
                        U.awaitQuiet(lockLatch);

                        doSleep(3000);

                        try (Transaction tx = grid(0).transactions().withLabel("label1").txStart(PESSIMISTIC, READ_COMMITTED, Integer.MAX_VALUE, 0)) {
                            grid(0).cache(DEFAULT_CACHE_NAME).putAll(generate(200, 110));

                            grid(0).cache(DEFAULT_CACHE_NAME).put(0, 0);
                        }

                        break;
                    case 2:
                        try (Transaction tx = grid(1).transactions().txStart()) {
                            U.awaitQuiet(lockLatch);

                            grid(1).cache(DEFAULT_CACHE_NAME).put(0, 0);
                        }

                        break;
                    case 3:
                        try (Transaction tx = client.transactions().withLabel("label2").txStart(OPTIMISTIC, READ_COMMITTED, 0, 0)) {
                            U.awaitQuiet(lockLatch);

                            client.cache(DEFAULT_CACHE_NAME).putAll(generate(100, 10));

                            client.cache(DEFAULT_CACHE_NAME).put(0, 0);

                            tx.commit();
                        }

                        break;
                }
            }
        }, 4, "tx-thread-" + testName);
    }

    /** */
    private static class IncrementClosure implements EntryProcessor<Long, Long, Void> {
        /** {@inheritDoc} */
        @Override public Void process(
            MutableEntry<Long, Long> entry,
            Object... arguments
        ) throws EntryProcessorException {
            entry.setValue(entry.exists() ? entry.getValue() + 1 : 0);

            return null;
        }
    }

    /**
     * Corrupts data entry.
     *
     * @param ctx Context.
     * @param key Key.
     * @param breakCntr Break counter.
     * @param breakData Break data.
     */
    private void corruptDataEntry(
        GridCacheContext<Object, Object> ctx,
        Object key,
        boolean breakCntr,
        boolean breakData
    ) {
        int partId = ctx.affinity().partition(key);

        try {
            long updateCntr = ctx.topology().localPartition(partId).updateCounter();

            Object valToPut = ctx.cache().keepBinary().get(key);

            if (breakCntr)
                updateCntr++;

            if (breakData)
                valToPut = valToPut.toString() + " broken";

            // Create data entry
            DataEntry dataEntry = new DataEntry(
                ctx.cacheId(),
                new KeyCacheObjectImpl(key, null, partId),
                new CacheObjectImpl(valToPut, null),
                GridCacheOperation.UPDATE,
                new GridCacheVersion(),
                new GridCacheVersion(),
                0L,
                partId,
                updateCntr
            );

            GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)ctx.shared().database();

            db.checkpointReadLock();

            try {
                U.invoke(GridCacheDatabaseSharedManager.class, db, "applyUpdate", ctx, dataEntry);
            }
            finally {
                db.checkpointReadUnlock();
            }
        }
        catch (IgniteCheckedException e) {
            e.printStackTrace();
        }
    }

    /** */
    @Test
    public void testKillHangingLocalTransactions() throws Exception {
        Ignite ignite = startGridsMultiThreaded(2);

        ignite.cluster().active(true);

        Ignite client = startGrid("client");

        client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME).
            setAtomicityMode(TRANSACTIONAL).
            setWriteSynchronizationMode(FULL_SYNC).
            setAffinity(new RendezvousAffinityFunction(false, 64)));

        Ignite prim = primaryNode(0L, DEFAULT_CACHE_NAME);

        // Blocks lock response to near node.
        TestRecordingCommunicationSpi.spi(prim).blockMessages(GridNearLockResponse.class, client.name());

        TestRecordingCommunicationSpi.spi(client).blockMessages(GridNearTxFinishRequest.class, prim.name());

        GridNearTxLocal clientTx = null;

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED, 2000, 1)) {
            clientTx = ((TransactionProxyImpl)tx).tx();

            client.cache(DEFAULT_CACHE_NAME).put(0L, 0L);

            fail();
        }
        catch (Exception e) {
            assertTrue(X.hasCause(e, TransactionTimeoutException.class));
        }

        assertNotNull(clientTx);

        IgniteEx primEx = (IgniteEx)prim;

        IgniteInternalTx tx0 = primEx.context().cache().context().tm().activeTransactions().iterator().next();

        assertNotNull(tx0);

        CommandHandler h = new CommandHandler();

        validate(h, map -> {
            ClusterNode node = grid(0).cluster().localNode();

            VisorTxTaskResult res = map.get(node);

            for (VisorTxInfo info : res.getInfos())
                assertEquals(tx0.xid(), info.getXid());

            assertEquals(1, map.size());
        }, "--tx", "--xid", tx0.xid().toString(), "--kill");

        tx0.finishFuture().get();

        TestRecordingCommunicationSpi.spi(prim).stopBlock();

        TestRecordingCommunicationSpi.spi(client).stopBlock();

        IgniteInternalFuture<?> nearFinFut = U.field(clientTx, "finishFut");

        nearFinFut.get();

        checkUserFutures();
    }

    /**
     * Verify that in case of setting baseline topology with offline node among others
     * {@link IgniteException} is thrown.
     *
     * @throws Exception If failed.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void setConsistenceIdsWithOfflineBaselineNode() throws Exception {
        Ignite ignite = startGrids(2);

        ignite.cluster().active(true);

        ignite(0).createCache(defaultCacheConfiguration().setNodeFilter(
            (IgnitePredicate<ClusterNode>)node -> node.attribute("some-attr") != null));

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR,
            execute("--baseline", "set", "non-existing-node-id ," + consistentIds(ignite)));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIdleVerifyPrintLostPartitions() throws Exception {
        IgniteEx ignite = startGrids(3);

        ignite.cluster().active(true);

        ignite.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 16))
            .setCacheMode(PARTITIONED)
            .setPartitionLossPolicy(READ_ONLY_SAFE)
            .setBackups(1));

        try (IgniteDataStreamer streamer = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < 10000; i++)
                streamer.addData(i, new byte[i]);
        }

        String g1Name = grid(1).name();

        stopGrid(1);

        cleanPersistenceDir(g1Name);

        //Start node 2 with empty PDS. Rebalance will be started.
        startGrid(1);

        //During rebalance stop node 3. Rebalance will be stopped which lead to lost partitions.
        stopGrid(2);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "--yes"));

        assertContains(log, testOut.toString(), "LOST partitions:");
    }
}
