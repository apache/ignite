/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.util;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.GridJobExecuteResponse;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.client.util.GridConcurrentHashSet;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.commandline.CommandList;
import org.apache.ignite.internal.commandline.argument.CommandArg;
import org.apache.ignite.internal.commandline.cache.CacheSubcommands;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
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
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.datastructures.GridCacheInternalKeyImpl;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.tx.VisorTxInfo;
import org.apache.ignite.internal.visor.tx.VisorTxTaskResult;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.SystemPropertiesRule;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.apache.ignite.transactions.TransactionState;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import static java.io.File.separatorChar;
import static java.util.Arrays.asList;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXPERIMENTAL_COMMAND;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_ILLEGAL_SATE_ERROR;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.internal.commandline.CommandHandler.UTILITY_NAME;
import static org.apache.ignite.internal.commandline.CommandList.WAL;
import static org.apache.ignite.internal.commandline.OutputFormat.MULTI_LINE;
import static org.apache.ignite.internal.commandline.OutputFormat.SINGLE_LINE;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.HELP;
import static org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor.DEFAULT_TARGET_FOLDER;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertNotContains;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/**
 * Command line handler test.
 */
public class GridCommandHandlerTest extends GridCommandHandlerAbstractTest {
    /** */
    @Rule public final TestRule methodRule = new SystemPropertiesRule();

    /** */
    private File defaultDiagnosticDir;
    /** */
    private File customDiagnosticDir;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        initDiagnosticDir();

        cleanDiagnosticDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanDiagnosticDir();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void initDiagnosticDir() throws IgniteCheckedException {
        defaultDiagnosticDir = new File(U.defaultWorkDirectory()
            + separatorChar + DEFAULT_TARGET_FOLDER + separatorChar);

        customDiagnosticDir = new File(U.defaultWorkDirectory()
            + separatorChar + "diagnostic_test_dir" + separatorChar);
    }

    /**
     * Clean diagnostic directories.
     */
    private void cleanDiagnosticDir() {
        U.delete(defaultDiagnosticDir);
        U.delete(customDiagnosticDir);
    }

    /** {@inheritDoc} */
    @Override public String getTestIgniteInstanceName() {
        return "gridCommandHandlerTest";
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

        testOut.reset();

        assertEquals(EXIT_CODE_OK, execute("--baseline", "--port", "11212"));

        crdStr = findCrdInfo();

        assertEquals("(Coordinator: ConsistentId=" +
            grid(1).cluster().localNode().consistentId() + ", Order=2)", crdStr);

        startGrid(0);

        testOut.reset();

        assertEquals(EXIT_CODE_OK, execute("--baseline", "--port", "11212"));

        crdStr = findCrdInfo();

        assertEquals("(Coordinator: ConsistentId=" +
            grid(1).cluster().localNode().consistentId() + ", Order=2)", crdStr);

        stopGrid(1);

        testOut.reset();

        assertEquals(EXIT_CODE_OK, execute("--baseline", "--port", "11211"));

        crdStr = findCrdInfo();

        assertEquals("(Coordinator: ConsistentId=" +
            grid(0).cluster().localNode().consistentId() + ", Order=4)", crdStr);

    }

    /**
     * Very basic tests for running the command in different enviroment which other command are running in.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFindAndDeleteGarbage() throws Exception {
        Ignite ignite = startGrids(2);

        ignite.cluster().active(true);

        injectTestSystemOut();

        ignite.createCaches(Arrays.asList(
            new CacheConfiguration<>("garbage1").setGroupName("groupGarbage"),
            new CacheConfiguration<>("garbage2").setGroupName("groupGarbage")));

        assertEquals(EXIT_CODE_OK, execute("--cache", "find_garbage", "--port", "11212"));

        assertContains(log, testOut.toString(), "garbage not found");

        testOut.reset();

        assertEquals(EXIT_CODE_OK, execute("--cache", "find_garbage",
            ignite(0).localNode().id().toString(), "--port", "11212"));

        assertContains(log, testOut.toString(), "garbage not found");

        testOut.reset();

        assertEquals(EXIT_CODE_OK, execute("--cache", "find_garbage",
            "groupGarbage", "--port", "11212"));

        assertContains(log, testOut.toString(), "garbage not found");
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

        assertEquals(EXIT_CODE_ILLEGAL_SATE_ERROR, execute("--baseline", "add", "non-existent-id"));

        Ignite other = startGrid(2);

        assertEquals(EXIT_CODE_OK, execute("--baseline", "add", consistentIds(other)));
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
     * Test that baseline auto_adjustment settings update works via control.sh
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBaselineAutoAdjustmentSettings() throws Exception {
        Ignite ignite = startGrid();

        ignite.cluster().active(true);

        IgniteCluster cl = ignite.cluster();

        assertFalse(cl.isBaselineAutoAdjustEnabled());

        long timeout = cl.baselineAutoAdjustTimeout();

        assertEquals(EXIT_CODE_OK, execute(
            "--baseline",
            "auto_adjust",
            "enable",
            "timeout",
            Long.toString(timeout + 1)
        ));

        assertTrue(cl.isBaselineAutoAdjustEnabled());

        assertEquals(timeout + 1, cl.baselineAutoAdjustTimeout());

        assertEquals(EXIT_CODE_OK, execute("--baseline", "auto_adjust", "disable"));

        assertFalse(cl.isBaselineAutoAdjustEnabled());

        assertEquals(timeout + 1, cl.baselineAutoAdjustTimeout());

        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute("--baseline", "auto_adjust"));

        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute("--baseline", "auto_adjust", "true"));

        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute("--baseline", "auto_adjust", "enable", "x"));

        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute("--baseline", "auto_adjust", "disable", "x"));

        log.info("================================================");
        System.out.println(testOut.toString());

        log.info("================================================");
    }

    /**
     * Test that if baseline auto_adjustment is enable, control.sh can not change a state manual.
     *
     * @throws Exception If failed.
     */
    @Test
    public void shouldReturnErrorCodeForManualSetInBaselineAutoAdjustmentEnable() throws Exception {
        Ignite ignite = startGrid();

        ignite.cluster().active(true);

        IgniteCluster cl = ignite.cluster();

        long timeout = cl.baselineAutoAdjustTimeout();

        assertEquals(EXIT_CODE_OK, execute(
            "--baseline",
            "auto_adjust",
            "enable",
            "timeout",
            Long.toString(timeout + 1)
        ));

        assertTrue(cl.isBaselineAutoAdjustEnabled());

        assertEquals(EXIT_CODE_ILLEGAL_SATE_ERROR, execute("--baseline", "version", "1"));
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

        assertEquals(EXIT_CODE_OK, execute("--baseline", "auto_adjust", "enable", "timeout", "2000"));

        assertEquals(3, ignite.cluster().currentBaselineTopology().size());

        stopGrid(2);

        assertEquals(3, ignite.cluster().currentBaselineTopology().size());

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
     * Smoke test for --tx --info command.
     */
    @Test
    public void testTransactionInfo() throws Exception {
        Ignite ignite = startGridsMultiThreaded(5);

        ignite.cluster().active(true);

        Ignite client = startGrid("client");

        client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL).setBackups(2).setWriteSynchronizationMode(FULL_SYNC));

        for (Ignite ig : G.allGrids())
            assertNotNull(ig.cache(DEFAULT_CACHE_NAME));

        CountDownLatch lockLatch = new CountDownLatch(1);
        CountDownLatch unlockLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut = startTransactions("testTransactionInfo", lockLatch, unlockLatch, false);

        try {
            U.awaitQuiet(lockLatch);

            doSleep(3000); // Should be more than enough for all transactions to appear in contexts.

            Set<GridCacheVersion> nearXids = new HashSet<>();

            for (int i = 0; i < 5; i++) {
                IgniteEx grid = grid(i);

                IgniteTxManager tm = grid.context().cache().context().tm();

                for (IgniteInternalTx tx : tm.activeTransactions())
                    nearXids.add(tx.nearXidVersion());
            }

            injectTestSystemOut();

            for (GridCacheVersion nearXid : nearXids)
                assertEquals(EXIT_CODE_OK, execute("--tx", "--info", nearXid.toString()));

            String out = testOut.toString();

            for (GridCacheVersion nearXid : nearXids)
                assertContains(log, out, nearXid.toString());
        }
        finally {
            unlockLatch.countDown();

            fut.get();
        }
    }

    /**
     * Smoke test for historical mode of --tx --info command.
     */
    @Test
    public void testTransactionHistoryInfo() throws Exception {
        Ignite ignite = startGridsMultiThreaded(2);

        ignite.cluster().active(true);

        Ignite client = startGrid("client");

        client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL).setBackups(2).setWriteSynchronizationMode(FULL_SYNC));

        for (Ignite ig : G.allGrids())
            assertNotNull(ig.cache(DEFAULT_CACHE_NAME));

        CountDownLatch lockLatch = new CountDownLatch(1);
        CountDownLatch unlockLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut = startTransactions("testTransactionHistoryInfo", lockLatch, unlockLatch, false);

        U.awaitQuiet(lockLatch);

        doSleep(3000); // Should be more than enough for all transactions to appear in contexts.

        Set<GridCacheVersion> nearXids = new HashSet<>();

        for (int i = 0; i < 2; i++) {
            IgniteEx grid = grid(i);

            IgniteTxManager tm = grid.context().cache().context().tm();

            for (IgniteInternalTx tx : tm.activeTransactions())
                nearXids.add(tx.nearXidVersion());
        }

        unlockLatch.countDown();

        fut.get();

        doSleep(3000); // Should be more than enough for all transactions to disappear from contexts after finish.

        injectTestSystemOut();

        boolean commitMatched = false;
        boolean rollbackMatched = false;

        for (GridCacheVersion nearXid : nearXids) {
            assertEquals(EXIT_CODE_OK, execute("--tx", "--info", nearXid.toString()));

            String out = testOut.toString();

            testOut.reset();

            assertContains(log, out, "Transaction was found in completed versions history of the following nodes:");

            if (out.contains(TransactionState.COMMITTED.name())) {
                commitMatched = true;

                assertNotContains(log, out, TransactionState.ROLLED_BACK.name());
            }

            if (out.contains(TransactionState.ROLLED_BACK.name())) {
                rollbackMatched = true;

                assertNotContains(log, out, TransactionState.COMMITTED.name());
            }

        }

        assertTrue(commitMatched);
        assertTrue(rollbackMatched);
    }

    /**
     *
     */
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

        assertEquals(EXIT_CODE_ILLEGAL_SATE_ERROR, execute("--baseline", "add", consistentIDs));

        String testOutStr = testOut.toString();

        // Ignite instase 1 can be logged only in arguments list.
        boolean isInstanse1Found = Arrays.stream(testOutStr.split("\n"))
                                        .filter(s -> s.contains("Arguments:"))
                                        .noneMatch(s -> s.contains(getTestIgniteInstanceName() + "1"));

        assertTrue(testOutStr, testOutStr.contains("Node not found for consistent ID:"));
        assertFalse(testOutStr, isInstanse1Found);
    }

    /**
     */
    @Test
    public void testCacheHelp() {
        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "help"));

        String output = testOut.toString();

        for (CacheSubcommands cmd : CacheSubcommands.values()) {
            if (cmd != HELP) {
                assertContains(log, output, cmd.toString());

                Class<? extends Enum<? extends CommandArg>> args = cmd.getCommandArgs();

                if (args != null)
                    for (Enum<? extends CommandArg> arg : args.getEnumConstants())
                        assertTrue(cmd + " " + arg, output.contains(arg.toString()));

            }
            else {
                assertContains(log, output, CommandHandler.UTILITY_NAME);

                assertNotContains(log, output, "control.sh");
            }
        }
    }

    /**
     */
    @Test
    public void testCorrectCacheOptionsNaming() {
        Pattern p = Pattern.compile("^--([a-z]+(-)?)+([a-z]+)");

        for (CacheSubcommands cmd : CacheSubcommands.values()) {
            Class<? extends Enum<? extends CommandArg>> args = cmd.getCommandArgs();

            if (args != null)
                for (Enum<? extends CommandArg> arg : args.getEnumConstants())
                    assertTrue(arg.toString(), p.matcher(arg.toString()).matches());
        }
    }

    /**
     */
    @Test
    public void testHelp() {
        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--help"));

        String testOutStr = testOut.toString();

        for (CommandList cmd : CommandList.values())
            assertContains(log, testOutStr, cmd.toString());

        assertContains(log, testOutStr, "Control utility script");
        assertContains(log, testOutStr, CommandHandler.UTILITY_NAME);

        assertNotContains(log, testOutStr, "Control.sh");
    }

    /** */
    @Test
    public void testPrintTimestampAtEndsOfExecution() {
        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute());

        String testOutStr = testOut.toString();

        assertContains(log, testOutStr, "Control utility has completed execution at: ");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIdleVerify() throws Exception {
        IgniteEx ignite = startGrids(2);

        ignite.cluster().active(true);

        createCacheAndPreload(ignite, 100);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));

        assertContains(log, testOut.toString(), "no conflicts have been found");

        HashSet<Integer> clearKeys = new HashSet<>(asList(1, 2, 3, 4, 5, 6));

        ignite.context().cache().cache(DEFAULT_CACHE_NAME).clearLocallyAll(clearKeys, true, true, true);

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));

        assertContains(log, testOut.toString(), "conflict partitions");
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
            .setCacheMode(CacheMode.PARTITIONED)
            .setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE)
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

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIdleVerifyNodeFilter() throws Exception {
        IgniteEx ignite = startGrids(3);

        ignite.cluster().active(true);

        Object lastNodeCId = ignite.localNode().consistentId();

        ignite.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setNodeFilter(node -> !node.consistentId().equals(lastNodeCId))
            .setBackups(1));

        try (IgniteDataStreamer streamer = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < 100; i++)
                streamer.addData(i, i);
        }

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", DEFAULT_CACHE_NAME));

        assertContains(log, testOut.toString(), "no conflicts have been found");
    }

    /**
     * Tests that both update counter and hash conflicts are detected.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIdleVerifyTwoConflictTypes() throws Exception {
        IgniteEx ignite = startGrids(2);

        ignite.cluster().active(true);

        createCacheAndPreload(ignite, 100);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));

        assertContains(log, testOut.toString(), "no conflicts have been found");

        GridCacheContext<Object, Object> cacheCtx = ignite.cachex(DEFAULT_CACHE_NAME).context();

        corruptDataEntry(cacheCtx, 1, true, false);

        corruptDataEntry(cacheCtx, 1 + cacheCtx.config().getAffinity().partitions() / 2, false, true);

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));

        assertContains(log, testOut.toString(), "found 2 conflict partitions");
    }

    /**
     * Tests that empty partitions with non-zero update counter are not included into the idle_verify dump.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIdleVerifyDumpSkipZerosUpdateCounters() throws Exception {
        IgniteEx ignite = startGrids(2);

        ignite.cluster().active(true);

        int emptyPartId = 31;

        // Less than parts number for ability to check skipZeros flag.
        createCacheAndPreload(ignite, emptyPartId);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "--dump", "--skip-zeros", DEFAULT_CACHE_NAME));

        Matcher fileNameMatcher = dumpFileNameMatcher();

        assertTrue(fileNameMatcher.find());

        String zeroUpdateCntrs = new String(Files.readAllBytes(Paths.get(fileNameMatcher.group(1))));

        assertContains(log, zeroUpdateCntrs, "idle_verify check has finished, found " + emptyPartId + " partitions");
        assertContains(log, zeroUpdateCntrs, "1 partitions was skipped");
        assertContains(log, zeroUpdateCntrs, "idle_verify check has finished, no conflicts have been found.");

        assertSort(emptyPartId, zeroUpdateCntrs);

        testOut.reset();

        // The result of the following cache operations is that
        // the size of the 32-th partition is equal to zero and update counter is equal to 2.
        ignite.cache(DEFAULT_CACHE_NAME).put(emptyPartId, emptyPartId);

        ignite.cache(DEFAULT_CACHE_NAME).remove(emptyPartId, emptyPartId);

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "--dump", "--skip-zeros", DEFAULT_CACHE_NAME));

        fileNameMatcher = dumpFileNameMatcher();

        assertTrue(fileNameMatcher.find());

        String nonZeroUpdateCntrs = new String(Files.readAllBytes(Paths.get(fileNameMatcher.group(1))));

        assertContains(log, nonZeroUpdateCntrs, "idle_verify check has finished, found " + 31 + " partitions");
        assertContains(log, nonZeroUpdateCntrs, "1 partitions was skipped");
        assertContains(log, nonZeroUpdateCntrs, "idle_verify check has finished, no conflicts have been found.");

        assertSort(31, zeroUpdateCntrs);

        assertEquals(zeroUpdateCntrs, nonZeroUpdateCntrs);
    }

    /**
     * Tests that idle verify print partitions info.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIdleVerifyDump() throws Exception {
        IgniteEx ignite = startGrids(3);

        ignite.cluster().active(true);

        int keysCount = 20;//less than parts number for ability to check skipZeros flag.

        createCacheAndPreload(ignite, keysCount);

        int parts = ignite.affinity(DEFAULT_CACHE_NAME).partitions();

        ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, parts))
            .setBackups(1)
            .setName(DEFAULT_CACHE_NAME + "other"));

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "--dump", DEFAULT_CACHE_NAME));

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "--dump", "--skip-zeros", DEFAULT_CACHE_NAME));

        Matcher fileNameMatcher = dumpFileNameMatcher();

        if (fileNameMatcher.find()) {
            String dumpWithZeros = new String(Files.readAllBytes(Paths.get(fileNameMatcher.group(1))));

            assertContains(log, dumpWithZeros, "idle_verify check has finished, found " + parts + " partitions");
            assertContains(log, dumpWithZeros, "Partition: PartitionKeyV2 [grpId=1544803905, grpName=default, partId=0]");
            assertContains(log, dumpWithZeros, "updateCntr=0, partitionState=OWNING, size=0, partHash=0");
            assertContains(log, dumpWithZeros, "no conflicts have been found");

            assertSort(parts, dumpWithZeros);
        }

        if (fileNameMatcher.find()) {
            String dumpWithoutZeros = new String(Files.readAllBytes(Paths.get(fileNameMatcher.group(1))));

            assertContains(log, dumpWithoutZeros, "idle_verify check has finished, found " + keysCount + " partitions");
            assertContains(log, dumpWithoutZeros, (parts - keysCount) + " partitions was skipped");
            assertContains(log, dumpWithoutZeros, "Partition: PartitionKeyV2 [grpId=1544803905, grpName=default, partId=");

            assertNotContains(log, dumpWithoutZeros, "updateCntr=0, partitionState=OWNING, size=0, partHash=0");

            assertContains(log, dumpWithoutZeros, "no conflicts have been found");

            assertSort(keysCount, dumpWithoutZeros);
        }
        else
            fail("Should be found both files");
    }

    /**
     * Common method for idle_verify tests with multiple options.
     *
     * @throws Exception if failed
     */
    @Test
    public void testCacheIdleVerifyMultipleCacheFilterOptions()
            throws Exception {
        IgniteEx ignite = startGrids(2);

        ignite.cluster().active(true);

        ignite.createCache(new CacheConfiguration<>()
                .setAffinity(new RendezvousAffinityFunction(false, 32))
                .setGroupName("shared_grp")
                .setBackups(1)
                .setName(DEFAULT_CACHE_NAME));

        ignite.createCache(new CacheConfiguration<>()
                .setAffinity(new RendezvousAffinityFunction(false, 32))
                .setGroupName("shared_grp")
                .setBackups(1)
                .setName(DEFAULT_CACHE_NAME + "_second"));

        ignite.createCache(new CacheConfiguration<>()
                .setAffinity(new RendezvousAffinityFunction(false, 64))
                .setBackups(1)
                .setName(DEFAULT_CACHE_NAME + "_third"));

        ignite.createCache(new CacheConfiguration<>()
                .setAffinity(new RendezvousAffinityFunction(false, 128))
                .setBackups(1)
                .setName("wrong_cache"));

        injectTestSystemOut();

        testCacheIdleVerifyMultipleCacheFilterOptionsCommon(
            true,
            "idle_verify check has finished, found",
            "idle_verify task was executed with the following args: caches=[], excluded=[wrong.*], cacheFilter=[SYSTEM]",
            "--cache", "idle_verify", "--dump", "--cache-filter", "SYSTEM", "--exclude-caches", "wrong.*"
        );
        testCacheIdleVerifyMultipleCacheFilterOptionsCommon(
            true,
            "idle_verify check has finished, found 96 partitions",
            null,
            "--cache", "idle_verify", "--dump", "--exclude-caches", "wrong.*"
        );
        testCacheIdleVerifyMultipleCacheFilterOptionsCommon(
            true,
            "idle_verify check has finished, found 32 partitions",
            null,
            "--cache", "idle_verify", "--dump", "shared.*"
        );
        testCacheIdleVerifyMultipleCacheFilterOptionsCommon(
            true,
            "idle_verify check has finished, found 160 partitions",
            null,
            "--cache", "idle_verify", "--dump", "shared.*,wrong.*"
        );
        testCacheIdleVerifyMultipleCacheFilterOptionsCommon(
            true,
            "idle_verify check has finished, found 160 partitions",
            null,
            "--cache", "idle_verify", "--dump", "shared.*,wrong.*", "--cache-filter", "USER"
        );
        testCacheIdleVerifyMultipleCacheFilterOptionsCommon(
            true,
            "idle_verify check has finished, found 160 partitions",
            null,
            "--cache", "idle_verify", "--dump", "shared.*,wrong.*"
        );
        testCacheIdleVerifyMultipleCacheFilterOptionsCommon(
            true,
            "There are no caches matching given filter options",
            null,
            "--cache", "idle_verify", "--exclude-caches", ".*"
        );
        testCacheIdleVerifyMultipleCacheFilterOptionsCommon(
            false,
            "Invalid cache name regexp",
            null,
            "--cache", "idle_verify", "--dump", "--exclude-caches", "["
        );
        testCacheIdleVerifyMultipleCacheFilterOptionsCommon(
            true,
            "idle_verify check has finished, no conflicts have been found.",
            null,
            "--cache", "idle_verify", "--exclude-caches", "wrong.*"
        );
        testCacheIdleVerifyMultipleCacheFilterOptionsCommon(
            true,
            "idle_verify check has finished, no conflicts have been found.",
            null,
            "--cache", "idle_verify", "--dump", "--cache-filter", "PERSISTENT"
        );
        testCacheIdleVerifyMultipleCacheFilterOptionsCommon(
            true,
            "There are no caches matching given filter options.",
            null,
            "--cache", "idle_verify", "--cache-filter", "NOT_PERSISTENT"
        );
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
     * Runs idle_verify with specified arguments and checks the dump if dump option was present.
     *
     * @param exitOk whether CommandHandler should exit without errors
     * @param outputExp expected dump output
     * @param cmdExp expected command built from command line arguments
     * @param args command handler arguments
     * @throws IOException if some of file operations failed
     */
    private void testCacheIdleVerifyMultipleCacheFilterOptionsCommon(
        boolean exitOk,
        String outputExp,
        String cmdExp,
        String... args
    ) throws IOException {
        testOut.reset();

        Set<String> argsSet = new HashSet<>(asList(args));

        int exitCode = execute(args);

        assertEquals(testOut.toString(), exitOk, EXIT_CODE_OK == exitCode);

        if (exitCode == EXIT_CODE_OK) {
            Matcher fileNameMatcher = dumpFileNameMatcher();

            if (fileNameMatcher.find()) {
                assertContains(log, argsSet, "--dump");

                Path filePath = Paths.get(fileNameMatcher.group(1));

                String dump = new String(Files.readAllBytes(filePath));

                Files.delete(filePath);

                assertContains(log, dump, outputExp);

                if (cmdExp != null)
                    assertContains(log, dump, cmdExp);
            }
            else {
                assertNotContains(log, argsSet, "--dump");

                assertContains(log, testOut.toString(), outputExp);
            }
        } else
            assertContains(log, testOut.toString(), outputExp);
    }

    /**
     * Checking sorting of partitions.
     *
     * @param expectedPartsCount Expected parts count.
     * @param output Output.
     */
    private void assertSort(int expectedPartsCount, String output) {
        Pattern partIdPattern = Pattern.compile(".*partId=([0-9]*)");
        Pattern primaryPattern = Pattern.compile("Partition instances: \\[PartitionHashRecordV2 \\[isPrimary=true");

        Matcher partIdMatcher = partIdPattern.matcher(output);
        Matcher primaryMatcher = primaryPattern.matcher(output);

        int i = 0;

        while (partIdMatcher.find()) {
            assertEquals(i++, Integer.parseInt(partIdMatcher.group(1)));
            assertTrue(primaryMatcher.find());//primary node should be first in every line
        }

        assertEquals(expectedPartsCount, i);
    }

    /**
     * Tests that idle verify print partitions info.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIdleVerifyDumpForCorruptedData() throws Exception {
        IgniteEx ignite = startGrids(3);

        ignite.cluster().active(true);

        createCacheAndPreload(ignite, 100);

        injectTestSystemOut();

        corruptingAndCheckDefaultCache(ignite, "USER", true);
    }

    /**
     * Tests that idle verify print partitions info while launched without dump option.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIdleVerifyForCorruptedData() throws Exception {
        IgniteEx ignite = startGrids(3);

        ignite.cluster().active(true);

        createCacheAndPreload(ignite, 100);

        injectTestSystemOut();

        corruptingAndCheckDefaultCache(ignite, "USER", false);
    }

    /**
     * @param ignite Ignite.
     * @param cacheFilter cacheFilter.
     * @param dump Whether idle_verify should be launched with dump option or not.
     */
    private void corruptingAndCheckDefaultCache(IgniteEx ignite, String cacheFilter, boolean dump) throws IOException {
        injectTestSystemOut();

        GridCacheContext<Object, Object> cacheCtx = ignite.cachex(DEFAULT_CACHE_NAME).context();

        corruptDataEntry(cacheCtx, 0, true, false);

        corruptDataEntry(cacheCtx, cacheCtx.config().getAffinity().partitions() / 2, false, true);

        String resReport = null;

        if (dump) {
            assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "--dump", "--cache-filter", cacheFilter));

            Matcher fileNameMatcher = dumpFileNameMatcher();

            if (fileNameMatcher.find()) {
                resReport = new String(Files.readAllBytes(Paths.get(fileNameMatcher.group(1))));

                log.info(resReport);
            }
            else
                fail("Should be found dump with conflicts");
        }
        else {
            assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "--cache-filter", cacheFilter));

            resReport = testOut.toString();
        }

        assertContains(log, resReport, "found 2 conflict partitions: [counterConflicts=1, hashConflicts=1]");
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

        IgniteInternalFuture fut = GridTestUtils.runAsync(() ->
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

        IgniteInternalFuture fut = GridTestUtils.runAsync(
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
            System.out.println(line);

        String outputStr = testOut.toString();

        assertTrue(outputStr, outputStr.contains("idle_verify failed on 1 node."));
        assertTrue(outputStr, outputStr.contains("idle_verify check has finished, no conflicts have been found."));
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
     * Creates default cache and preload some data entries.
     *
     * @param ignite Ignite.
     * @param countEntries Count of entries.
     */
    private void createCacheAndPreload(Ignite ignite, int countEntries) {
        ignite.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setBackups(1));

        try (IgniteDataStreamer streamer = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < countEntries; i++)
                streamer.addData(i, i);
        }
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
     * Tests that idle verify print partitions info over system caches.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIdleVerifyDumpForCorruptedDataOnSystemCache() throws Exception {
        int parts = 32;

        atomicConfiguration = new AtomicConfiguration()
            .setAffinity(new RendezvousAffinityFunction(false, parts))
            .setBackups(2);

        IgniteEx ignite = startGrids(3);

        ignite.cluster().active(true);

        if (!isSystemOutAlreadyInjected())
            injectTestSystemOut();

        // Adding some assignments without deployments.
        for (int i = 0; i < 100; i++) {
            ignite.semaphore("s" + i, i, false, true);

            ignite.atomicSequence("sq" + i, 0, true)
                .incrementAndGet();
        }

        CacheGroupContext storedSysCacheCtx = ignite.context().cache().cacheGroup(CU.cacheId("default-ds-group"));

        assertNotNull(storedSysCacheCtx);

        corruptDataEntry(storedSysCacheCtx.caches().get(0), new GridCacheInternalKeyImpl("sq0",
            "default-ds-group"), true, false);

        corruptDataEntry(storedSysCacheCtx.caches().get(0), new GridCacheInternalKeyImpl("sq" + parts / 2,
            "default-ds-group"), false, true);

        CacheGroupContext memorySysCacheCtx = ignite.context().cache().cacheGroup(CU.cacheId("default-volatile-ds-group"));

        assertNotNull(memorySysCacheCtx);

        corruptDataEntry(memorySysCacheCtx.caches().get(0), new GridCacheInternalKeyImpl("s0",
            "default-volatile-ds-group"), true, false);

        corruptDataEntry(memorySysCacheCtx.caches().get(0), new GridCacheInternalKeyImpl("s" + parts / 2,
            "default-volatile-ds-group"), false, true);

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "--dump", "--cache-filter", "SYSTEM"));

        Matcher fileNameMatcher = dumpFileNameMatcher();

        if (fileNameMatcher.find()) {
            String dumpWithConflicts = new String(Files.readAllBytes(Paths.get(fileNameMatcher.group(1))));

            U.log(log, dumpWithConflicts);

            assertContains(log, dumpWithConflicts, "found 4 conflict partitions: [counterConflicts=2, " +
                "hashConflicts=2]");
        }
        else
            fail("Should be found dump with conflicts");
    }

    /**
     * Tests that idle verify print partitions info over persistence client caches.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIdleVerifyDumpForCorruptedDataOnPersistenceClientCache() throws Exception {
        IgniteEx ignite = startGrids(3);

        ignite.cluster().active(true);

        createCacheAndPreload(ignite, 100);

        corruptingAndCheckDefaultCache(ignite, "PERSISTENT", true);
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
     * Tests that idle verify print partitions info with exclude cache group.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIdleVerifyDumpExcludedCacheGrp() throws Exception {
        IgniteEx ignite = startGrids(3);

        ignite.cluster().active(true);

        int parts = 32;

        IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, parts))
            .setGroupName("shared_grp")
            .setBackups(1)
            .setName(DEFAULT_CACHE_NAME));

        IgniteCache<Object, Object> secondCache = ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, parts))
            .setGroupName("shared_grp")
            .setBackups(1)
            .setName(DEFAULT_CACHE_NAME + "_second"));

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "--dump", "--exclude-caches", "shared_grp"));

        Matcher fileNameMatcher = dumpFileNameMatcher();

        if (fileNameMatcher.find()) {
            String dumpWithConflicts = new String(Files.readAllBytes(Paths.get(fileNameMatcher.group(1))));

            assertContains(log, dumpWithConflicts, "There are no caches matching given filter options");
        }
        else
            fail("Should be found dump with conflicts");
    }

    /**
     * Tests that idle verify print partitions info with exclude caches.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIdleVerifyDumpExcludedCaches() throws Exception {
        IgniteEx ignite = startGrids(3);

        ignite.cluster().active(true);

        int parts = 32;

        ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, parts))
            .setGroupName("shared_grp")
            .setBackups(1)
            .setName(DEFAULT_CACHE_NAME));

        ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, parts))
            .setGroupName("shared_grp")
            .setBackups(1)
            .setName(DEFAULT_CACHE_NAME + "_second"));

        ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, parts))
            .setBackups(1)
            .setName(DEFAULT_CACHE_NAME + "_third"));

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "--dump", "--exclude-caches", DEFAULT_CACHE_NAME
            + "," + DEFAULT_CACHE_NAME + "_second"));

        Matcher fileNameMatcher = dumpFileNameMatcher();

        if (fileNameMatcher.find()) {
            String dumpWithConflicts = new String(Files.readAllBytes(Paths.get(fileNameMatcher.group(1))));

            assertContains(log, dumpWithConflicts, "idle_verify check has finished, found 32 partitions");
            assertContains(log, dumpWithConflicts, "default_third");
            assertNotContains(log, dumpWithConflicts, "shared_grp");
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

    /**
     *
     */
    @Test
    public void testCacheContention() throws Exception {
        int cnt = 10;

        final ExecutorService svc = Executors.newFixedThreadPool(cnt);

        try {
            Ignite ignite = startGrids(2);

            ignite.cluster().active(true);

            final IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>()
                .setAffinity(new RendezvousAffinityFunction(false, 32))
                .setAtomicityMode(TRANSACTIONAL)
                .setBackups(1)
                .setName(DEFAULT_CACHE_NAME));

            final CountDownLatch l = new CountDownLatch(1);

            final CountDownLatch l2 = new CountDownLatch(1);

            svc.submit(new Runnable() {
                @Override public void run() {
                    try (final Transaction tx = ignite.transactions().txStart()) {
                        cache.put(0, 0);

                        l.countDown();

                        U.awaitQuiet(l2);

                        tx.commit();
                    }
                }
            });

            for (int i = 0; i < cnt - 1; i++) {
                svc.submit(new Runnable() {
                    @Override public void run() {
                        U.awaitQuiet(l);

                        try (final Transaction tx = ignite.transactions().txStart()) {
                            cache.get(0);

                            tx.commit();
                        }
                    }
                });
            }

            U.awaitQuiet(l);

            Thread.sleep(300);

            injectTestSystemOut();

            assertEquals(EXIT_CODE_OK, execute("--cache", "contention", "5"));

            l2.countDown();

            String out = testOut.toString();

            assertContains(log, out, "TxEntry");
            assertContains(log, out, "op=READ");
            assertContains(log, out, "op=CREATE");
            assertContains(log, out, "id=" + ignite(0).cluster().localNode().id());
            assertContains(log, out, "id=" + ignite(1).cluster().localNode().id());
        }
        finally {
            svc.shutdown();
            svc.awaitTermination(100, TimeUnit.DAYS);
        }
    }

    /**
     *
     */
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
     *
     */
    @Test
    public void testCacheGroups() throws Exception {
        Ignite ignite = startGrid();

        ignite.cluster().active(true);

        IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setBackups(1)
            .setGroupName("G100")
            .setName(DEFAULT_CACHE_NAME));

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "list", ".*", "--groups"));

        assertContains(log, testOut.toString(), "G100");
    }

    /**
     *
     */
    @Test
    public void testCacheAffinity() throws Exception {
        Ignite ignite = startGrid();

        ignite.cluster().active(true);

        IgniteCache<Object, Object> cache1 = ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setBackups(1)
            .setName(DEFAULT_CACHE_NAME));

        for (int i = 0; i < 100; i++)
            cache1.put(i, i);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "list", ".*"));

        String out = testOut.toString();

        assertContains(log, out, "cacheName=" + DEFAULT_CACHE_NAME);
        assertContains(log, out, "prim=32");
        assertContains(log, out, "mapped=32");
        assertContains(log, out, "affCls=RendezvousAffinityFunction");
    }

    /** */
    @Test
    public void testCacheConfigNoOutputFormat() throws Exception {
        testCacheConfig(null, 1, 1);
    }

    /** */
    @Test
    public void testCacheConfigSingleLineOutputFormatSingleNodeSignleCache() throws Exception {
        testCacheConfigSingleLineOutputFormat(1, 1);
    }

    /** */
    @Test
    public void testCacheConfigSingleLineOutputFormatTwoNodeSignleCache() throws Exception {
        testCacheConfigSingleLineOutputFormat(2, 1);
    }

    /** */
    @Test
    public void testCacheConfigSingleLineOutputFormatTwoNodeManyCaches() throws Exception {
        testCacheConfigSingleLineOutputFormat(2, 100);
    }

    /** */
    @Test
    public void testCacheConfigMultiLineOutputFormatSingleNodeSingleCache() throws Exception {
        testCacheConfigMultiLineOutputFormat(1, 1);
    }

    /** */
    @Test
    public void testCacheConfigMultiLineOutputFormatTwoNodeSingleCache() throws Exception {
        testCacheConfigMultiLineOutputFormat(2, 1);
    }

    /** */
    @Test
    public void testCacheConfigMultiLineOutputFormatTwoNodeManyCaches() throws Exception {
        testCacheConfigMultiLineOutputFormat(2, 100);
    }

    /** */
    private void testCacheConfigSingleLineOutputFormat(int nodesCnt, int cachesCnt) throws Exception {
        testCacheConfig("single-line", nodesCnt, cachesCnt);
    }

    /** */
    private void testCacheConfigMultiLineOutputFormat(int nodesCnt, int cachesCnt) throws Exception {
        testCacheConfig("multi-line", nodesCnt, cachesCnt);
    }

    /** */
    private void testCacheConfig(String outputFormat, int nodesCnt, int cachesCnt) throws Exception {
        assertTrue("Invalid number of nodes or caches", nodesCnt > 0 && cachesCnt > 0);

        Ignite ignite = startGrid(nodesCnt);

        ignite.cluster().active(true);

        List<CacheConfiguration> ccfgs = new ArrayList<>(cachesCnt);

        for (int i = 0; i < cachesCnt; i++) {
            ccfgs.add(
                new CacheConfiguration<>()
                    .setAffinity(new RendezvousAffinityFunction(false, 32))
                    .setBackups(1)
                    .setName(DEFAULT_CACHE_NAME + i)
            );
        }

        ignite.createCaches(ccfgs);

        IgniteCache<Object, Object> cache1 = ignite.cache(DEFAULT_CACHE_NAME + 0);

        for (int i = 0; i < 100; i++)
            cache1.put(i, i);

        injectTestSystemOut();

        int exitCode;

        if (outputFormat == null)
            exitCode = execute("--cache", "list", ".*", "--config");
        else
            exitCode = execute("--cache", "list", ".*", "--config", "--output-format", outputFormat);

        assertEquals(EXIT_CODE_OK, exitCode);

        String outStr = testOut.toString();

        if (outputFormat == null || SINGLE_LINE.text().equals(outputFormat)) {
            for (int i = 0; i < cachesCnt; i++)
                assertContains(log, outStr, "name=" + DEFAULT_CACHE_NAME + i);

            assertContains(log, outStr, "partitions=32");
            assertContains(log, outStr, "function=o.a.i.cache.affinity.rendezvous.RendezvousAffinityFunction");
        }
        else if (MULTI_LINE.text().equals(outputFormat)) {
            for (int i = 0; i < cachesCnt; i++)
                assertContains(log, outStr, "[cache = '" + DEFAULT_CACHE_NAME + i + "']");

            assertContains(log, outStr, "Affinity Partitions: 32");
            assertContains(log, outStr, "Affinity Function: o.a.i.cache.affinity.rendezvous.RendezvousAffinityFunction");
        }
        else
            fail("Unknown output format: " + outputFormat);
    }

    /**
     *
     */
    @Test
    public void testCacheDistribution() throws Exception {
        Ignite ignite = startGrids(2);

        ignite.cluster().active(true);

        createCacheAndPreload(ignite, 100);

        injectTestSystemOut();

        // Run distribution for all node and all cache
        assertEquals(EXIT_CODE_OK, execute("--cache", "distribution", "null"));

        String out = testOut.toString();

        // Result include info by cache "default"
        assertContains(log ,out, "[next group: id=1544803905, name=default]");

        // Result include info by cache "ignite-sys-cache"
        assertContains(log ,out, "[next group: id=-2100569601, name=ignite-sys-cache]");

        // Run distribution for all node and all cache and include additional user attribute
        assertEquals(EXIT_CODE_OK, execute("--cache", "distribution", "null", "--user-attributes", "ZONE,CELL,DC"));

        out = testOut.toString();

        List<String> outLines = Arrays.stream(out.split("\n"))
                                .map(String::trim)
                                .collect(Collectors.toList());

        int firstIndex = outLines.indexOf("[next group: id=1544803905, name=default]");
        int lastIndex  = outLines.lastIndexOf("[next group: id=1544803905, name=default]");

        String dataLine = outLines.get(firstIndex + 1);
        String userArrtDataLine = outLines.get(lastIndex + 1);

        long commaNum = dataLine.chars().filter(i -> i == ',').count();
        long userArrtCommaNum = userArrtDataLine.chars().filter(i -> i == ',').count();

        // Check that number of columns increased by 3
        assertEquals(3, userArrtCommaNum - commaNum);
    }

    /**
     *
     */
    @Test
    public void testCacheResetLostPartitions() throws Exception {
        Ignite ignite = startGrids(2);

        ignite.cluster().active(true);

        createCacheAndPreload(ignite, 100);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "reset_lost_partitions", "ignite-sys-cache,default"));

        final String out = testOut.toString();

        assertContains(log, out, "Reset LOST-partitions performed successfully. Cache group (name = 'ignite-sys-cache'");

        assertContains(log, out, "Reset LOST-partitions performed successfully. Cache group (name = 'default'");
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
     * Test execution of --wal print command.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testUnusedWalPrint() throws Exception {
        Ignite ignite = startGrids(2);

        ignite.cluster().active(true);

        List<String> nodes = new ArrayList<>(2);

        for (ClusterNode node : ignite.cluster().forServers().nodes())
            nodes.add(node.consistentId().toString());

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--wal", "print"));

        String out = testOut.toString();

        for (String id : nodes)
            assertContains(log, out, id);

        assertNotContains(log, out, "error");

        testOut.reset();

        assertEquals(EXIT_CODE_OK, execute("--wal", "print", nodes.get(0)));

        out = testOut.toString();

        assertNotContains(log, out, nodes.get(1));

        assertNotContains(log, out, "error");
    }

    /**
     * Test execution of --wal delete command.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testUnusedWalDelete() throws Exception {
        Ignite ignite = startGrids(2);

        ignite.cluster().active(true);

        List<String> nodes = new ArrayList<>(2);

        for (ClusterNode node : ignite.cluster().forServers().nodes())
            nodes.add(node.consistentId().toString());

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--wal", "delete"));

        String out = testOut.toString();

        for (String id : nodes)
            assertContains(log, out, id);

        assertNotContains(log, out, "error");

        testOut.reset();

        assertEquals(EXIT_CODE_OK, execute("--wal", "delete", nodes.get(0)));

        out = testOut.toString();

        assertNotContains(log, out, nodes.get(1));

        assertNotContains(log, out, "error");
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
                U.invoke(GridCacheDatabaseSharedManager.class, db, "applyUpdate", ctx, dataEntry, false);
            }
            finally {
                db.checkpointReadUnlock();
            }
        }
        catch (IgniteCheckedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Don't show wal commands by --help in case
     * {@link org.apache.ignite.IgniteSystemProperties#IGNITE_ENABLE_EXPERIMENTAL_COMMAND} = false or empty.
     */
    @Test
    @WithSystemProperty(key = IGNITE_ENABLE_EXPERIMENTAL_COMMAND, value = "false")
    public void testHideWalInHelpWhenDisableExperimentalCommand() {
        injectTestSystemOut();

        execute("--help");

        assertNotContains(log, testOut.toString(), WAL.text());
    }

    /**
     * Wal commands should ignored and print warning in case
     * {@link org.apache.ignite.IgniteSystemProperties#IGNITE_ENABLE_EXPERIMENTAL_COMMAND} = false or empty.
     * */
    @Test
    @WithSystemProperty(key = IGNITE_ENABLE_EXPERIMENTAL_COMMAND, value = "false")
    public void testWalCommandsInCaseDisableExperimentalCommand() {
        injectTestSystemOut();

        String warning = String.format("For use experimental command add %s=true to JVM_OPTS in %s",
            IGNITE_ENABLE_EXPERIMENTAL_COMMAND, UTILITY_NAME);

        Stream.of("print", "delete")
            .peek(c -> testOut.reset())
            .peek(c -> assertEquals(EXIT_CODE_OK, execute(WAL.text(), c)))
            .forEach(c -> assertContains(log, testOut.toString(), warning));
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
}
