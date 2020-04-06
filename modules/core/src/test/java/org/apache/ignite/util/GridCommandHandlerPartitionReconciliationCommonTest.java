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

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.ComputeTaskInternalFuture;
import org.apache.ignite.internal.GridJobExecuteResponse;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationAffectedEntries;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.processors.cache.verify.checker.tasks.PartitionReconciliationProcessorTask;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.TestStorageUtils.corruptDataEntry;
import static org.apache.ignite.internal.GridTopic.TOPIC_TASK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertNotContains;

/**
 * Common partition reconciliation tests.
 */
public class GridCommandHandlerPartitionReconciliationCommonTest
    extends GridCommandHandlerPartitionReconciliationAbstractTest {
    /** */
    public static final int INVALID_KEY = 100;

    /** */
    public static final String VALUE_PREFIX = "abc_";

    /** @inheritDoc */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        prepareCache();
    }

    /**
     * Check the simple case of inconsistency
     * <b>Preconditions:</b>
     * <ul>
     *     <li>Grid with 4 nodes is started, cache with 3 backups with some data is up and running</li>
     * </ul>
     * <b>Test steps:</b>
     * <ol>
     *     <li>Emulate non-consistency when keys were updated on primary and 2 backups</li>
     *     <li>Start bin/control.sh --cache partition_reconciliation</li>
     * </ol>
     * <b>Expected result:</b>
     * <ul>
     *     <li>Check that report contains info about one and only one inconsistent key</li>
     * </ul>
     */
    @Test
    public void testSimpleCaseOfInconsistencyDetection() {
        ignite(0).cache(DEFAULT_CACHE_NAME).put(INVALID_KEY, VALUE_PREFIX + INVALID_KEY);

        List<ClusterNode> nodes = ignite(0).cachex(DEFAULT_CACHE_NAME).cache().context().affinity().
            nodesByKey(
                INVALID_KEY,
                ignite(0).cachex(DEFAULT_CACHE_NAME).context().topology().readyTopologyVersion());

        corruptDataEntry(((IgniteEx)grid(nodes.get(2))).cachex(
            DEFAULT_CACHE_NAME).context(),
            INVALID_KEY,
            false,
            false,
            new GridCacheVersion(0, 0, 2),
            null);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "partition_reconciliation", "--local-output"));

        assertContains(log, testOut.toString(), "INCONSISTENT KEYS: 1");
    }

    /**
     * Check that console scoped report contains aggregated info about amount of inconsistent keys and
     * doesn't contain detailed info in case not using --local-output argument.
     * <b>Preconditions:</b>
     * <ul>
     *     <li>Grid with 4 nodes is started, cache with 3 backups with some data is up and running</li>
     * </ul>
     * <b>Test steps:</b>
     * <ol>
     *     <li>Emulate non-consistency when keys were updated on primary and 2 backups</li>
     *     <li>Start bin/control.sh --cache partition_reconciliation</li>
     * </ol>
     * <b>Expected result:</b>
     * <ul>
     *     <li>Check that console report contains aggregated info about amount of inconsistent keys and
     *     doesn't contain detailed info.</li>
     * </ul>
     */
    @Test
    public void testConsoleScopedReportContainsAggregatedInfoAboutAmountAndNotDetailedInfoInCaseOfNonConsoleMode() {
        ignite(0).cache(DEFAULT_CACHE_NAME).put(INVALID_KEY, VALUE_PREFIX + INVALID_KEY);

        List<ClusterNode> nodes = ignite(0).cachex(DEFAULT_CACHE_NAME).cache().context().affinity().
            nodesByKey(
                INVALID_KEY,
                ignite(0).cachex(DEFAULT_CACHE_NAME).context().topology().readyTopologyVersion());

        corruptDataEntry(((IgniteEx)grid(nodes.get(1))).cachex(
            DEFAULT_CACHE_NAME).context(),
            INVALID_KEY,
            false,
            false,
            new GridCacheVersion(0, 0, 2),
            null);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "partition_reconciliation"));

        assertContains(log, testOut.toString(), "INCONSISTENT KEYS: 1");

        assertNotContains(log, testOut.toString(), "<nodeConsistentId>, <nodeId>: <value> <version>");
    }

    /**
     * Check that console scoped report contains detailed info in case of using --local-output argument.
     * <b>Preconditions:</b>
     * <ul>
     *     <li>Grid with 4 nodes is started, cache with 3 backups with some data is up and running</li>
     * </ul>
     * <b>Test steps:</b>
     * <ol>
     *     <li>Emulate non-consistency when keys were updated on primary and 2 backups</li>
     *     <li>Start bin/control.sh --cache partition_reconciliation --local-output</li>
     * </ol>
     * <b>Expected result:</b>
     * <ul>
     *     <li>Check that console scoped report contains detailed info about inconsistent keys.</li>
     * </ul>
     */
    @Test
    public void testConsoleScopedReportContainsAggregatedInfoAboutAmountAndNotDetailedInfoInCaseOfConsoleMode() {
        ignite(0).cache(DEFAULT_CACHE_NAME).put(INVALID_KEY, VALUE_PREFIX + INVALID_KEY);

        List<ClusterNode> nodes = ignite(0).cachex(DEFAULT_CACHE_NAME).cache().context().affinity().
            nodesByKey(
                INVALID_KEY,
                ignite(0).cachex(DEFAULT_CACHE_NAME).context().topology().readyTopologyVersion());

        corruptDataEntry(((IgniteEx)grid(nodes.get(1))).cachex(
            DEFAULT_CACHE_NAME).context(),
            INVALID_KEY,
            false,
            false,
            new GridCacheVersion(0, 0, 2),
            null);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "partition_reconciliation", "--local-output"));

        assertContains(log, testOut.toString(), "INCONSISTENT KEYS: 1");

        assertContains(log, testOut.toString(), "<nodeConsistentId>, <nodeId>: <value> <version>");
    }

    /**
     * Checks that -includeSensitive parameter raises a warining requiring consent from user to print sensitive data to output.
     * <b>Preconditions:</b>
     * <ul>
     *     <li>Grid with 4 nodes is started, cache with 3 backups with some data is up and running</li>
     * </ul>
     * <b>Test steps:</b>
     * <ol>
     *     <li>Start bin/control.sh --cache partition_reconciliation --local-output --include-sensitive</li>
     * </ol>
     * <b>Expected result:</b>
     * <ul>
     *     <li>Check that console scoped report contains a warning that sensitive information will be printed.</li>
     * </ul>
     */
    @Test
    public void testConsoleOutputContainsWarningAboutSensitiveInformation() {
        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "partition_reconciliation", "--local-output", "--include-sensitive"));

        assertContains(
            log,
            testOut.toString(),
            "WARNING: Please be aware that sensitive data will be printed to the console and output file(s).");
    }

    /**
     * Checks that sensitive information is hidden when -includeSensitive parameter is not specified.
     * <b>Preconditions:</b>
     * <ul>
     *     <li>Grid with 4 nodes is started, cache with 3 backups with some data is up and running</li>
     * </ul>
     * <b>Test steps:</b>
     * <ol>
     *     <li>Start bin/control.sh --cache partition_reconciliation --local-output</li>
     * </ol>
     * <b>Expected result:</b>
     * <ul>
     *     <li>Check that console scoped report does not contain sensitive information.</li>
     * </ul>
     *
     * @throws Exception if failed.
     */
    @Test
    public void testConsoleOutputHidesSensetiveInformation() throws Exception {
        ignite(0).cache(DEFAULT_CACHE_NAME).put(INVALID_KEY, VALUE_PREFIX + INVALID_KEY);

        List<ClusterNode> nodes = ignite(0).cachex(DEFAULT_CACHE_NAME).cache().context().affinity().
            nodesByKey(
                INVALID_KEY,
                ignite(0).cachex(DEFAULT_CACHE_NAME).context().topology().readyTopologyVersion());

        corruptDataEntry(((IgniteEx)grid(nodes.get(1))).cachex(
            DEFAULT_CACHE_NAME).context(),
            INVALID_KEY,
            false,
            false,
            new GridCacheVersion(0, 0, 2),
            null);

        injectTestSystemOut();

        CommandHandler hnd = new CommandHandler();

        assertEquals(EXIT_CODE_OK, execute(hnd,"--cache", "partition_reconciliation", "--local-output"));

        assertContains(log, testOut.toString(), "INCONSISTENT KEYS: 1");

        // Check console output.
        assertContains(log, testOut.toString(), ReconciliationAffectedEntries.HIDDEN_DATA + " ver=[topVer=");

        ClusterNode primaryNode = ignite(0).affinity(DEFAULT_CACHE_NAME).mapKeyToNode(INVALID_KEY);

        assertNotNull("Cannot find primary node for the key [key=" + INVALID_KEY + ']', primaryNode);

        ReconciliationResult res = hnd.getLastOperationResult();

        String pathToReport = res.nodeIdToFolder().get(primaryNode.id());

        assertNotNull("Cannot find partition reconciliation report", pathToReport);

        String inconsistencyReport = new String(Files.readAllBytes(Paths.get(pathToReport)));

        // Check inconsistency report.
        assertContains(log, inconsistencyReport, ReconciliationAffectedEntries.HIDDEN_DATA + " ver=[topVer=");
    }

    /**
     * Checks that partition reconciliation task does not block graceful stop of a node for a long period of time.
     * <b>Preconditions:</b>
     * <ul>
     *     <li>Grid with 4 nodes is started, cache with 3 backups with some data is up and running</li>
     * </ul>
     * <b>Test steps:</b>
     * <ol>
     *     <li>Start bin/control.sh --cache partition_reconciliation --local-output --recheck-delay 10</li>
     *     <li>Try to gracefuly stop a node/cluster.</li>
     * </ol>
     * <b>Expected result:</b>
     * <ul>
     *     <li>Check that grid will be stopped in less than 20 sec.</li>
     * </ul>
     *
     * @throws Exception if failed.
     */
    @Test
    public void testPartitionReconciliationTaskDoesNotBlockGracefulStop() throws Exception {
        ignite(0).cache(DEFAULT_CACHE_NAME).put(INVALID_KEY, VALUE_PREFIX + INVALID_KEY);

        List<ClusterNode> nodes = ignite(0).cachex(DEFAULT_CACHE_NAME).cache().context().affinity().
            nodesByKey(
                INVALID_KEY,
                ignite(0).cachex(DEFAULT_CACHE_NAME).context().topology().readyTopologyVersion());

        corruptDataEntry(((IgniteEx)grid(nodes.get(1))).cachex(
            DEFAULT_CACHE_NAME).context(),
            INVALID_KEY,
            false,
            false,
            new GridCacheVersion(0, 0, 2),
            null);

        // Triggers graceful stop of the cluster when patrition reconciliation task is in progress.
        final CountDownLatch evtLatch = new CountDownLatch(1);

        G.allGrids().forEach(ig -> ((IgniteEx)ig).context().io().addMessageListener(
            TOPIC_TASK,
            new GridMessageListener() {
                /** {@inheritDoc} */
                @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                    if (msg instanceof GridJobExecuteResponse) {
                        GridJobExecuteResponse jobRes = (GridJobExecuteResponse)msg;

                        ComputeTaskInternalFuture<?> fut = ((IgniteEx)ig)
                            .context()
                            .task()
                            .taskFuture(jobRes.getSessionId());

                        boolean partReconciliationTask =
                            fut != null &&
                            fut.getTaskSession().getTaskName()
                                .contains(PartitionReconciliationProcessorTask.class.getSimpleName());

                        if (partReconciliationTask)
                            evtLatch.countDown();
                    }
                }
            }));

        long start = System.nanoTime();

        int batchSize = INVALID_KEY / 4;
        int recheckAttempts = 3;
        int recheckDelay = 10;

        CommandHandler hnd = new CommandHandler();

        GridTestUtils.runAsync(() -> {
            execute(
                hnd,
                "--cache",
                "partition_reconciliation",
                "--load-factor", "0.1",
                "--batch-size", Integer.toString(batchSize),
                "--recheck-attempts", Integer.toString(recheckAttempts),
                "--recheck-delay", Integer.toString(recheckDelay));
        });

        assertTrue(
            "The partition reconciliation task is not started in " + (recheckDelay * recheckAttempts) +" sec.",
            evtLatch.await(recheckDelay * recheckAttempts, TimeUnit.SECONDS));

        try {
            stopAllGrids();

            long end = System.nanoTime();

            assertTrue(
                "It seems that partition reconciliation task blocked garceful stop of the cluster.",
                TimeUnit.SECONDS.toNanos(recheckDelay * recheckAttempts) > (end - start));
        }
        finally {
            ignite = startGrids(4);
        }
    }

    /**
     * Create cache and populate it with some data.
     */
    @Override protected void prepareCache() {
        ignite(0).destroyCache(DEFAULT_CACHE_NAME);

        ignite(0).createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 16))
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(3));

        try (IgniteDataStreamer<Integer, String> streamer = ignite(0).dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < INVALID_KEY; i++)
                streamer.addData(i, VALUE_PREFIX + i);
        }
    }
}
