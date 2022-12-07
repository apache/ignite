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
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.ReadRepairStrategy;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.commandline.consistency.ConsistencyCommand;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicSingleUpdateRequest;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.transactions.TransactionHeuristicException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.ReadRepairStrategy.PRIMARY;
import static org.apache.ignite.cache.ReadRepairStrategy.RELATIVE_MAJORITY;
import static org.apache.ignite.cache.ReadRepairStrategy.REMOVE;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheOffheapManager.DFLT_WAL_MARGIN_FOR_ATOMIC_CACHE_HISTORICAL_REBALANCE;
import static org.apache.ignite.internal.visor.consistency.VisorConsistencyRepairTask.CONSISTENCY_VIOLATIONS_FOUND;
import static org.apache.ignite.internal.visor.consistency.VisorConsistencyRepairTask.NOTHING_FOUND;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertNotContains;
import static org.apache.ignite.testframework.LogListener.matches;

/**
 *
 */
@RunWith(Parameterized.class)
public class GridCommandHandlerConsistencyCountersTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** */
    @Parameterized.Parameters(name = "strategy={0}, reuse={1}, historical={2}, atomicity={3}, walRestore={4}")
    public static Iterable<Object[]> data() {
        List<Object[]> res = new ArrayList<>();

        for (ReadRepairStrategy strategy : ReadRepairStrategy.values()) {
            for (boolean reuse : new boolean[] {false, true}) {
                for (boolean historical : new boolean[] {false, true}) {
                    for (CacheAtomicityMode atomicityMode : new CacheAtomicityMode[] {ATOMIC, TRANSACTIONAL}) {
                        for (boolean walRestore: new boolean[] {false, true})
                            res.add(new Object[] {strategy, reuse, historical, atomicityMode, walRestore});
                    }
                }
            }
        }

        return res;
    }

    /**
     * ReadRepair strategy
     */
    @Parameterized.Parameter
    public ReadRepairStrategy strategy;

    /**
     * When true, updates will reuse already existing keys.
     */
    @Parameterized.Parameter(1)
    public boolean reuseKeys;

    /**
     * When true, historical rebalance will be used instead of full.
     */
    @Parameterized.Parameter(2)
    public boolean historical;

    /**
     * Cache atomicity mode
     */
    @Parameterized.Parameter(3)
    public CacheAtomicityMode atomicityMode;

    /**
     * Ignite nodes use WAL for restoring logical updates at restart after the crash.
     */
    @Parameterized.Parameter(4)
    public boolean walRestore;

    /** Listening logger. */
    protected final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /** File IO blocked flag. */
    private static volatile boolean ioBlocked;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setGridLogger(listeningLog);

        cfg.getDataStorageConfiguration().setFileIOFactory(
            new BlockableFileIOFactory(cfg.getDataStorageConfiguration().getFileIOFactory()));

        cfg.getDataStorageConfiguration().setWalMode(WALMode.FSYNC); // Allows to use special IO at WAL as well.

        cfg.setFailureHandler(new StopNodeFailureHandler()); // Helps to kill nodes on stop with disabled IO.

        return cfg;
    }

    /**
     *
     */
    private static class BlockableFileIOFactory implements FileIOFactory {
        /** IO Factory. */
        private final FileIOFactory factory;

        /**
         * @param factory Factory.
         */
        public BlockableFileIOFactory(FileIOFactory factory) {
            this.factory = factory;
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            return new FileIODecorator(factory.create(file, modes)) {
                @Override public int write(ByteBuffer srcBuf) throws IOException {
                    if (ioBlocked)
                        throw new IOException();

                    return super.write(srcBuf);
                }

                @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
                    if (ioBlocked)
                        throw new IOException();

                    return super.write(srcBuf, position);
                }

                @Override public int write(byte[] buf, int off, int len) throws IOException {
                    if (ioBlocked)
                        throw new IOException();

                    return super.write(buf, off, len);
                }
            };
        }
    }

    /**
     * This tests checks crash recovery procedure (idle_verify -> consistency repair -> consistency finalize) for
     * the cluster with persistence enabled but failed for some reason under the load.
     * Precondition: A broken cluster with missed update counters and inconsistent data.
     * Step 0 (precondition):
     * Generating broken data and missed updates.
     * Detecting data is broken using 'idle_verify'
     *  - counters are different on nodes and has missed updates,
     *  - partition hashes are also differ
     * Killing the cluster.
     * Starting the cluster from persistence.
     * Step 1: Checking cluster state via 'idle_verify'.
     * It may detect counters and/or hash inconsistency (depends on test params).
     * Step 2: Repairing the data using '--consistency repair'.
     * Step 3: Fixing counters using '--consistency finalize'.
     * Profit :)
     */
    @Test
    public void testCountersOnCrachRecovery() throws Exception {
        int nodes = 3;
        int backupNodes = nodes - 1;

        IgniteEx ignite = startGrids(nodes);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, 1))
            .setBackups(backupNodes)
            .setName(DEFAULT_CACHE_NAME)
            .setAtomicityMode(atomicityMode)
            .setWriteSynchronizationMode(FULL_SYNC) // Allows to be sure that all messages are sent when put succeed.
            .setReadFromBackup(true)); // Allows to check values on backups.

        int updateCnt = 0;

        // Initial preloading.
        for (int i = 0; i < 2_000; i++) { // Enough to have historical rebalance when needed.
            cache.put(i, i);

            updateCnt++;
        }

        // Trick to have historical rebalance on cluster recovery (decreases percent of updates in comparison to cache size).
        if (historical) {
            stopAllGrids();
            startGrids(nodes);
        }

        int preloadCnt = updateCnt;

        Ignite prim = primaryNode(0L, DEFAULT_CACHE_NAME);
        List<Ignite> backups = backupNodes(0L, DEFAULT_CACHE_NAME);

        AtomicBoolean prepareBlock = new AtomicBoolean();
        AtomicBoolean finishBlock = new AtomicBoolean();

        AtomicReference<CountDownLatch> blockLatch = new AtomicReference<>();

        TestRecordingCommunicationSpi.spi(prim).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                if ((msg instanceof GridDhtTxPrepareRequest && prepareBlock.get()) ||
                    ((msg instanceof GridDhtTxFinishRequest || msg instanceof GridDhtAtomicSingleUpdateRequest) && finishBlock.get())) {
                    CountDownLatch latch = blockLatch.get();

                    assertTrue(latch.getCount() > 0);

                    latch.countDown();

                    return true; // Generating counter misses.
                }
                else
                    return false;
            }
        });

        int blockedFrom = reuseKeys ? 0 : preloadCnt;
        int committedFrom = blockedFrom + 1_000;

        int blockedKey = blockedFrom - 1; // None
        int committedKey = committedFrom - 1; // None

        List<String> backupMissed = new ArrayList<>();
        List<String> primaryMissed = new ArrayList<>();

        IgniteCache<Integer, Integer> primCache = prim.cache(DEFAULT_CACHE_NAME);

        Consumer<Integer> cachePut = (key) -> primCache.put(key, -key);

        GridCompoundFuture<?, ?> asyncPutFuts = new GridCompoundFuture<>();

        Consumer<Integer> cachePutAsync = (key) -> asyncPutFuts.add(GridTestUtils.runAsync(() -> cachePut.accept(key)));

        int primaryLwm = -1; // Primary LWM after data prepatation (at tx caches).
        int backupHwm = -1; // Backups HWM after data preparation (at tx caches).

        String backupMissedTail = null; // Misses after backupHwm, which backups are not aware of before the recovery.

        int primaryKeysCnt = preloadCnt; // Keys present on primary.
        int backupsKeysCnt = preloadCnt; // Keys present on backups.

        int iters = 11;

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        // The main idea of this section is to generate missed counters updates, some of them should be on backups only,
        // some at primary too.
        // This emulates the situation of reordering possible on real highloaded clusters.
        for (int it = 0; it < iters; it++) {
            boolean first = it == 0;
            boolean last = it == iters - 1;

            boolean globalBlock = it % 2 != 0 && // Global means all nodes (including primary) will miss the counters updates.
                // Since atomic cache commits entry on primary before sending the request to backups, so all misses are only on backups.
                atomicityMode != ATOMIC;

            if (first || last) // Odd number to gain misses on backups only at first and last iteration.
                // First iteration will guarantee rebalance for tx caches: Primary LWM > backup LWM.
                // Last iteration will guarantee rebalance for atomic caches: Primary counter > backup counter.
                assertTrue(!globalBlock);

            int range = rnd.nextInt(1, 5);

            blockLatch.set(new CountDownLatch(backupNodes * range));

            if (globalBlock)
                prepareBlock.set(true);
            else
                finishBlock.set(true);

            for (int i = 0; i < range; i++) {
                blockedKey++;
                updateCnt++;

                if (!globalBlock)
                    primaryKeysCnt++;

                for (Ignite backup : backups) // Check before put.
                    assertEquals(reuseKeys, backup.cache(DEFAULT_CACHE_NAME).get(blockedKey) != null);

                cachePutAsync.accept(blockedKey);
            }

            blockLatch.get().await();

            prepareBlock.set(false);
            finishBlock.set(false);

            String missed = range == 1 ? String.valueOf(updateCnt) : (updateCnt - range + 1) + " - " + updateCnt;

            if (last)
                backupMissedTail = missed;
            else
                backupMissed.add(missed);

            if (globalBlock)
                primaryMissed.add(missed);

            if (!last)
                for (int i = 0; i < range; i++) {
                    committedKey++;
                    updateCnt++;
                    primaryKeysCnt++;
                    backupsKeysCnt++;

                    cachePut.accept(committedKey);
                }

            if (first)
                primaryLwm = updateCnt;

            if (!last)
                backupHwm = updateCnt;
        }

        assertNotNull(backupMissedTail);
        assertTrue(primaryLwm != -1);
        assertTrue(backupHwm != -1);
        assertTrue(blockedKey < committedFrom); // Intersection check.

        if (reuseKeys)
            assertTrue(blockedKey < preloadCnt); // Owerflow check.

        for (int key = blockedFrom; key <= blockedKey; key++) {
            for (Ignite backup : backups) // Check after put.
                assertEquals(reuseKeys, backup.cache(DEFAULT_CACHE_NAME).get(key) != null);
        }

        for (int key = committedFrom; key <= committedKey; key++) {
            assertNotNull(primCache.get(key));

            for (Ignite backup : backups)
                assertNotNull(backup.cache(DEFAULT_CACHE_NAME).get(key));
        }

        injectTestSystemOut();

        if (!walRestore) {
            // Idle verify triggers checkpoint and then no WAL restore is performed after cluster restart.
            assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));

            assertConflicts(true, true);

            if (atomicityMode == TRANSACTIONAL) {
                assertTxCounters(primaryLwm, primaryMissed, updateCnt); // Primary
                assertTxCounters(preloadCnt, backupMissed, backupHwm); // Backups
            }
            else {
                assertAtomicCounters(updateCnt); // Primary
                assertAtomicCounters(backupHwm); // Backups
            }
        }

        // On node start up it applies WAL changes twice: one for metastore updates, second for logical updates.
        // Then this record is written to log (2 * nodes) times. This test doesn't perform metastore updates.
        LogListener lsnrWalRestoreNoUpdates = LogListener
            .matches("Finished applying WAL changes [updatesApplied=0,")
            .times(walRestore ? nodes : 2 * nodes) // For walRestore=false nodes have neither metastore nor logical updates.
                                                   // For walRestore=true nodes don't have metastore updates.
            .build();

        LogListener lsnrPrimaryWalRestoreUpdates = LogListener
            .matches("Finished applying WAL changes [updatesApplied=" + (historical ? primaryKeysCnt - preloadCnt : primaryKeysCnt) + ',')
            .times(walRestore ? 1 : 0)  // Only for walRestore=true nodes have logical updates.
            .build();

        LogListener lsnrBackupsWalRestoreUpdates = LogListener
            .matches("Finished applying WAL changes [updatesApplied=" + (historical ? backupsKeysCnt - preloadCnt : backupsKeysCnt) + ',')
            .times(walRestore ? nodes - 1 : 0) // Only for walRestore=true nodes have logical updates.
            .build();

        LogListener lsnrRebalanceType = matches("fullPartitions=[" + (historical ? "" : 0) + "], " +
            "histPartitions=[" + (historical ? 0 : "") + "]").times(backupNodes).build();

        LogListener lsnrRebalanceAmount = matches("receivedKeys=" +
            (historical ?
                atomicityMode == TRANSACTIONAL ?
                    primaryLwm - preloadCnt : // Diff between primary LWM and backup LWM.
                    updateCnt - backupHwm + // Diff between primary and backup counters
                        DFLT_WAL_MARGIN_FOR_ATOMIC_CACHE_HISTORICAL_REBALANCE : // + magic number.
                reuseKeys ?
                    preloadCnt : // Since keys are reused, amount is the same as at initial preloading.
                    atomicityMode == TRANSACTIONAL ?
                        primaryKeysCnt : // Calculated amount of entries (initial preload amount + updates not missed at primary)
                        updateCnt)) // All updates since only misses on backups generated for atomic caches at this test.
            .times(backupNodes).build();

        listeningLog.registerListener(lsnrRebalanceType);
        listeningLog.registerListener(lsnrRebalanceAmount);
        listeningLog.registerListener(lsnrWalRestoreNoUpdates);
        listeningLog.registerListener(lsnrBackupsWalRestoreUpdates);
        listeningLog.registerListener(lsnrPrimaryWalRestoreUpdates);

        ioBlocked = true; // Emulating power off, OOM or disk overflow. Keeping data as is, with missed counters updates.

        stopAllGrids();

        checkAsyncPutOperationsFinished(asyncPutFuts);

        ioBlocked = false;

        ignite = startGrids(nodes);

        awaitPartitionMapExchange();

        assertTrue(lsnrRebalanceType.check());
        assertTrue(lsnrWalRestoreNoUpdates.check());
        assertTrue(lsnrBackupsWalRestoreUpdates.check());
        assertTrue(lsnrPrimaryWalRestoreUpdates.check());

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));

        assertConflicts(
            atomicityMode == TRANSACTIONAL, // Atomic cache backup counters are automatically set as primary on crash recovery.
            historical); // Full rebalance fixes the consistency.

        assertTrue(lsnrRebalanceAmount.check());

        if (atomicityMode == TRANSACTIONAL) { // Same as before the crash.
            assertTxCounters(primaryLwm, primaryMissed, updateCnt); // Primary
            assertTxCounters(preloadCnt, backupMissed, backupHwm); // Backups
        }
        else if (historical) {
            assertAtomicCounters(updateCnt); // Primary

            // Backups updated with primary counter.
            // Entries between primary and backup counters are rebalanced.
            assertNoneAtomicCounters(backupHwm); // Automaticaly "repaired".
        }
        else
            assertNoneAtomicCounters();

        assertEquals(EXIT_CODE_OK, execute("--consistency", "repair",
            ConsistencyCommand.CACHE, DEFAULT_CACHE_NAME,
            ConsistencyCommand.PARTITIONS, "0",
            ConsistencyCommand.STRATEGY, strategy.toString()));

        int repairedCnt = repairedEntriesCount();

        assertContains(log, testOut.toString(), historical ? CONSISTENCY_VIOLATIONS_FOUND : NOTHING_FOUND);

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));

        switch (strategy) {
            case PRIMARY:
            case REMOVE:
            case RELATIVE_MAJORITY:
                assertConflicts(atomicityMode == TRANSACTIONAL, false); // Inconsistency fixed.

                break;

            case LWW:
                // LWM can't fix the data when some nodes contain the key, but some not,
                // because missed entry has no version and can not be compared.
                assertConflicts(atomicityMode == TRANSACTIONAL, historical && !reuseKeys);

                break;

            case CHECK_ONLY: // Keeps as is.
                assertConflicts(atomicityMode == TRANSACTIONAL, historical);

                break;

            default:
                throw new UnsupportedOperationException("Unsupported strategy");
        }

        int postloadFrom = reuseKeys ? 0 : (committedKey + 1 /*greater than max key used*/);

        for (int i = postloadFrom; i < postloadFrom + preloadCnt; i++) {
            updateCnt++;

            ignite.cache(DEFAULT_CACHE_NAME).put(i, i);
        }

        // Repairing one more time, but with guarantee to fix (primary strategy);
        assertEquals(EXIT_CODE_OK, execute("--consistency", "repair",
            ConsistencyCommand.CACHE, DEFAULT_CACHE_NAME,
            ConsistencyCommand.PARTITIONS, "0",
            ConsistencyCommand.STRATEGY, PRIMARY.toString()));

        repairedCnt += repairedEntriesCount();

        updateCnt += repairedCnt;

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));

        backupMissed.add(backupMissedTail); // Now backups got updates after the generated data and aware of all missed updates.

        if (atomicityMode == TRANSACTIONAL) {
            assertConflicts(true, false); // Still has counters conflicts, while inconsistency already fixed.

            assertTxCounters(primaryLwm, primaryMissed, updateCnt); // Primary (same as before crash).
            assertTxCounters(preloadCnt, backupMissed, updateCnt); // Backups (all missed updates, increased hwm).
        }
        else
            assertConflicts(false, false); // Fixed both counters and inconsistency.

        int rmvd = strategy == RELATIVE_MAJORITY || strategy == REMOVE ? repairedCnt /*entries removed during the repair*/ : 0;

        for (Ignite node : G.allGrids()) // Checking the cache size after the fixes.
            assertEquals((reuseKeys ? preloadCnt : primaryKeysCnt + preloadCnt /*postload*/ - rmvd),
                node.cache(DEFAULT_CACHE_NAME).localSize(CachePeekMode.ALL));

        assertEquals(EXIT_CODE_OK, execute("--consistency", "finalize")); // Fixing partitions update counters.

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));

        assertConflicts(false, false); // Counters are fixed.

        // Checking finalization did not break something.
        for (int i = 0; i < updateCnt * 2; i += 7) { // From 0 to maximum and even more.
            ignite.cache(DEFAULT_CACHE_NAME).put(i, i + 42);
        }

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));

        assertConflicts(false, false); // Counters still fixed.

        for (int i = 0; i < updateCnt * 2; i += 7) { // Checking data updated after the counters fix.
            for (Ignite node : G.allGrids())
                assertEquals(i + 42, node.cache(DEFAULT_CACHE_NAME).get(i));
        }
    }

    /**
     * Checks idle_vefify result.
     *
     * @param counter Counter conflicts.
     * @param hash    Hash conflicts.
     */
    private void assertConflicts(boolean counter, boolean hash) {
        if (counter || hash)
            assertContains(log, testOut.toString(), "conflict partitions has been found: " +
                "[counterConflicts=" + (counter ? 1 : 0) + ", hashConflicts=" + (hash ? 1 : 0) + "]");
        else
            assertContains(log, testOut.toString(), "no conflicts have been found");
    }

    /**
     * @param lwm Lwm.
     * @param missed Missed.
     * @param hwm Hwm.
     */
    private void assertTxCounters(int lwm, List<String> missed, int hwm) {
        assertContains(log, testOut.toString(),
            "updateCntr=[lwm=" + lwm + ", missed=" + missed + ", hwm=" + hwm + "]");
    }

    /**
     * @param cnt Counter.
     */
    private void assertAtomicCounters(int cnt) {
        assertContains(log, testOut.toString(), "updateCntr=" + cnt);
    }

    /**
     * @param cnt Counter.
     */
    private void assertNoneAtomicCounters(int... cnt) {
        assertNotContains(log, testOut.toString(), "updateCntr=" + (F.isEmpty(cnt) ? "" : cnt[0]));
    }

    /**
     * Returns amount of entries repaired by Consistency Repair operation.
     */
    private int repairedEntriesCount() {
        Pattern pattern = Pattern.compile("repaired=(\\d*),");
        Matcher matcher = pattern.matcher(testOut.toString());

        return matcher.find() ?
            Integer.parseInt(testOut.toString().substring(matcher.start(1), matcher.end(1))) :
            0;
    }

    /**
     * Checks that all async put operations are finished.
     */
    private void checkAsyncPutOperationsFinished(GridCompoundFuture<?, ?> asyncPutFuts) {
        asyncPutFuts.markInitialized();

        try {
            asyncPutFuts.get();

            if (atomicityMode != ATOMIC) // Atomics already committed at primary before the fail, so no falure on get() is expected.
                fail(); // But tx cache is still committing, so get() must throw an exception.
        }
        catch (IgniteCheckedException | TransactionHeuristicException ex) {
            assertTrue(atomicityMode != ATOMIC);
        }
    }
}
