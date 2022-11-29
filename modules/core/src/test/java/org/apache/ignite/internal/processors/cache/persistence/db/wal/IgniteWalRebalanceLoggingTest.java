/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;
import static org.apache.ignite.internal.pagemem.wal.record.RolloverType.CURRENT_SEGMENT;

/**
 * Tests for checking historical rebalance log messages.
 */
public class IgniteWalRebalanceLoggingTest extends GridCommonAbstractTest {
    /** This timeout should be big enough in order to prohibit checkpoint triggered by timeout. */
    private static final int CHECKPOINT_FREQUENCY = 600_000;

    /** Test logger. */
    private final ListeningTestLogger srvLog = new ListeningTestLogger(log);

    /** */
    private static final int KEYS_LOW_BORDER = 100;

    /** */
    private static final int KEYS_UPPER_BORDER = 200;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setGridLogger(srvLog);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        storageCfg.getDefaultDataRegionConfiguration()
            .setMaxSize(200L * 1024 * 1024)
            .setPersistenceEnabled(true);

        storageCfg.setWalMode(WALMode.LOG_ONLY)
            .setMaxWalArchiveSize(-1)
            .setWalCompactionEnabled(true)
            .setWalCompactionLevel(1);

        storageCfg.setCheckpointFrequency(CHECKPOINT_FREQUENCY);

        cfg.setDataStorageConfiguration(storageCfg);

        return cfg;
    }

    /** {@inheritDoc}*/
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc}*/
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Check that in case of Historical rebalance we log appropriate messages.
     * <p>
     *     <b>Steps:</b>
     *     <ol>
     *         <li>set IGNITE_PDS_WAL_REBALANCE_THRESHOLD to 1</li>
     *         <li>Start two nodes.</li>
     *         <li>Create two caches each in it's own cache group and populate them with some data.</li>
     *         <li>Stop second node and add more data to both caches.</li>
     *         <li>Wait checkpoint frequency * 2. This is required to guarantee that at least one checkpoint would be
     *         created.</li>
     *         <li>Start, previously stopped node and await for PME.</li>
     *     </ol>
     * <p>
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "1")
    public void testHistoricalRebalanceLogMsg() throws Exception {
        LogListener expMsgsLsnr = LogListener.matches(str ->
            str.startsWith("Cache groups with earliest reserved checkpoint and a reason why a previous checkpoint was inapplicable:") &&
                str.contains("cache_group1") && str.contains("cache_group2")).times(3).
            andMatches(str -> str.startsWith("Starting rebalance routine") &&
                (str.contains("cache_group1") || str.contains("cache_group2")) &&
                str.contains("fullPartitions=[], histPartitions=[0,1,2,3,4,5,6,7]")).times(2).build();

        LogListener unexpectedMessagesLsnr = LogListener.matches((str) ->
            str.startsWith("Partitions weren't present in any history reservation:") ||
                str.startsWith("Partitions were reserved, but maximum available counter is greater than demanded:")
        ).build();

        checkFollowingPartitionsWereReservedForPotentialHistoryRebalanceMsg(expMsgsLsnr, unexpectedMessagesLsnr);

        assertTrue(expMsgsLsnr.check());
        assertFalse(unexpectedMessagesLsnr.check());
    }

    /**
     * Check that in case of Full rebalance we log appropriate messages.
     * <p>
     *     <b>Steps:</b>
     *     <ol>
     *         <li>restore IGNITE_PDS_WAL_REBALANCE_THRESHOLD to default 500000</li>
     *         <li>Start two nodes.</li>
     *         <li>Create two caches each in it's own cache group and populate them with some data.</li>
     *         <li>Stop second node and add more data to both caches.</li>
     *         <li>Wait checkpoint frequency * 2. This is required to guarantee that at least one checkpoint would be
     *         created.</li>
     *         <li>Start, previously stopped node and await for PME.</li>
     *     </ol>
     * <p>
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "500000")
    public void testFullRebalanceLogMsgs() throws Exception {
        LogListener expMsgsLsnr = LogListener.
            matches("Partitions weren't present in any history reservation: " +
                "[[grp=cache_group2 part=[[0,1,2,3,4,5,6,7]]], [grp=cache_group1 part=[[0,1,2,3,4,5,6,7]]]]").
            andMatches(str -> str.startsWith("Starting rebalance routine") &&
                (str.contains("cache_group1") || str.contains("cache_group2")) &&
                str.contains("fullPartitions=[0,1,2,3,4,5,6,7], histPartitions=[]")).times(2).build();

        checkFollowingPartitionsWereReservedForPotentialHistoryRebalanceMsg(expMsgsLsnr);

        assertTrue(expMsgsLsnr.check());
    }

    /**
     * Test checks log messages in case of short history of checkpoint.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE, value = "2")
    @WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "1")
    public void testFullRebalanceWithShortCpHistoryLogMsgs() throws Exception {
        LogListener expMsgsLsnr = LogListener.
            matches(str -> str.startsWith("Partitions were reserved, but maximum available counter is greater than " +
                "demanded or WAL contains too many updates: ") &&
                str.contains("grp=cache_group1") && str.contains("grp=cache_group2")).
            andMatches(str -> str.startsWith("Starting rebalance routine") &&
                (str.contains("cache_group1") || str.contains("cache_group2")) &&
                str.contains("fullPartitions=[0,1,2,3,4,5,6,7], histPartitions=[]")).times(2).build();

        checkFollowingPartitionsWereReservedForPotentialHistoryRebalanceMsg(expMsgsLsnr);

        assertTrue(expMsgsLsnr.check());
    }

    /**
     * Test utility method.
     *
     * @param lsnrs Listeners to register with server logger.
     * @throws Exception If failed.
     */
    private void checkFollowingPartitionsWereReservedForPotentialHistoryRebalanceMsg(LogListener... lsnrs)
        throws Exception {
        startGridsMultiThreaded(2).cluster().active(true);

        IgniteCache<Integer, String> cache1 = createCache("cache1", "cache_group1");
        IgniteCache<Integer, String> cache2 = createCache("cache2", "cache_group2");

        for (int i = 0; i < KEYS_LOW_BORDER; i++) {
            cache1.put(i, "abc" + i);
            cache2.put(i, "abc" + i);

            if (i % 20 == 0)
                forceCheckpointAndRollOwerWal();
        }

        stopGrid(1);

        for (int i = KEYS_LOW_BORDER; i < KEYS_UPPER_BORDER; i++) {
            cache1.put(i, "abc" + i);
            cache2.put(i, "abc" + i);

            if (i % 20 == 0)
                forceCheckpointAndRollOwerWal();
        }

        srvLog.clearListeners();

        for (LogListener lsnr: lsnrs)
            srvLog.registerListener(lsnr);

        startGrid(1);

        awaitPartitionMapExchange(false, true, null);
    }

    /**
     * Create cache with specific name and group name.
     * @param cacheName Cache name.
     * @param cacheGrpName Cache group name.
     * @return Created cache.
     */
    private IgniteCache<Integer, String> createCache(String cacheName, String cacheGrpName) {
        return ignite(0).createCache(
            new CacheConfiguration<Integer, String>(cacheName).
                setAffinity(new RendezvousAffinityFunction().setPartitions(8))
                .setGroupName(cacheGrpName).
                setBackups(1));
    }

    /**
     * Invokes checkpoint forcibly and rollovers WAL segment.
     * It might be need for simulate long checkpoint history in test.
     *
     * @throws Exception If failed.
     */
    private void forceCheckpointAndRollOwerWal() throws Exception {
        forceCheckpoint();

        for (Ignite ignite : G.allGrids()) {
            if (ignite.cluster().localNode().isClient())
                continue;

            IgniteEx ig = (IgniteEx)ignite;

            IgniteWriteAheadLogManager walMgr = ig.context().cache().context().wal();

            ig.context().cache().context().database().checkpointReadLock();

            try {
                WALPointer ptr = walMgr.log(new AdHocWALRecord(), CURRENT_SEGMENT);
            }
            finally {
                ig.context().cache().context().database().checkpointReadUnlock();
            }
        }
    }

    /** Tets WAL record. */
    private static class AdHocWALRecord extends CheckpointRecord {
        /** Default constructor. */
        private AdHocWALRecord() {
            super(null);
        }
    }
}
