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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for checking rebalance log messages.
 */
public class IgniteWalRebalanceLoggingTest extends GridCommonAbstractTest {
    /** This timeout should be big enough in order to prohibit checkpoint triggered by timeout. */
    private static final int CHECKPOINT_FREQUENCY = 600_000;

    /** Test logger. */
    private final ListeningTestLogger srvLog = new ListeningTestLogger(false, log);

    /** */
    private static final int KEYS_LOW_BORDER = 100;

    /** */
    private static final int KEYS_UPPER_BORDER = 200;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setGridLogger(srvLog);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        storageCfg.setWalMode(WALMode.LOG_ONLY).setWalCompactionEnabled(true).setWalCompactionLevel(1);

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
     *  <p>
     *      <b>Expected result:</b>
     *      Assert that (after restarting second node) log would contain following messages expected number of times:
     *      <ul>
     *          <li>'Following partitions were reserved for potential history rebalance [groupId=1813188848
     *          parts=[0-7], groupId=1813188847 parts=[0-7]]'
     *          Meaning that partitions were reserved for history rebalance.</li>
     *          <li>'Starting rebalance routine [cache_group1, topVer=AffinityTopologyVersion [topVer=4, minorTopVer=0],
     *          supplier=*, fullPartitions=[0], histPartitions=[0-7]]' Meaning that history rebalance started.</li>
     *      </ul>
     *      And assert that (after restarting second node) log would <b>not</b> contain following messages:
     *      <ul>
     *          <li>Unable to perform historical rebalance...</li>
     *      </ul>
     * @throws Exception If failed.
     */
    public void testHistoricalRebalanceLogMsg() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD, "1");

        LogListener expMsgsLsnr = LogListener.
            matches("Following partitions were reserved for potential history rebalance [grpId=1813188848," +
                " grpName=cache_group2, parts=[0-7], grpId=1813188847, grpName=cache_group1, parts=[0-7]]").times(3).
            andMatches("fullPartitions=[], histPartitions=[0-7]").times(2).build();

        LogListener unexpectedMessagesLsnr =
            LogListener.matches("Unable to perform historical rebalance").build();

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
     *  <p>
     *      <b>Expected result:</b>
     *      Assert that (after restarting second node) log would contain following messages expected number of times:
     *      <ul>
     *          <li>'Following partitions were reserved for potential history rebalance []'
     *          Meaning that no partitions were reserved for history rebalance.</li>
     *          <li>'Unable to perform historical rebalance cause history supplier is not available'</li>
     *          <li>'Unable to perform historical rebalance cause partition is supposed to be cleared'</li>
     *          <li>'Starting rebalance routine [cache_group1, topVer=AffinityTopologyVersion [topVer=4, minorTopVer=0],
     *          supplier=* fullPartitions=[0-7], histPartitions=[]]'</li>
     *      </ul>
     * @throws Exception If failed.
     */
    public void testFullRebalanceLogMsgs() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD, "500000");
        LogListener expMsgsLsnr = LogListener.
            matches("Following partitions were reserved for potential history rebalance []").times(4).
            andMatches("Unable to perform historical rebalance cause history supplier is not available [" +
                "grpId=1813188848, grpName=cache_group2, parts=[0-7]," +
                " topVer=AffinityTopologyVersion [topVer=4, minorTopVer=0]]").
            andMatches("Unable to perform historical rebalance cause history supplier is not available [" +
                "grpId=1813188847, grpName=cache_group1, parts=[0-7]," +
                " topVer=AffinityTopologyVersion [topVer=4, minorTopVer=0]]").
            andMatches("fullPartitions=[0-7], histPartitions=[]").times(2).build();

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
        }

        forceCheckpoint();

        stopGrid(1);

        for (int i = KEYS_LOW_BORDER; i < KEYS_UPPER_BORDER; i++) {
            cache1.put(i, "abc" + i);
            cache2.put(i, "abc" + i);
        }

        forceCheckpoint();

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
}
