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

package org.apache.ignite.internal.processors.database;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.util.IgniteUtils.GB;

/**
 *
 */
public class GridCacheReadOnlyStoreSelfTest extends GridCommonAbstractTest {
    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setBackups(2);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 16));
        //ccfg.setBackups(1);
        // todo check different sync modes
//        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        DataStorageConfiguration dscfg = new DataStorageConfiguration();

        DataRegionConfiguration reg = new DataRegionConfiguration();

        reg.setMaxSize(2 * GB);
        reg.setPersistenceEnabled(true);

        dscfg.setDefaultDataRegionConfiguration(reg);
        dscfg.setCheckpointFrequency(3_000);

        cfg.setDataStorageConfiguration(dscfg);

        return cfg;
    }

    /** */
    @Before
    public void setup() throws Exception {
        cleanPersistenceDir();
    }

    /** */
    @After
    public void tearDown() throws Exception {
        stopAllGrids();

//        cleanPersistenceDir();
    }

    @Test
    public void checkSwitchUnderConstantLoad() throws Exception {
        doCheckReadonlyMode(4, 5, false, false);
    }

    @Test
    public void checkSwitchOnlyUnderConstantLoad() throws Exception {
        doCheckReadonlyMode(4, 5, true, false);
    }

    @Test
    public void checkSwitchUnderConstantLoadUpdatesFromBackup() throws Exception {
        doCheckReadonlyMode(4, 5, false, true);
    }

    /**
     * Ensure that partition counter doesn't change when evicting read-only partition.
     *
     * @throws Exception If failed.
     */
    @Test
    public void checkEvictions() throws Exception {
        IgniteEx node = startGrid(0);

        node.cluster().active(true);
        node.cluster().baselineAutoAdjustTimeout(0);

        IgniteInternalCache<Object, Object> cache = node.cachex(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 80_000; i++)
            cache.put(i, i);

        int evictedId = 6;

        GridDhtLocalPartition part = cache.context().topology().localPartition(evictedId);

        part.moving();

        long cntr = part.updateCounter();

        assert cntr > 0 : cntr;

        log.info(">xxx> >> READ-ONLY");

        node.context().cache().context().database().checkpointReadLock();

        try {
            part.readOnly(true);
        } finally {
            node.context().cache().context().database().checkpointReadUnlock();
        }

        assert cache.context().topology().localPartition(evictedId).dataStore().readOnly();

        // generate keys
        Set<Integer> keys = new HashSet<>();

        for (int i = 160_000; i < 160_300; i++) {
            if (cache.affinity().partition(i) == evictedId)
                keys.add(i);
        }

        assert !keys.isEmpty();

        CountDownLatch waitRent = new CountDownLatch(1);

        part.clearAsync();

        for (Integer key : keys)
            cache.put(key, key);

        part.onClearFinished(f -> waitRent.countDown());

        waitRent.await();

        forceCheckpoint(node);

        part = cache.context().topology().localPartition(evictedId);

        assertEquals(cntr + keys.size(), part.updateCounter());

        assertEquals(0, part.fullSize());

        assertEquals(0, part.entries(cache.context().cacheId()).size());
    }

    private void doCheckReadonlyMode(int grids, int partId, boolean validateCounters, boolean reqFromBackup) throws Exception {
        Ignite node = startGrids(grids);

        node.cluster().active(true);
        node.cluster().baselineAutoAdjustTimeout(0);

        awaitPartitionMapExchange();

        AffinityTopologyVersion topVer = grid(0).context().cache().context().exchange().readyAffinityVersion();

        AtomicBoolean stopper = new AtomicBoolean();
        AtomicBoolean rmv = new AtomicBoolean();

        CountDownLatch startLatch = new CountDownLatch(1);

        T2<Integer, Integer> pair = detectPrimaryAndBackupNodes(grids, partId, topVer);

        int primaryIdx = pair.get1();
        int backupIdx = pair.get2();

        IgniteCache<Object, Object> reqCache = grid(reqFromBackup ? backupIdx : primaryIdx).cache(DEFAULT_CACHE_NAME);

        ConstantLoader ldr = new ConstantLoader(stopper, rmv, reqCache, startLatch);

        IgniteInternalFuture<Integer> fut = GridTestUtils.runAsync(ldr);

        IgniteEx primaryNode = grid(primaryIdx);
        IgniteEx backupNode = grid(backupIdx);

        log.info(">xxx> Partition: " + partId);
        log.info(">xxx> Primary: " + primaryNode.localNode().id());
        log.info(">xxx> Backup: " + backupNode.localNode().id());

        IgniteInternalCache<Integer, Integer> backupCache = backupNode.cachex(DEFAULT_CACHE_NAME);
        GridDhtLocalPartition backupPart = backupCache.context().topology().localPartition(partId);

        backupPart.moving();

        assert backupPart.state() == MOVING : backupPart.state();

        startLatch.await();

        U.sleep(300);

        log.info(">xxx> >> READ-ONLY");

        backupNode.context().cache().context().database().checkpointReadLock();

        try {
            // Switching mode under the write lock.
            backupPart.readOnly(true);

            rmv.set(true);
        } finally {
            backupNode.context().cache().context().database().checkpointReadUnlock();
        }

        U.sleep(500);

        if (!validateCounters) {
            log.info(">xxx> >> FULL");

            backupNode.context().cache().context().database().checkpointReadLock();

            try {
                // Switching mode under the write lock.
                backupPart.readOnly(false);

                rmv.set(false);
            }
            finally {
                backupNode.context().cache().context().database().checkpointReadUnlock();
            }
        }

        stopper.set(true);

        int lastKey = fut.get();

        // validate
        int rmvStop = ldr.rmvStopIdx();
        int rmvStart = ldr.rmvStartIdx();

        CachePeekMode[] peekAll = new CachePeekMode[]{CachePeekMode.ALL};

        Iterable<Cache.Entry<Integer, Integer>> it = backupCache.localEntries(peekAll);

        log.info("Range [" + rmvStart + " - " + rmvStop + "]");

        for (Cache.Entry<Integer, Integer> e : it) {
            // todo This check in unstable when raneg starts from zero.
            if (backupCache.affinity().partition(e.getKey()) == partId && e.getKey() > rmvStart && e.getKey() < rmvStop)
                fail("range [" + rmvStart + " - " + rmvStop + "] key=" + e.getKey());
        }

        // Ensure not readonly partitions consistency.
        for (int i = 0; i < lastKey; i++) {
            for (int n = 0; n < grids; n++) {
                if (n == backupIdx)
                    continue;

                IgniteEx node0 = grid(n);

                IgniteInternalCache<Object, Object> cache = node0.cachex(DEFAULT_CACHE_NAME);

                if (cache.affinity().isPrimaryOrBackup(node0.localNode(), i) && !ldr.rmvKeys().contains(i))
                    assertNotNull("node=" + n + " " + i + " not found", cache.localPeek(i, peekAll));
            }
        }

        // validate counters
        if (validateCounters) {
            long cntr = -1;// prevReserved = -1;

            String fail = null;

            for (int n = 0; n < grids; n++) {
                GridDhtLocalPartition part = grid(n).cachex(DEFAULT_CACHE_NAME).context().topology().localPartition(partId);

                if (part == null)
                    continue;

                UUID nodeId = grid(n).localNode().id();

                if (cntr >= 0 && cntr != part.updateCounter()) {
                    fail = "Incorrect update counter on node " + nodeId + ", expected=" + cntr +
                    ", actual=" + part.updateCounter();
                }
                else
                    cntr = part.updateCounter();

                log.info("Node " + nodeId + ", counter=" + part.updateCounter() + ", reserved=" + part.reservedCounter());
            }

            if (fail != null)
                fail(fail);
        }
    }

    private T2<Integer, Integer> detectPrimaryAndBackupNodes(int cnt, int partId, AffinityTopologyVersion topVer)
        throws GridDhtInvalidPartitionException {
        Integer primary = null;
        Integer backup = null;

        for (int n = 0; n < cnt; n++) {
            try {
                if (grid(n).cachex(DEFAULT_CACHE_NAME).context().topology().localPartition(partId).primary(topVer))
                    primary = n;
                else
                    backup = n;
            }
            catch (GridDhtInvalidPartitionException | NullPointerException ignore) {
                continue;
            }

            if (primary != null && backup != null)
                return new T2<>(primary, backup);
        }

        throw new IllegalStateException("primary=" + primary + ", backup=" + backup);
    }

    /** */
    private class ConstantLoader implements Callable<Integer> {
        /** */
        private final AtomicBoolean stopper;

        /** */
        private final AtomicBoolean rmv;

        /** */
        private final IgniteCache cache;

        /** */
        private final CountDownLatch startLatch;

        /** */
        private int off = 0;

        /** */
        private int cnt = 10;

        /** */
        private volatile int rmvOffset = 0;

        /** */
        private volatile int rmvOffsetStop = -1;

        /** */
        private final Set<Integer> rmvKeys = new HashSet<>();

        /** */
        private final Random rnd = ThreadLocalRandom.current();

        public ConstantLoader(AtomicBoolean stopper, AtomicBoolean rmv, IgniteCache cache,
            CountDownLatch startLatch) {
            this.stopper = stopper;
            this.rmv = rmv;
            this.cache = cache;
            this.startLatch = startLatch;
        }

        /** {@inheritDoc} */
        @Override public Integer call() throws Exception {
            startLatch.countDown();

            boolean rmvPrev = false;

            while (!stopper.get()) {
                for (int i = off; i < off + cnt; i++) {
                    boolean rmv0 = rmv.get();

                    if (rmv0 != rmvPrev) {
                        if (rmv0)
                            rmvOffset = i;
                        else
                            rmvOffsetStop = i;

                        rmvPrev = rmv0;
                    }

                    cache.put(i, i);

                    if (off > 0 && rmv0 && rnd.nextBoolean()) {
                        int rmvKey = i - off;
                        cache.remove(rmvKey);

                        rmvKeys.add(rmvKey);
                    }
                }

                U.sleep(rnd.nextInt(10));

                off += cnt;
            }

            int last = off - 1;

            if (rmvOffsetStop == -1)
                rmvOffsetStop = last;

            return last;
        }

        public int rmvStopIdx() {
            return rmvOffsetStop;
        }

        public int rmvStartIdx() {
            return rmvOffset;
        }

        public Set<Integer> rmvKeys() {
            return rmvKeys;
        }
    }
}
