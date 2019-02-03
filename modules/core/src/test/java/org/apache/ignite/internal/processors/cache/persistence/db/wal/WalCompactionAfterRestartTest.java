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
package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.events.WalSegmentCompactedEvent;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_WAL_SEGMENT_COMPACTED;

/** */
public class WalCompactionAfterRestartTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(200L * 1024 * 1024))
            .setWalMode(WALMode.LOG_ONLY)
            .setWalSegmentSize(512 * 1024)
            .setWalCompactionEnabled(true)
            .setMaxWalArchiveSize(2 * 512 * 1024)
        );

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(DEFAULT_CACHE_NAME);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 16));
        ccfg.setBackups(0);

        cfg.setCacheConfiguration(ccfg);
        cfg.setConsistentId(name);

        cfg.setIncludeEventTypes(EVT_WAL_SEGMENT_COMPACTED);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void test() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        doCachePuts(ig, 10_000);

        ig.cluster().active(false);

        stopGrid(0);

        IgniteEx ig0 = startGrid(0);

        ig0.cluster().active(true);

        List<IgniteBiTuple<Long, Long>> discrepancies = Collections.synchronizedList(new ArrayList<>());

        ig0.events().localListen(e -> {
            long evtSegIdx = ((WalSegmentCompactedEvent)e).getAbsWalSegmentIdx();
            long lastCompactedIdx = ig0.context().cache().context().wal().lastCompactedSegment();

            if (lastCompactedIdx < 0 || lastCompactedIdx > evtSegIdx)
                discrepancies.add(F.t(evtSegIdx, lastCompactedIdx));

            return true;
        }, EVT_WAL_SEGMENT_COMPACTED);

        doCachePuts(ig0, 5_000);

        stopGrid(0);

        if (!discrepancies.isEmpty()) {
            fail("Discrepancies (EVT_WAL_SEGMENT_COMPACTED index vs. lastCompactedSegment):" + System.lineSeparator() +
                discrepancies.stream()
                    .map(t -> String.format("%d <-> %d", t.get1(), t.get2()))
                    .collect(Collectors.joining(System.lineSeparator())));
        }
    }

    /** */
    private void doCachePuts(IgniteEx ig, long millis) throws IgniteCheckedException {
        IgniteCache<Integer, byte[]> cache = ig.getOrCreateCache(DEFAULT_CACHE_NAME);

        AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<Long> putFut = GridTestUtils.runMultiThreadedAsync(() -> {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            while (!stop.get())
                cache.put(rnd.nextInt(), "Ignite".getBytes());
        }, 4, "cache-filler");

        U.sleep(millis);

        stop.set(true);

        putFut.get();
    }
}
