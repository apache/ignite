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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.util.AttributeNodeFilter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 */
@RunWith(Parameterized.class)
public class CachePartitionLossWithRestartsTest extends GridCommonAbstractTest {
    /** */
    private static final int PARTS_CNT = 32;

    /** */
    private static final String START_CACHE_ATTR = "has_cache";

    /** Possible values: -1, 0, 2 */
    @Parameterized.Parameter(value = 0)
    public int nonAffIdx;

    /** Possible values: true, false */
    @Parameterized.Parameter(value = 1)
    public boolean startClientCache;

    /** Possible values: true, false */
    @Parameterized.Parameter(value = 2)
    public boolean dfltRegionPersistence;

    /** Possible values: 3, -1 */
    @Parameterized.Parameter(value = 3)
    public int clientIdx;

    /** Possible values: true, false */
    @Parameterized.Parameter(value = 4)
    public boolean mvccEnabled;

    /** */
    @Parameterized.Parameters(name = "{0} {1} {2} {3}")
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>();

        for (boolean mvcc : new boolean[]{false, true}) {
            for (boolean persistent : new boolean[] {false, true}) {
                params.add(new Object[] {-1, false, persistent, 3, mvcc});
                params.add(new Object[] {0, false, persistent, 3, mvcc});
                params.add(new Object[] {2, false, persistent, 3, mvcc});

                params.add(new Object[] {-1, false, persistent, -1, mvcc});
                params.add(new Object[] {0, false, persistent, -1, mvcc});
                params.add(new Object[] {2, false, persistent, -1, mvcc});

                params.add(new Object[] {-1, true, persistent, 3, mvcc});
                params.add(new Object[] {0, true, persistent, 3, mvcc});
                params.add(new Object[] {2, true, persistent, 3, mvcc});

                params.add(new Object[] {-1, true, persistent, -1, mvcc});
                params.add(new Object[] {0, true, persistent, -1, mvcc});
                params.add(new Object[] {2, true, persistent, -1, mvcc});
            }
        }

        return params;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setActiveOnStart(false);

        cfg.setConsistentId(igniteInstanceName);

        if (getTestIgniteInstanceIndex(igniteInstanceName) == clientIdx)
            cfg.setClientMode(true);

        cfg.setIncludeEventTypes(EventType.EVTS_ALL);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();
        dsCfg.setWalSegmentSize(4 * 1024 * 1024);
        dsCfg.setWalMode(WALMode.LOG_ONLY);

        final int size = 50 * 1024 * 1024;

        DataRegionConfiguration dfltRegCfg = new DataRegionConfiguration();
        dfltRegCfg.setName(DEFAULT_CACHE_NAME).setInitialSize(size).setMaxSize(size).setPersistenceEnabled(dfltRegionPersistence);

        dsCfg.setDefaultDataRegionConfiguration(dfltRegCfg);

        cfg.setDataStorageConfiguration(dsCfg);

        // Do not start cache on non-affinity node.
        CacheConfiguration ccfg = defaultCacheConfiguration().setNearConfiguration(null).
                setNodeFilter(new AttributeNodeFilter(START_CACHE_ATTR, Boolean.TRUE)).
                setBackups(0).
                setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT));

        if (mvccEnabled)
            ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT);

        if (startClientCache)
            cfg.setCacheConfiguration(ccfg);

        if (getTestIgniteInstanceIndex(igniteInstanceName) != nonAffIdx) {
            cfg.setUserAttributes(F.asMap(START_CACHE_ATTR, Boolean.TRUE));

            if (!startClientCache)
                cfg.setCacheConfiguration(ccfg);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     *
     */
    @Test
    public void testPartitionLossDetectionOnClientTopology() throws Exception {
        final IgniteEx crd = startGrids(3);
        crd.cluster().baselineAutoAdjustEnabled(false);
        crd.cluster().active(true);

        assertTrue(grid(1).cache(DEFAULT_CACHE_NAME).lostPartitions().isEmpty());
        assertTrue(grid(2).cache(DEFAULT_CACHE_NAME).lostPartitions().isEmpty());

        final IgniteEx g3 = startGrid(3);

        awaitPartitionMapExchange();

        stopGrid(1);

        // Loss detection is done just before exchange future completion.
        // Will wait for it's finishing.
        AffinityTopologyVersion topVer = new AffinityTopologyVersion(5, 0);

        GridDhtPartitionTopology top0 = waitForDetection(crd, topVer);
        GridDhtPartitionTopology top1 = waitForDetection(grid(2), topVer);
        GridDhtPartitionTopology top2 = waitForDetection(g3, topVer);

        final Set<Integer> lost1 = new HashSet<>(top0.lostPartitions());
        final Set<Integer> lost2 = new HashSet<>(top1.lostPartitions());
        final Set<Integer> lost3 = new HashSet<>(top2.lostPartitions());

        assertFalse(lost1.isEmpty());

        assertEquals(lost1, lost2);
        assertEquals(lost1, lost3);

        GridDhtPartitionTopology top = startGrid(1).cachex(DEFAULT_CACHE_NAME).context().topology();
        assertEquals(lost1, top.lostPartitions());

        // TODO https://issues.apache.org/jira/browse/IGNITE-13053
        grid(1).resetLostPartitions(Collections.singleton(DEFAULT_CACHE_NAME));

        awaitPartitionMapExchange();
    }

    /**
     * @param node Node.
     * @param topVer Topology version.
     */
    private GridDhtPartitionTopology waitForDetection(IgniteEx node, AffinityTopologyVersion topVer) throws Exception {
        GridCacheSharedContext<Object, Object> cctx = node.context().cache().context();

        CacheGroupDescriptor desc = cctx.affinity().cacheGroups().get(CU.cacheId(DEFAULT_CACHE_NAME));

        CacheGroupContext grp = cctx.cache().cacheGroup(desc.groupId());

        GridDhtPartitionTopology top = grp != null ? grp.topology() :
            cctx.exchange().clientTopology(desc.groupId(), null);

        cctx.exchange().affinityReadyFuture(topVer).get();
        cctx.exchange().lastTopologyFuture().get();

        return top;
    }
}
