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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;

/**
 * Tests partition scan query fallback.
 */
public class CacheScanPartitionQueryFallbackSelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 5;

    /** Kys count. */
    private static final int KEYS_CNT = 1;

    /** Backups. */
    private int backups;

    /** Cache mode. */
    private CacheMode cacheMode;

    /** Fallback. */
    private boolean fallback;

    /** Primary node id. */
    private static volatile UUID expNodeId;

    /** Fail node id. */
    private static volatile UUID failNodeId;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCommunicationSpi(new TestCommunicationSpi());

        CacheConfiguration ccfg = defaultCacheConfiguration();
        ccfg.setCacheMode(cacheMode);
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setBackups(backups);
        ccfg.setNearConfiguration(null);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimary() throws Exception {
        cacheMode = CacheMode.PARTITIONED;
        backups = 0;
        failNodeId = null;
        fallback = false;

        doTestScanPartition();
    }

    /**
     * @throws Exception If failed.
     */
    public void testFallbackToBackup() throws Exception {
        cacheMode = CacheMode.PARTITIONED;
        backups = 1;
        failNodeId = null;
        fallback = true;

        doTestScanPartition();
    }

    /**
     * @throws Exception If failed.
     */
    protected void doTestScanPartition() throws Exception {
        try {
            Ignite ignite = startGrids(GRID_CNT);

            IgniteCacheProxy<Integer, Integer> cache =
                (IgniteCacheProxy<Integer, Integer>)ignite.<Integer, Integer>cache(null);

            Map<Integer, Map<Integer, Integer>> entries = new HashMap<>();

            for (int i = 0; i < KEYS_CNT; i++) {
                cache.put(i, i);

                int part = cache.context().affinity().partition(i);

                Map<Integer, Integer> partEntries = entries.get(part);

                if (partEntries == null)
                    entries.put(part, partEntries = new HashMap<>());

                partEntries.put(i, i);
            }

            IgniteBiTuple<Integer, UUID> tup = remotePartition(cache.context(), true);

            int part = tup.get1();

            if (fallback)
                failNodeId = tup.get2();
            else
                expNodeId = tup.get2();

            if (fallback)
                expNodeId = remoteBackup(part, cache.context());

            CacheQuery<Map.Entry<Integer, Integer>> qry = cache.context().queries().createScanQuery(null, part, false);

            CacheQueryFuture<Map.Entry<Integer, Integer>> fut = qry.execute();

            Collection<Map.Entry<Integer, Integer>> expEntries = fut.get();

            for (Map.Entry<Integer, Integer> e : expEntries) {
                Map<Integer, Integer> map = entries.get(part);

                if(map == null)
                    assertTrue(expEntries.isEmpty());
                else
                    assertEquals(map.get(e.getKey()), e.getValue());
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param cctx Cctx.
     * @param primary Primary.
     */
    private IgniteBiTuple<Integer, UUID> remotePartition(final GridCacheContext cctx, boolean primary) {
        ClusterNode node = F.first(cctx.kernalContext().grid().cluster().forRemotes().nodes());

        GridCacheAffinityManager affMgr = cctx.affinity();

        AffinityTopologyVersion topVer = affMgr.affinityTopologyVersion();

        Set<Integer> parts = primary ?
            affMgr.primaryPartitions(node.id(), topVer) : affMgr.backupPartitions(node.id(), topVer);

        return new IgniteBiTuple<>(F.first(parts), node.id());
    }

    /**
     * @param part Partition.
     * @param cctx Cctx.
     */
    private UUID remoteBackup(int part, final GridCacheContext cctx) {
        final UUID locUuid = cctx.localNodeId();

        GridCacheAffinityManager affMgr = cctx.affinity();

        AffinityTopologyVersion topVer = affMgr.affinityTopologyVersion();

        return F.first(F.view(affMgr.backups(part, topVer), new IgnitePredicate<ClusterNode>() {
            @Override public boolean apply(ClusterNode node) {
                return !node.id().equals(locUuid);
            }
        })).id();
    }

    /**
     *
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg)
            throws IgniteSpiException {
            Object origMsg = ((GridIoMessage)msg).message();

            if (origMsg instanceof GridCacheQueryRequest) {
                if (node.id().equals(failNodeId))
                    throw new IgniteSpiException("");
                else
                    assertEquals(expNodeId, node.id());
            }

            super.sendMessage(node, msg);
        }
    }
}
