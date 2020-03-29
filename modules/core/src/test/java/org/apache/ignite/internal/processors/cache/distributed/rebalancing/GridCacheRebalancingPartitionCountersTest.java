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
package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopologyImpl;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class GridCacheRebalancingPartitionCountersTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private static final int PARTITIONS_CNT = 10;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
                .setConsistentId(igniteInstanceName)
                .setDataStorageConfiguration(
                    new DataStorageConfiguration()
                        .setCheckpointFrequency(3_000)
                        .setDefaultDataRegionConfiguration(
                            new DataRegionConfiguration()
                                .setPersistenceEnabled(true)
                                .setMaxSize(100L * 1024 * 1024))
                        .setWalMode(WALMode.LOG_ONLY))
                .setCacheConfiguration(new CacheConfiguration(CACHE_NAME)
                    .setAtomicityMode(atomicityMode())
                    .setBackups(2)
                    .setRebalanceBatchSize(4096) // Force to create several supply messages during rebalancing.
                    .setAffinity(
                        new RendezvousAffinityFunction(false, PARTITIONS_CNT)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false));
    }

    /**
     * @return Cache atomicity mode.
     */
    protected CacheAtomicityMode atomicityMode() {
        return CacheAtomicityMode.ATOMIC;
    }

    /**
     *
     */
    private boolean contains(int[] arr, int a) {
        for (int i : arr)
            if (i == a)
                return true;

        return false;
    }

    /**
     * Tests that after rebalancing all partition update counters have the same value on all nodes.
     */
    @Test
    public void test() throws Exception {
        IgniteEx ignite = (IgniteEx)startGrids(3);

        ignite.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ignite.cache(CACHE_NAME);

        for (int i = 0; i < 256; i++)
            cache.put(i, i);

        final int problemNode = 2;

        IgniteEx node = (IgniteEx) ignite(problemNode);
        int[] primaryPartitions = node.affinity(CACHE_NAME).primaryPartitions(node.cluster().localNode());

        ignite.cluster().active(false);

        boolean primaryRemoved = false;
        for (int i = 0; i < PARTITIONS_CNT; i++) {
            String nodeName = getTestIgniteInstanceName(problemNode);

            Path dirPath = Paths.get(U.defaultWorkDirectory(), "db", nodeName.replace(".", "_"), CACHE_NAME + "-" + CACHE_NAME);

            info("Path: " + dirPath.toString());

            assertTrue(Files.exists(dirPath));

            for (File f : dirPath.toFile().listFiles()) {
                if (f.getName().equals("part-" + i + ".bin")) {
                    if (contains(primaryPartitions, i)) {
                        info("Removing: " + f.getName());

                        primaryRemoved = true;

                        f.delete();
                    }
                }
                else if ("index.bin".equals(f.getName())) {
                    info("Removing: " + f.getName());

                    f.delete();
                }
            }
        }

        assertTrue(primaryRemoved);

        ignite.cluster().active(true);

        awaitPartitionMapExchange();

        List<String> issues = new ArrayList<>();
        HashMap<Integer, Long> partMap = new HashMap<>();

        for (int i = 0; i < 3; i++)
            checkUpdCounter((IgniteEx)ignite(i), issues, partMap);

        for (String issue : issues)
            error(issue);

        assertTrue(issues.isEmpty());
    }

    /**
     *
     */
    private void checkUpdCounter(IgniteEx ignite, List<String> issues, HashMap<Integer, Long> partMap) {
        final CacheGroupContext grpCtx = ignite.context().cache().cacheGroup(CU.cacheId(CACHE_NAME));

        assertNotNull(grpCtx);

        GridDhtPartitionTopologyImpl top = (GridDhtPartitionTopologyImpl)grpCtx.topology();

        List<GridDhtLocalPartition> locParts = top.localPartitions();

        for (GridDhtLocalPartition part : locParts) {
            Long cnt = partMap.get(part.id());

            if (cnt == null)
                partMap.put(part.id(), part.updateCounter());

            if ((cnt != null && part.updateCounter() != cnt) || part.updateCounter() == 0)
                issues.add("Node name " + ignite.name() + "Part = " + part.id() + " updCounter " + part.updateCounter());
        }
    }
}
