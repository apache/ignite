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

package org.apache.ignite.internal.mem;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.mem.InterleavedNumaAllocationStrategy;
import org.apache.ignite.mem.LocalNumaAllocationStrategy;
import org.apache.ignite.mem.NumaAllocationStrategy;
import org.apache.ignite.mem.NumaAllocator;
import org.apache.ignite.mem.SimpleNumaAllocationStrategy;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** */
@RunWith(Parameterized.class)
public class NumaAllocatorBasicTest extends GridCommonAbstractTest {
    /** */
    private static final long INITIAL_SIZE = 30L * 1024 * 1024;

    /** */
    private static final long MAX_SIZE = 100L * 1024 * 1024;

    /** */
    private static final String TEST_CACHE = "test";

    /** */
    private static final byte[] BUF = new byte[4096];

    /** */
    private static final int NUM_NODES = 3;

    static {
        ThreadLocalRandom.current().nextBytes(BUF);
    }

    /** */
    @Parameterized.Parameters(name = "allocationStrategy={0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(
            new Object[] {new LocalNumaAllocationStrategy()},
            new Object[] {new InterleavedNumaAllocationStrategy(IntStream.range(0, NumaAllocUtil.NUMA_NODES_CNT)
                .toArray())},
            new Object[] {new SimpleNumaAllocationStrategy(NumaAllocUtil.NUMA_NODES_CNT - 1)}
        );
    }

    /** */
    @Parameterized.Parameter()
    public NumaAllocationStrategy strategy;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setInitialSize(INITIAL_SIZE)
                .setMaxSize(MAX_SIZE)
                .setMetricsEnabled(true)
                .setMemoryAllocator(new NumaAllocator(strategy)));

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(NUM_NODES);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids(true);
    }

    /** */
    @Test
    public void testLoadData() throws Exception {
        IgniteEx client = startClientGrid("client");

        client.getOrCreateCache(TEST_CACHE);

        try (IgniteDataStreamer<Integer, byte[]> ds = client.dataStreamer(TEST_CACHE)) {
            int cnt = 0;
            while (hasFreeSpace()) {
                ds.addData(++cnt, BUF);

                if (cnt % 100 == 0)
                    ds.flush();
            }
        }

        assertEquals(NUM_NODES, serverGrids().count());
    }

    /** */
    private boolean hasFreeSpace() {
       return serverGrids().allMatch(g -> {
            DataRegion dr = getDefaultRegion(g);

            return dr.metrics().getTotalAllocatedSize() < 0.9 * MAX_SIZE;
        });
    }

    /** */
    private static Stream<IgniteEx> serverGrids() {
        return G.allGrids().stream().filter(g -> !g.cluster().localNode().isClient()).map(g -> (IgniteEx)g);
    }

    /** */
    private static DataRegion getDefaultRegion(IgniteEx g) {
        assertFalse(g.cluster().localNode().isClient());

        String dataRegionName = g.configuration().getDataStorageConfiguration()
            .getDefaultDataRegionConfiguration().getName();

        try {
            return g.context().cache().context().database().dataRegion(dataRegionName);
        }
        catch (IgniteCheckedException e) {
            throw new RuntimeException(e);
        }
    }
}
