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

package org.apache.ignite.internal.processors.cache.index;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests basic functionality of enabling indexing.
 */
@RunWith(Parameterized.class)
public class DynamicEnableIndexingBasicSelfTest extends DynamicEnableIndexingAbstractTest {
    /** Test parameters. */
    @Parameters(name = "hasNear={0},nodeIdx={1},cacheMode={2},atomicityMode={3}")
    public static Iterable<Object[]> params() {
        int[] opNodes = new int[] {IDX_CLI, IDX_SRV_CRD, IDX_SRV_NON_CRD, IDX_SRV_FILTERED};

        CacheMode[] cacheModes = new CacheMode[] {CacheMode.PARTITIONED, CacheMode.REPLICATED};

        CacheAtomicityMode[] atomicityModes = new CacheAtomicityMode[] {
            CacheAtomicityMode.ATOMIC,
            CacheAtomicityMode.TRANSACTIONAL,
            CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT
        };

        List<Object[]> res = new ArrayList<>();

        for (int node : opNodes) {
            for (CacheMode cacheMode : cacheModes) {
                for (CacheAtomicityMode atomicityMode : atomicityModes) {
                    res.add(new Object[] {true, node, cacheMode, atomicityMode});

                    // For TRANSACTIONAL_SNAPSHOT near caches is forbidden.
                    if (atomicityMode != CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT)
                        res.add(new Object[] {false, node, cacheMode, atomicityMode});

                }
            }
        }

        return res;
    }

    /** */
    @Parameter(0)
    public Boolean hasNear;

    /** */
    @Parameter(1)
    public int nodeIdx;

    /** */
    @Parameter(2)
    public CacheMode cacheMode;

    /** */
    @Parameter(3)
    public CacheAtomicityMode atomicityMode;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        for (IgniteConfiguration cfg : configurations())
            startGrid(cfg);

        node().cluster().state(ClusterState.ACTIVE);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        CacheConfiguration<?, ?> ccfg = testCacheConfiguration(POI_CACHE_NAME, cacheMode, atomicityMode);

        if (hasNear && atomicityMode != CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT)
            ccfg.setNearConfiguration(new NearCacheConfiguration<>());

        node().getOrCreateCache(ccfg);

        if (atomicityMode != CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT)
            grid(IDX_CLI_NEAR_ONLY).getOrCreateNearCache(POI_CACHE_NAME, new NearCacheConfiguration<>());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        node().destroyCache(POI_CACHE_NAME);

        super.afterTest();
    }

    /** */
    @Test
    public void testEnableDynamicIndexing() throws Exception {
        loadData(node(), 0, NUM_ENTRIES / 2);

        createTable();

        grid(IDX_SRV_CRD).cache(POI_CACHE_NAME).indexReadyFuture().get();

        loadData(node(), NUM_ENTRIES / 2, NUM_ENTRIES);

        for (Ignite ig : G.allGrids()) {
            assertEquals(NUM_ENTRIES, query(ig, SELECT_ALL_QUERY).size());

            performQueryingIntegrityCheck(ig);

            checkQueryParallelism((IgniteEx)ig, cacheMode);
        }
    }

    /** */
    @SuppressWarnings("ThrowableNotThrown")
    private void createTable() {
        if (cacheMode == CacheMode.REPLICATED) {
            GridTestUtils.assertThrows(log, () -> createTable(node().cache(POI_CACHE_NAME), QUERY_PARALLELISM),
                IgniteException.class, "Segmented indices are supported for PARTITIONED mode only.");

            createTable(node().cache(POI_CACHE_NAME), 1);
        }
        else
            createTable(node().cache(POI_CACHE_NAME), QUERY_PARALLELISM);
    }

    /** */
    private IgniteEx node() {
        return grid(nodeIdx);
    }
}
