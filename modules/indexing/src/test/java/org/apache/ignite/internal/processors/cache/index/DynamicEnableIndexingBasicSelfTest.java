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

import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 *
 */
@RunWith(Parameterized.class)
public class DynamicEnableIndexingBasicSelfTest extends DynamicEnableIndexingAbstractTest {
    /** Test parameters. */
    @Parameters(name = "isNear={0},opNode={1}")
    public static Iterable<Object[]> params() {
        return Arrays.asList(
                new Object[] {true, IDX_CLI},
                new Object[] {true, IDX_SRV_CRD},
                new Object[] {true, IDX_SRV_NON_CRD},
                new Object[] {true, IDX_SRV_FILTERED},
                new Object[] {false, IDX_CLI},
                new Object[] {false, IDX_SRV_CRD},
                new Object[] {false, IDX_SRV_NON_CRD},
                new Object[] {false, IDX_SRV_FILTERED}
        );
    }

    /** */
    @Parameter(0)
    public boolean hasNear;

    /** */
    @Parameter(1)
    public int nodeIdx;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTest();

        for (IgniteConfiguration cfg : configurations())
            Ignition.start(cfg);

        node().cluster().state(ClusterState.ACTIVE);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        node().context().cache().dynamicDestroyCache(POI_CACHE_NAME, false, true, false, null).get();

        super.afterTest();
    }

    /** */
    @Test
    public void testAtomicPartitioned() throws Exception {
        enableDynamicIndexingTest(CacheMode.PARTITIONED, CacheAtomicityMode.ATOMIC);
    }

    /** */
    @Test
    public void testAtomicReplicated() throws Exception {
        enableDynamicIndexingTest(CacheMode.REPLICATED, CacheAtomicityMode.ATOMIC);
    }

    /** */
    @Test
    public void testTransactionalPartitioned() throws Exception {
        enableDynamicIndexingTest(CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL);
    }

    /** */
    @Test
    public void testTransactionalReplicated() throws Exception {
        enableDynamicIndexingTest(CacheMode.REPLICATED, CacheAtomicityMode.TRANSACTIONAL);
    }

    /** */
    private void enableDynamicIndexingTest(CacheMode mode, CacheAtomicityMode atomicityMode) throws Exception {
        CacheConfiguration<?, ?> ccfg = testCacheConfiguration(POI_CACHE_NAME, mode, atomicityMode);

        if (hasNear)
            ccfg.setNearConfiguration(new NearCacheConfiguration<>());

        node().context().cache().dynamicStartCache(ccfg, POI_CACHE_NAME, null, true, true, true).get();

        grid(IDX_CLI_NEAR_ONLY).getOrCreateNearCache(POI_CACHE_NAME, new NearCacheConfiguration<>());

        loadData(node(), 0, NUM_ENTRIES / 2);

        createTable(node().cache(POI_CACHE_NAME));

        grid(IDX_SRV_CRD).cache(POI_CACHE_NAME).indexReadyFuture().get();

        loadData(node(), NUM_ENTRIES / 2, NUM_ENTRIES);

        for (Ignite ig: G.allGrids())
            performQueryingIntegrityCheck((IgniteEx)ig);
    }

    /** */
    private IgniteEx node() {
        return grid(nodeIdx);
    }
}
