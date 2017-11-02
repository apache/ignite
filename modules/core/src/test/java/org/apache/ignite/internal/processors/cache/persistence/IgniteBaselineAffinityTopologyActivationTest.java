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
package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cluster.BaselineTopology;
import org.apache.ignite.internal.processors.cluster.DiscoveryDataClusterState;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

/**
 *
 */
public class IgniteBaselineAffinityTopologyActivationTest extends GridCommonAbstractTest {
    /** */
    private String consId;

    /** Entries count to add to cache. */
    private static final int ENTRIES_COUNT = 100;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (consId != null)
            cfg.setConsistentId(consId);

        MemoryConfiguration memCfg = new MemoryConfiguration();
        memCfg.setPageSize(1024);
        memCfg.setDefaultMemoryPolicySize(10 * 1024 * 1024);

        cfg.setMemoryConfiguration(memCfg);

        PersistentStoreConfiguration pCfg = new PersistentStoreConfiguration();
        pCfg.setWalMode(WALMode.LOG_ONLY);

        cfg.setPersistentStoreConfiguration(pCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        GridTestUtils.deleteDbFiles();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids(false);

        GridTestUtils.deleteDbFiles();
    }

    /**
     * Verifies scenario when parts of grid were activated independently they are not allowed to join
     * into the same grid again (due to risks of incompatible data modifications).
     */
    public void testIncompatibleBltNodeIsProhibitedToJoinCluster() throws Exception {
        startGridWithConsistentId("A");
        startGridWithConsistentId("B");
        startGridWithConsistentId("C").active(true);

        stopAllGrids(false);

        startGridWithConsistentId("A");
        startGridWithConsistentId("B").active(true);

        stopAllGrids(false);

        startGridWithConsistentId("C").active(true);

        stopAllGrids(false);

        startGridWithConsistentId("A");
        startGridWithConsistentId("B");

        boolean expectedExceptionThrown = false;

        try {
            startGridWithConsistentId("C");
        }
        catch (IgniteCheckedException e) {
            expectedExceptionThrown = true;

            if (e.getCause() != null && e.getCause().getCause() != null) {
                Throwable rootCause = e.getCause().getCause();

                if (!(rootCause instanceof IgniteSpiException) || !rootCause.getMessage().contains("not compatible"))
                    Assert.fail("Unexpected ignite exception was thrown: " + e);
            }
            else
                throw e;
        }

        assertTrue("Expected exception wasn't thrown.", expectedExceptionThrown);
    }

    /**
     * Test verifies that node with out-of-data but still compatible Baseline Topology is allowed to join the cluster.
     */
    public void testNodeWithOldBltIsAllowedToJoinCluster() throws Exception {
        startGridWithConsistentId("A");
        startGridWithConsistentId("B");
        startGridWithConsistentId("C").active(true);

        stopAllGrids(false);

        startGridWithConsistentId("A");
        startGridWithConsistentId("B").active(true);

        stopAllGrids(false);

        startGridWithConsistentId("A");
        startGridWithConsistentId("B");

        startGridWithConsistentId("C");
    }

    /** */
    private Ignite startGridWithConsistentId(String consId) throws Exception {
        this.consId = consId;

        return startGrid(consId);
    }

    /**
     * Verifies that when new node outside of baseline topology joins active cluster with BLT already set
     * it receives BLT from the cluster and stores it locally.
     */
    public void testNewNodeJoinsToActiveCluster() throws Exception {
        Ignite nodeA = startGridWithConsistentId("A");
        Ignite nodeB = startGridWithConsistentId("B");
        Ignite nodeC = startGridWithConsistentId("C");

        nodeC.active(true);

        BaselineTopologyVerifier verifier1 = new BaselineTopologyVerifier() {
            @Override public void verify(BaselineTopology blt) {
                assertNotNull(blt);

                assertEquals(3, blt.consistentIds().size());
            }
        };

        verifyBaselineTopology(verifier1, getBaselineTopology(nodeA));
        verifyBaselineTopology(verifier1, getBaselineTopology(nodeB));
        verifyBaselineTopology(verifier1, getBaselineTopology(nodeC));

        Ignite nodeD = startGridWithConsistentId("D");

        verifyBaselineTopology(verifier1, getBaselineTopology(nodeD));

        stopAllGrids(false);

        nodeD = startGridWithConsistentId("D");

        assertFalse(nodeD.active());

        verifyBaselineTopology(verifier1, getBaselineTopology(nodeD));
    }

    private interface BaselineTopologyVerifier {
        public void verify(BaselineTopology blt);
    }

    private void verifyBaselineTopology(BaselineTopologyVerifier bltVerifier, BaselineTopology blt) {
        bltVerifier.verify(blt);
    }

    /**
     *
     */
    public void testAutoActivationSimple() throws Exception {
        startGrids(3);

        IgniteEx srv = grid(0);

        srv.active(true);

        createAndFillCache(srv);

        // TODO: final implementation should work with cancel == true.
        stopAllGrids();

        //note: no call for activation after grid restart
        startGrids(3);

        srv = grid(0);

        awaitActivation(srv);

        checkDataInCache(srv);
    }

    /**
     * Retrieves baseline topology from ignite node instance.
     *
     * @param ig Ig.
     */
    private BaselineTopology getBaselineTopology(Ignite ig) {
        return ((DiscoveryDataClusterState) U.field(
            (Object) U.field(
                (Object) U.field(ig, "ctx"),
                "stateProc"),
            "globalState"))
            .baselineTopology();
    }

    /** */
    private void checkDataInCache(IgniteEx srv) {
        IgniteCache<Object, Object> cache = srv.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < ENTRIES_COUNT; i++) {
            TestValue testVal = (TestValue) cache.get(i);

            assertNotNull(testVal);

            assertEquals(i, testVal.id);
        }
    }

    /** */
    private void createAndFillCache(Ignite srv) {
        IgniteCache cache = srv.getOrCreateCache(cacheConfiguration());

        for (int i = 0; i < ENTRIES_COUNT; i++)
            cache.put(i, new TestValue(i, "str" + i));
    }

    /** */
    private void awaitActivation(Ignite srv) throws Exception {
        //TODO busy spinning for now, need to introduce an API to await for automatic activation
        int awaitCntr = 0;

        while (!srv.active()) {
            if (awaitCntr++ > 12)
                throw new Exception("Grid auto activation didn't happen in one minute");

            Thread.sleep(5_000);
        }
    }

    /** */
    private CacheConfiguration cacheConfiguration() {
        return new CacheConfiguration()
            .setName(DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setBackups(2)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
    }

    /** */
    private static final class TestValue {
        /** */
        private final int id;

        /** */
        private final String strId;

        /** */
        private TestValue(int id, String strId) {
            this.id = id;
            this.strId = strId;
        }
    }

}
