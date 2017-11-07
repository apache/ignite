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

import java.util.Arrays;
import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
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
     * Verifies that when old but compatible node
     * (it is the node that once wasn't presented in activationHistory but hasn't participated in any branching point)
     * joins the cluster after restart, cluster gets activated.
     */
    public void testAutoActivationWithCompatibleOldNode() throws Exception {
        startGridWithConsistentId("A");
        startGridWithConsistentId("B");
        startGridWithConsistentId("C").active(true);

        stopAllGrids(false);

        startGridWithConsistentId("A");
        startGridWithConsistentId("B").active(true);

        stopAllGrids(false);

        startGridWithConsistentId("A");
        startGridWithConsistentId("B");
        final Ignite nodeC = startGridWithConsistentId("C");

        boolean active = GridTestUtils.waitForCondition(
            new GridAbsPredicate() {
                @Override public boolean apply() {
                    return nodeC.active();
                }
            },
            10_000
        );

        assertTrue(active);
    }

    /**
     * Verifies that online nodes cannot be removed from BaselineTopology (this may change in future).
     */
    public void testOnlineNodesCannotBeRemovedFromBaselineTopology() throws Exception {
        Ignite nodeA = startGridWithConsistentId("A");
        Ignite nodeB = startGridWithConsistentId("B");
        Ignite nodeC = startGridWithConsistentId("C");

        nodeC.active(true);

        boolean expectedExceptionIsThrown = false;

        try {
            ((IgniteEx) nodeC).setBaselineTopology(Arrays.asList(nodeA.cluster().localNode(), nodeB.cluster().localNode()));
        } catch (IgniteException e) {
            assertTrue(e.getMessage().startsWith("Removing online nodes"));

            expectedExceptionIsThrown = true;
        }

        assertTrue(expectedExceptionIsThrown);
    }

    /**
     *
     */
    public void testNodeFailsToJoinWithIncompatiblePreviousBaselineTopology() throws Exception {
        startGridWithConsistentId("A");
        startGridWithConsistentId("B");
        Ignite nodeC = startGridWithConsistentId("C");

        nodeC.active(true);

        stopAllGrids(false);

        Ignite nodeA = startGridWithConsistentId("A");
        startGridWithConsistentId("B").active(true);

        ((IgniteEx)nodeA).setBaselineTopology(nodeA.cluster().forServers().nodes());

        stopAllGrids(false);

        startGridWithConsistentId("C").active(true);

        stopGrid("C", false);

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
        final long expectedHash1 = (long)"A".hashCode() + "B".hashCode() + "C".hashCode();

        BaselineTopologyVerifier verifier1 = new BaselineTopologyVerifier() {
            @Override public void verify(BaselineTopology blt) {
                assertNotNull(blt);

                assertEquals(3, blt.consistentIds().size());

                long activationHash = U.field(blt, "activationHash");

                assertEquals(expectedHash1, activationHash);
            }
        };

        final long expectedHash2 = (long)"A".hashCode() + "B".hashCode();

        BaselineTopologyVerifier verifier2 = new BaselineTopologyVerifier() {
            @Override public void verify(BaselineTopology blt) {
                assertNotNull(blt);

                assertEquals(3, blt.consistentIds().size());

                long activationHash = U.field(blt, "activationHash");

                assertEquals(expectedHash2, activationHash);
            }
        };

        Ignite nodeA = startGridWithConsistentId("A");
        Ignite nodeB = startGridWithConsistentId("B");
        Ignite nodeC = startGridWithConsistentId("C");

        nodeC.active(true);
        verifyBaselineTopologyOnNodes(verifier1, new Ignite[] {nodeA, nodeB, nodeC});

        stopAllGrids(false);

        nodeA = startGridWithConsistentId("A");
        nodeB = startGridWithConsistentId("B");

        nodeB.active(true);

        verifyBaselineTopologyOnNodes(verifier2, new Ignite[] {nodeA, nodeB});

        stopAllGrids(false);

        nodeA = startGridWithConsistentId("A");
        nodeB = startGridWithConsistentId("B");
        nodeC = startGridWithConsistentId("C");

        verifyBaselineTopologyOnNodes(verifier2, new Ignite[] {nodeA, nodeB, nodeC});
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

        verifyBaselineTopologyOnNodes(verifier1, new Ignite[] {nodeA, nodeB, nodeC});

        Ignite nodeD = startGridWithConsistentId("D");

        verifyBaselineTopologyOnNodes(verifier1, new Ignite[] {nodeD});

        stopAllGrids(false);

        nodeD = startGridWithConsistentId("D");

        assertFalse(nodeD.active());

        verifyBaselineTopologyOnNodes(verifier1, new Ignite[] {nodeD});
    }

    /**
     *
     */
    public void testRemoveNodeFromBaselineTopology() throws Exception {
        final long expectedActivationHash = (long)"A".hashCode() + "C".hashCode();

        BaselineTopologyVerifier verifier = new BaselineTopologyVerifier() {
            @Override public void verify(BaselineTopology blt) {
                assertNotNull(blt);

                assertEquals(2, blt.consistentIds().size());

                long activationHash = U.field(blt, "activationHash");

                assertEquals(expectedActivationHash, activationHash);
            }
        };

        Ignite nodeA = startGridWithConsistentId("A");
        startGridWithConsistentId("B");
        Ignite nodeC = startGridWithConsistentId("C");

        nodeC.active(true);

        stopGrid("B", false);

        ((IgniteEx)nodeA).setBaselineTopology(nodeA.cluster().forServers().nodes());

        verifyBaselineTopologyOnNodes(verifier, new Ignite[] {nodeA, nodeC});

        stopAllGrids(false);

        nodeA = startGridWithConsistentId("A");
        nodeC = startGridWithConsistentId("C");

        boolean activated = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return grid("A").active();
            }
        }, 10_000);

        assertTrue(activated);

        verifyBaselineTopologyOnNodes(verifier, new Ignite[] {nodeA, nodeC});
    }

    /**
     *
     */
    public void testAddNodeToBaselineTopology() throws Exception {
        final long expectedActivationHash = (long)"A".hashCode() + "B".hashCode() + "C".hashCode() + "D".hashCode();

        BaselineTopologyVerifier verifier = new BaselineTopologyVerifier() {
            @Override public void verify(BaselineTopology blt) {
                assertNotNull(blt);

                assertEquals(4, blt.consistentIds().size());

                long activationHash = U.field(blt, "activationHash");

                assertEquals(expectedActivationHash, activationHash);
            }
        };

        Ignite nodeA = startGridWithConsistentId("A");
        Ignite nodeB = startGridWithConsistentId("B");
        Ignite nodeC = startGridWithConsistentId("C");

        nodeC.active(true);

        IgniteEx nodeD = (IgniteEx) startGridWithConsistentId("D");

        nodeD.setBaselineTopology(nodeA.cluster().forServers().nodes());

        verifyBaselineTopologyOnNodes(verifier, new Ignite[]{nodeA, nodeB, nodeC, nodeD});
    }

    /**
     * Verifies that baseline topology is removed successfully through baseline changing API.
     */
    public void testRemoveBaselineTopology() throws Exception {
        BaselineTopologyVerifier verifier = new BaselineTopologyVerifier() {
            @Override public void verify(BaselineTopology blt) {
                assertNull(blt);
            }
        };

        Ignite nodeA = startGridWithConsistentId("A");
        Ignite nodeB = startGridWithConsistentId("B");
        Ignite nodeC = startGridWithConsistentId("C");

        nodeA.active(true);

        ((IgniteEx)nodeA).setBaselineTopology(null);

        verifyBaselineTopologyOnNodes(verifier, new Ignite[] {nodeA, nodeB, nodeC});
    }

    /** */
    private interface BaselineTopologyVerifier {
        /** */
        public void verify(BaselineTopology blt);
    }

    /** */
    private void verifyBaselineTopologyOnNodes(BaselineTopologyVerifier bltVerifier, Ignite[] igs) {
        for (Ignite ig : igs) {
            BaselineTopology blt = getBaselineTopology(ig);

            bltVerifier.verify(blt);
        }
    }

    /** */
    private Ignite startGridWithConsistentId(String consId) throws Exception {
        this.consId = consId;

        return startGrid(consId);
    }

    /** */
    public void testAutoActivationSimple() throws Exception {
        startGrids(3);

        IgniteEx srv = grid(0);

        srv.active(true);

        createAndFillCache(srv);

        // TODO: final implementation should work with cancel == true.
        stopAllGrids();

        //note: no call for activation after grid restart
        startGrids(3);

        final Ignite ig = grid(0);

        boolean clusterActive = GridTestUtils.waitForCondition(
            new GridAbsPredicate() {
                @Override public boolean apply() {
                    return ig.active();
                }
            },
            10_000);

        assertTrue(clusterActive);

        checkDataInCache((IgniteEx) ig);
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
