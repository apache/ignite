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

package org.apache.ignite.cache.affinity.rendezvous;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Verifies behaviour of {@link MdcAffinityBackupFilter} - guarantees that each DC has at least one copy of every partition.
 * Verified distribution uniformity in each DC separately.
 */
public class MdcAffinityBackupFilterSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int PARTS_CNT = 1024;

    /** */
    private int backups;

    /** */
    private String[] dcIds;

    /** */
    private IgniteBiPredicate<ClusterNode, List<ClusterNode>> filter;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(defaultCacheConfiguration()
            .setBackups(backups)
            .setAffinity(
                new RendezvousAffinityFunction(false, PARTS_CNT)
                    .setAffinityBackupFilter(filter)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Verifies that {@link MdcAffinityBackupFilter} prohibits single data center deployment.
     */
    @Test
    public void testSingleDcDeploymentIsProhibited() {
        verifyMdcAffinityBackupFilterValidation(1, 1, "Number of datacenters must be at least 2.");
    }

    /**
     * Verifies that {@link MdcAffinityBackupFilter} enforces even number of partition copies per datacenter.
     */
    @Test
    public void testUniformNumberOfPartitionCopiesPerDcIsEnforced() {
        verifyMdcAffinityBackupFilterValidation(3, 1, "recommended value is 2");

        verifyMdcAffinityBackupFilterValidation(3, 7, "recommended values are 5 and 8");
    }

    /** */
    private void verifyMdcAffinityBackupFilterValidation(int dcsNum, int backups, String msg) {
        try {
            new MdcAffinityBackupFilter(dcsNum, backups);
        } catch (IllegalArgumentException argEx) {
            String errMsg = argEx.getMessage();

            assertTrue(errMsg.contains(msg));
        }
    }

    /**
     * Verifies that partition copies are assigned evenly across a cluster in two data centers.
     * <p/>
     * When a node from one data center is stopped, partition distribution is that data center should stay uniform.
     *
     * @throws Exception If failed.
     */
    @Test
    public void test2DcDistribution() throws Exception {
        dcIds = new String[] {"DC_0", "DC_1"};
        int nodesPerDc = 4;
        backups = 3;
        filter = new MdcAffinityBackupFilter(dcIds.length, backups);

        IgniteEx srv = startClusterAcrossDataCenters(dcIds, nodesPerDc);

        verifyDistributionProperties(srv, dcIds, nodesPerDc, backups);

        //stopping one node in DC_1 should not compromise distribution as there are additional nodes in the same DC
        stopGrid(5);

        awaitPartitionMapExchange();

        verifyDistributionProperties(srv, dcIds, nodesPerDc, backups);

        //stopping another node in DC_1 should not compromise distribution as well
        stopGrid(6);

        awaitPartitionMapExchange();

        verifyDistributionProperties(srv, dcIds, nodesPerDc, backups);
    }

    /**
     * Verifies that partition copies are assigned evenly across a cluster in three data centers.
     *
     * @throws Exception If failed.
     */
    @Test
    public void test3DcDistribution() throws Exception {
        dcIds = new String[] {"DC_0", "DC_1", "DC_2"};
        int nodesPerDc = 2;
        backups = 5;
        filter = new MdcAffinityBackupFilter(dcIds.length, backups);

        IgniteEx srv = startClusterAcrossDataCenters(dcIds, 2);

        verifyDistributionProperties(srv, dcIds, nodesPerDc, backups);
    }

    /**
     * Verifies that node is prohibited from joining cluster if its affinityBackupFilter configuration differs
     * from the one specified in the cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAffinityFilterConfigurationValidation() throws Exception {
        dcIds = new String[] {"DC_0", "DC_1"};
        backups = 3;
        filter = new MdcAffinityBackupFilter(dcIds.length, backups);
        startGrid(0);

        filter = new ClusterNodeAttributeAffinityBackupFilter("DC_ID");
        try {
            startGrid(1);

            fail("Expected exception was not thrown.");
        } catch (IgniteCheckedException e) {
            String errMsg = e.getMessage();

            assertNotNull(errMsg);

            assertTrue(errMsg.contains("Affinity backup filter class mismatch"));
        }

        filter = new MdcAffinityBackupFilter(dcIds.length, backups + dcIds.length);
        try {
            startGrid(1);

            fail("Expected exception was not thrown.");
        } catch (IgniteCheckedException e) {
            String errMsg = e.getMessage();

            assertNotNull(errMsg);

            assertTrue(errMsg.contains("Affinity backup filter mismatch"));
        }
    }

    /** Starts specified number of nodes in each DC. */
    private IgniteEx startClusterAcrossDataCenters(String[] dcIds, int nodesPerDc) throws Exception {
        int nodeIdx = 0;
        IgniteEx lastNode = null;

        for (String dcId : dcIds) {
            System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, dcId);

            for (int i = 0; i < nodesPerDc; i++)
                lastNode = startGrid(nodeIdx++);
        }

        return lastNode;
    }

    /**
     * Checks that copies of each partition are distributed evenly across data centers and copies are spread evenly across nodes.
     */
    private void verifyDistributionProperties(
        IgniteEx srv,
        String[] dcIds,
        int nodesPerDc,
        int backups
    ) {
        IgniteCache cache = srv.getOrCreateCache(DEFAULT_CACHE_NAME);

        int partCnt = cacheConfiguration(srv.configuration(), cache.getName()).getAffinity().partitions();
        Affinity aff = affinity(cache);
        int expectedCopiesPerNode = (backups + 1) / dcIds.length;

        Map<ClusterNode, Integer> overallCopiesPerNode = new HashMap<>();
        int[] copiesPerNode = new int[dcIds.length * nodesPerDc];

        for (int partId = 0; partId < partCnt; partId++) {
            int[] partCopiesPerDc = new int[dcIds.length];

            Collection<ClusterNode> nodes = aff.mapKeyToPrimaryAndBackups(partId);

            //calculate actual number of copies in each data center
            //aggregate copies per each node
            for (ClusterNode node : nodes) {
                copiesPerNode[(int)(node.order() - 1)]++;

                overallCopiesPerNode.compute(node, (k, v) -> {
                    if (v == null)
                        return 1;
                    else
                        return v + 1;
                });

                for (int j = 0; j < dcIds.length; j++) {
                    if (node.dataCenterId().equals(dcIds[j])) {
                        partCopiesPerDc[j]++;
                        break;
                    }
                }
            }

            verifyCopyInEachDcGuarantee(partId, expectedCopiesPerNode, partCopiesPerDc);
        }

        verifyDistributionUniformity(dcIds, overallCopiesPerNode);
    }

    /** */
    private void verifyCopyInEachDcGuarantee(int partId, int expectedCopiesPerNode, int[] partCopiesPerDc) {
        for (int dcIdx = 0; dcIdx < dcIds.length; dcIdx++) {
            assertEquals(String.format("Unexpected number of copies of partition %d in data center %s", partId, dcIds[dcIdx]),
                expectedCopiesPerNode,
                partCopiesPerDc[dcIdx]);
        }
    }

    /** */
    private void verifyDistributionUniformity(String[] dcIds, Map<ClusterNode, Integer> overallCopiesPerNode) {
        for (String dcId : dcIds) {
            long nodesInDc = overallCopiesPerNode.entrySet().stream().filter(e -> e.getKey().dataCenterId().equals(dcId)).count();

            double idealCopiesPerNode = (double)((PARTS_CNT * (backups + 1)) / (dcIds.length * nodesInDc));

            List<Integer> numOfCopiesPerNode = overallCopiesPerNode.entrySet().stream()
                .filter(e -> e.getKey().dataCenterId().equals(dcId)).map(Map.Entry::getValue).collect(Collectors.toList());

            for (int copiesOnNode : numOfCopiesPerNode) {
                double deviation = (Math.abs(copiesOnNode - idealCopiesPerNode) / idealCopiesPerNode);

                assertTrue(
                    String.format("Too big deviation from ideal distribution: partitions assigned = %d, " +
                            "ideal partitions assigned = %d, deviation = %d",
                        copiesOnNode,
                        (int)idealCopiesPerNode,
                        (int)(deviation * 100)
                    ),
                    deviation < 0.1);
            }
        }
    }
}
