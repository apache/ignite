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

package org.apache.ignite.cache.affinity;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.fair.FairAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Tests backup filter for {@link FairAffinityFunction}.
 */
public abstract class AbstractAffinityFunctionRechecksBackupFilterTest extends GridCommonAbstractTest {

    private static final String ATTRIBUTE_NAME = "machine";
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);
    private String attrVal = null;

    protected MyFilter filter = new MyFilter();

    /**
     * @return Affinity function for affinityFunctionRecheckBackupFilterTest.
     */
    protected abstract AffinityFunction affinityFunctionWithAffinityBackupFilter();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setBackups(1);

        cacheCfg.setAffinity(affinityFunctionWithAffinityBackupFilter());

        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        spi.setIpFinder(IP_FINDER);

        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(cacheCfg);
        cfg.setDiscoverySpi(spi);
        cfg.setUserAttributes(F.asMap(ATTRIBUTE_NAME, attrVal));

        return cfg;
    }

    /**
     * @throws Exception
     */
    public void testAffinityFunctionAlwaysKeepBackupCountSave() throws Exception {
        try {
            attrVal = "A";

            IgniteEx ignite = startGrid(0);
            startGrid(1);

            awaitPartitionMapExchange();

            checkBackupsAssigned(ignite);

            attrVal = "B";
            startGrid(2);

            attrVal = "C";
            startGrid(3);

            awaitPartitionMapExchange();

            checkAffinity(ignite);

            stopGrid(3);

            stopGrid(2);

            awaitPartitionMapExchange();

            checkBackupsAssigned(ignite);

        }
        finally {
            Ignition.stopAll(true);
        }
    }

    /**
     * @throws Exception
     */
    public void testAffinityFunctionRechecksAffinityBackupFilter() throws Exception {
        try {
            attrVal = "A";

            IgniteEx ignite = startGrid(0);
            startGrid(1);

            attrVal = "B";
            startGrid(2);

            attrVal = "C";
            startGrid(3);

            checkAffinity(ignite);
        }
        finally {
            Ignition.stopAll(true);
        }
    }

    /**
     * @param ignite
     */
    private void checkBackupsAssigned(IgniteEx ignite) {
        Affinity<?> aff = ignite.affinity(null);

        int backups = ignite.cache(null).getConfiguration(CacheConfiguration.class).getBackups();

        for (int i = 0; i < aff.partitions(); i++) {
            Collection<ClusterNode> nodes = aff.mapPartitionToPrimaryAndBackups(i);

            assertTrue(nodes.size() == backups + 1);
        }
    }

    private static void checkAffinity(Ignite ignite) throws InterruptedException {
        Affinity<?> aff = ignite.affinity(null);

        for (int i = 0; i < aff.partitions(); i++) {
            Collection<ClusterNode> nodes = aff.mapPartitionToPrimaryAndBackups(i);

            Collection<String> machines = new HashSet<>();

            for (ClusterNode node : nodes)
                machines.add(node.<String>attribute(ATTRIBUTE_NAME));

            assertTrue(machines.size() > 1);
        }
    }

    private static class MyFilter implements IgniteBiPredicate<ClusterNode, List<ClusterNode>> {
        @Override public boolean apply(ClusterNode clusterNode, List<ClusterNode> clusterNodes) {
            String machineId = clusterNode.attribute(ATTRIBUTE_NAME);

            for (ClusterNode node : clusterNodes) {
                if (machineId.equals(node.attribute(ATTRIBUTE_NAME)))
                    return false;
            }

            return true;
        }
    }

}
