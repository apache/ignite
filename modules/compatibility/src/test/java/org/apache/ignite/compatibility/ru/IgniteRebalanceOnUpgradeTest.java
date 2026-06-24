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

package org.apache.ignite.compatibility.ru;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compatibility.testframework.testcontainers.IgniteClusterContainer;
import org.apache.ignite.compatibility.testframework.testcontainers.IgniteContainer;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.compatibility.testframework.testcontainers.IgniteContainer.LOCAL_WORK_DIR_PATH;
import static org.apache.ignite.testframework.GridTestUtils.DFLT_TEST_TIMEOUT;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** Smoke test for rolling upgrade with persistence. */
public class IgniteRebalanceOnUpgradeTest extends GridCommonAbstractTest {
    /** Source commit hash. */
    private static final String SOURCE_COMMIT_HASH = "b93116048a4df65881b279142871fc16b91c54b3"; //"6b172a8b";

    /** Cache name. */
    private static final String CACHE_NAME = "ru-test-cache";

    /** Local work directory. */
    private static final File LOCAL_WORK_DIR = new File(LOCAL_WORK_DIR_PATH);

    /** */
    private final List<IgniteEx> nodes = new ArrayList<>();

    /** */
    private final int NODES_CNT = 3;

    /** {@inheritDoc} */
    @Override protected void beforeTest() {
        U.delete(LOCAL_WORK_DIR);
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return super.getTestTimeout() * 3;
    }

    /** Basic RU test. */
    @Test
    public void testRollingUpgrade() throws Exception {
        try (IgniteClusterContainer cluster = new IgniteClusterContainer(SOURCE_COMMIT_HASH, NODES_CNT)) {
            cluster.start();

            ClientCacheConfiguration cliCacheCfg = getClientConfiguration();

            try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER))) {
                ClientCache<Integer, Integer> cache = client.getOrCreateCache(cliCacheCfg);

                for (int i = 0; i < 1000; i++)
                    cache.put(i, i);
            }
            
            upgradeCluster(cluster);

            IgniteCache<Integer, Integer> targetCache = nodes.get(0).cache(CACHE_NAME);

            for (int i = 0; i < 1000; i++)
                assertEquals("Data mismatch after upgrade at key: " + i, i, (int)targetCache.get(i));

            targetCache.put(1001, 1001);

            assertEquals(1001, (int)targetCache.get(1001));
        }
    }

    /** */
    private void upgradeCluster(IgniteClusterContainer srcCluster) throws Exception {
        int nodeIdx = 0;

        for (IgniteContainer container : srcCluster.containers()) {
            System.out.println(">>> Upgrade " + container.getHost());

            container.stop();

            IgniteEx ignite;

            try {
                Thread.sleep(20_000);

                ignite = startGrid(configuration(nodeIdx, container.localWorkDirectory()));

                nodeIdx++;
            }
            catch (Exception ex) {
                System.out.println(">>> ERR=" + ex);

                throw ex;
            }

            IgniteEx finalIgnite = ignite;

            waitForCondition(() -> NODES_CNT == finalIgnite.cluster().nodes().size(), DFLT_TEST_TIMEOUT);

            nodes.add(ignite);
        }
    }

    /** */
    private IgniteConfiguration configuration(int nodeIdx, String workDir) {
        DataRegionConfiguration dataRegionCfg = new DataRegionConfiguration()
            .setName("testRegion")
            .setInitialSize(100L * 1024 * 1024)
            .setMaxSize(1024 * 1024 * 1024)
            .setPersistenceEnabled(true);

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi()
            .setIpFinder(new TcpDiscoveryVmIpFinder().setAddresses(Collections.singleton("127.0.0.1:47500..47509")));

        return new IgniteConfiguration()
            .setIgniteInstanceName(getTestIgniteInstanceName(nodeIdx))
            .setWorkDirectory(workDir)
            .setDataStorageConfiguration(
                new DataStorageConfiguration().setDefaultDataRegionConfiguration(dataRegionCfg))
            .setDiscoverySpi(discoverySpi);
    }

    /** */
    private ClientCacheConfiguration getClientConfiguration() {
        return new ClientCacheConfiguration()
            .setName(CACHE_NAME)
            .setBackups(1)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
    }
}
