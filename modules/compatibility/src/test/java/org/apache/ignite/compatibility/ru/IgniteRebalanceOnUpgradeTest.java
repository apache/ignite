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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compatibility.testframework.testcontainers.IgniteClusterContainer;
import org.apache.ignite.compatibility.testframework.testcontainers.IgniteContainer;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.sharedfs.TcpDiscoverySharedFsIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.ignite.compatibility.testframework.testcontainers.IgniteContainer.LOCAL_WORK_DIR_PATH;
import static org.apache.ignite.testframework.GridTestUtils.DFLT_TEST_TIMEOUT;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** Smoke test for rolling upgrade with persistence. */
public class IgniteRebalanceOnUpgradeTest extends GridCommonAbstractTest {
    /** Node IDs. */
    private static final List<String> NODE_IDS = List.of(
        "ad26bff6-5ff5-49f1-9a61-425a827953ed",
        "c1099d16-e7d7-49f4-925c-53329286c444",
        "7b880b69-8a9e-4b84-b555-250d365e2e67"
    );

    /** Source commit hash. */
    private static final String SOURCE_COMMIT_HASH = "6b172a8b";

    /** Cache name. */
    private static final String CACHE_NAME = "ru-test-cache";

    /** Local work directory. */
    private static final File LOCAL_WORK_DIR = new File(LOCAL_WORK_DIR_PATH);

    /** Thin client. */
    private IgniteClient client;

    /** */
    private final List<IgniteEx> nodes = new ArrayList<>();

    /** */
    private final Map<String, String> addrs = new HashMap<>();

    /** */
    public IgniteRebalanceOnUpgradeTest() {
        // Конструктор остается пустым, так как ipFinder инициализируется в методе configuration
    }

    /** */
    @BeforeClass
    public static void beforeClass() {
        U.delete(LOCAL_WORK_DIR);

        // Установка свойства для предпочтения IPv4 адресов
        System.setProperty("java.net.preferIPv4Stack", "true");
        
        // Также устанавливаем свойство для принудительного использования IPv4
        System.setProperty("java.net.preferIPv6Addresses", "false");
    }

    /** */
    @AfterClass
    public static void afterClass() {
        U.delete(LOCAL_WORK_DIR);
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return super.getTestTimeout() * 2;
    }

    /** Basic RU test. */
    @Test
    public void testRollingUpgrade() throws Exception {
        try (IgniteClusterContainer cluster = new IgniteClusterContainer(SOURCE_COMMIT_HASH, NODE_IDS)) {
            cluster.start();

            for (IgniteContainer container : cluster.containers())
                addrs.put(container.nodeId(), container.discoveryAddress());

            System.out.println(">>> Addresses=" + addrs);

            IgniteContainer node = cluster.containers().get(0);

            node.activateCluster();

            ClientCacheConfiguration cfg = new ClientCacheConfiguration()
                .setName(CACHE_NAME)
                .setBackups(1)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

            ClientCache<Integer, Integer> cache = client(node.clientAddress()).createCache(cfg);

            for (int i = 0; i < 1000; i++)
                cache.put(i, i);

            closeClient();
            
            upgradeCluster(cluster);

            IgniteCache<Integer, Integer> targetCache = nodes.get(0).cache(CACHE_NAME);

            for (int i = 0; i < 1000; i++)
                assertEquals("Data mismatch after upgrade at key: " + i, i, (int)targetCache.get(i));

            targetCache.put(1001, 1001);

            assertEquals(1001, (int)targetCache.get(1001));
        }
        finally {
            closeClient();
        }
    }

    /** */
    private void upgradeCluster(IgniteClusterContainer srcCluster) throws Exception {
        for (IgniteContainer container : srcCluster.containers()) {
            System.out.println(">>> Upgrade " + container.nodeId());

            container.stop();

            addrs.remove(container.nodeId());

            System.out.println(">>> CONNECT TO=" + addrs.values());

            IgniteEx ignite = null;

            try {
                Thread.sleep(5_000);

                ignite = startGrid(configuration(container.nodeId(), container.localWorkDirectory(), addrs.values()));
            }
            catch (Exception ex) {
                System.out.println(">>> ERR=" + ex);
                Thread.sleep(Long.MAX_VALUE);
            }

            IgniteEx finalIgnite = ignite;

            waitForCondition(() -> NODE_IDS.size() == finalIgnite.cluster().nodes().size(), DFLT_TEST_TIMEOUT);

            addrs.put(container.nodeId(), ignite.cluster().localNode().addresses().stream().findFirst().orElseThrow());

            nodes.add(ignite);
        }
    }

    /** */
    private IgniteConfiguration configuration(String nodeId, String workDir, Collection<String> addrs0) throws UnknownHostException {
        DataRegionConfiguration dataRegionCfg = new DataRegionConfiguration()
            .setName("testRegion")
            .setInitialSize(1024L * 1024 * 1024)
            .setMaxSize(10L * 1024 * 1024 * 1024)
            .setPersistenceEnabled(true);

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi()
            .setIpFinder(new TcpDiscoveryVmIpFinder().setAddresses(addrs0))
            .setNetworkTimeout(10000)
            .setAckTimeout(5000)
            .setJoinTimeout(10000)
            // Установим локальный адрес для связи с контейнерами
            .setLocalAddress(InetAddress.getLocalHost().getHostAddress())
            // Установим порты для дисковери
            .setLocalPort(47500);
            //.setLocalPortRange(20);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();
            //.setLocalAddress("0.0.0.0");
            //.setLocalPort(47100)
            //.setLocalPortRange(100);

        return new IgniteConfiguration()
            //.setLocalHost(InetAddress.getLocalHost().getHostAddress())
            .setConsistentId(nodeId)
            .setWorkDirectory(workDir)
            .setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(dataRegionCfg))
            .setDiscoverySpi(discoverySpi);
            //.setCommunicationSpi(commSpi);
    }

    /** */
    private IgniteClient client(String addr) {
        if (client == null)
            client = Ignition.startClient(new ClientConfiguration().setAddresses(addr));

        return client;
    }

    /** */
    private void closeClient() {
        if (client != null) {
            client.close();

            client = null;
        }
    }
}


/**
 [23:47:56,251][INFO][tcp-disco-sock-reader-[]-#11-#113][TcpDiscoverySpi] Started serving remote node connection [rmtAddr=/192.168.65.1:47965, rmtPort=47965]

 [23:47:56,253][INFO][tcp-disco-sock-reader-[53822b8b 192.168.65.1:47965]-#11-#113][TcpDiscoverySpi] Initialized connection with remote server node [nodeId=53822b8b-42d2-4e41-9866-5fb49a60395b, rmtAddr=/192.168.65.1:47965]

 [23:47:56,971][WARNING][tcp-disco-sock-reader-[53822b8b 192.168.65.1:47965]-#11-#113][TcpDiscoverySpi] Failed to ping node [nodeId=53822b8b-42d2-4e41-9866-5fb49a60395b, address=/127.0.0.1:47520]. Node has left or is leaving topology. Cause: Connection refused

 [23:47:56,972][WARNING][tcp-disco-sock-reader-[53822b8b 192.168.65.1:47965]-#11-#113][TcpDiscoverySpi] Failed to ping joining node, closing connection. [node=TcpDiscoveryNode [id=53822b8b-42d2-4e41-9866-5fb49a60395b, consistentId=ad26bff6-5ff5-49f1-9a61-425a827953ed, addrs=ArrayList [127.0.0.1], sockAddrs=HashSet [/127.0.0.1:47520], discPort=47520, order=0, intOrder=0, loc=false, ver=2.19.0#19700101-sha1:00000000, isClient=false, dataCenterId=null]]
 */
