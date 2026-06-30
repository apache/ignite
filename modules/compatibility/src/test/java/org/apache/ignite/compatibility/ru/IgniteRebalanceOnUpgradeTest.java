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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    /** Consistent ID's. */
    private static final List<String> CONSISTENT_IDS = List.of(
        "ad26bff6-5ff5-49f1-9a61-425a827953ed",
        "c1099d16-e7d7-49f4-925c-53329286c444",
        "7b880b69-8a9e-4b84-b555-250d365e2e67"
    );

    /** Source version image tag, overridable via {@code -Dru.source.commit.hash}. */
    private static final String SOURCE_COMMIT_HASH = System.getProperty("ru.source.commit.hash",
        "0ad4656eef09acda288cbad96f80f0138732d94a");

    /** Upgrade mode. */
    private static final UpgradeMode UPGRADE_MODE = UpgradeMode.valueOf(System.getProperty("ru.upgrade.mode",
        UpgradeMode.DOCKER.name()));

    /** Cache name. */
    private static final String CACHE_NAME = "ru-test-cache";

    /** Local work directory. */
    private static final File LOCAL_WORK_DIR = new File(LOCAL_WORK_DIR_PATH);

    /** Local host-JVM nodes (LOCAL mode only). */
    private final List<IgniteEx> nodes = new ArrayList<>();

    /** Consistent ID -> discovery address. */
    private final Map<String, String> addrs = new HashMap<>();

    /** Thin client. */
    private IgniteClient client;

    /** */
    @BeforeClass
    public static void beforeClass() {
        U.delete(LOCAL_WORK_DIR);
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
        try (IgniteClusterContainer cluster = new IgniteClusterContainer(SOURCE_COMMIT_HASH, CONSISTENT_IDS)) {
            cluster.start();

            ClientCacheConfiguration cfg = new ClientCacheConfiguration()
                .setName(CACHE_NAME)
                .setBackups(1)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

            ClientCache<Integer, Integer> cache = client(cluster.containers().get(0).clientAddress()).createCache(cfg);

            for (int i = 0; i < 1000; i++)
                cache.put(i, i);

            closeClient();

            upgradeCluster(cluster);

            if (UPGRADE_MODE == UpgradeMode.DOCKER)
                verifyViaDockerNodes(cluster);
            else
                verifyViaLocalNodes();
        }
        finally {
            closeClient();

            if (UPGRADE_MODE == UpgradeMode.LOCAL)
                stopLocalNodes();
        }
    }

    /** Verify data via local host-JVM nodes. */
    private void verifyViaLocalNodes() {
        IgniteCache<Integer, Integer> targetCache = nodes.get(0).cache(CACHE_NAME);

        for (int i = 0; i < 1000; i++)
            assertEquals("Data mismatch after upgrade at key: " + i, (Integer)i, targetCache.get(i));

        targetCache.put(1001, 1001);

        assertEquals((Integer)1001, targetCache.get(1001));
    }

    /** Verify data via thin client connected to upgraded Docker nodes. */
    private void verifyViaDockerNodes(IgniteClusterContainer cluster) {
        IgniteContainer con = cluster.containers().get(0);

        con.checkNodeCount(cluster.containers().size());

        ClientCache<Integer, Integer> targetCache = client(con.clientAddress()).getOrCreateCache(CACHE_NAME);

        for (int i = 0; i < 1000; i++)
            assertEquals("Data mismatch after upgrade at key: " + i, (Integer)i, targetCache.get(i));

        targetCache.put(1001, 1001);

        assertEquals((Integer)1001, targetCache.get(1001));
    }

    /** */
    private void upgradeCluster(IgniteClusterContainer srcCluster) throws Exception {
        List<IgniteContainer> srcContainers = srcCluster.containers();

        if (UPGRADE_MODE == UpgradeMode.LOCAL)
            for (IgniteContainer con : srcContainers)
                addrs.put(con.consistentId(), con.discoveryAddress());

        for (int i = 0; i < srcContainers.size(); i++) {
            IgniteContainer con = srcContainers.get(i);

            log.info(">>> Upgrade node=" + con.consistentId() + " (mode=" + UPGRADE_MODE + ")");

            if (UPGRADE_MODE == UpgradeMode.DOCKER)
                con.upgradeAndRestart();
            else
                upgradeLocally(con, i);
        }
    }

    /** Stop container, start a local host-JVM node with the same consistent ID. */
    private void upgradeLocally(IgniteContainer con, int idx) throws Exception {
        // Address containers use to reach this (host JVM) node: the Docker bridge gateway on Linux, the
        // host.docker.internal alias on macOS.
        String hostIp = IgniteContainer.LINUX
            ? con.gatewayIp()
            : con.execInContainer("sh", "-c",
                "getent ahostsv4 host.docker.internal | awk '{print $1}' | head -1").getStdout().trim();

        con.stop();

        addrs.remove(con.consistentId());

        IgniteEx ignite = startGrid(configuration(con.consistentId(), con.localWorkDirectory(), addrs.values(), hostIp, idx));

        assertTrue("Upgraded node did not rejoin the full topology in time",
            waitForCondition(() -> CONSISTENT_IDS.size() == ignite.cluster().nodes().size(), DFLT_TEST_TIMEOUT));

        addrs.put(con.consistentId(), "127.0.0.1:" + (48500 + idx));

        nodes.add(ignite);
    }

    /** */
    private IgniteConfiguration configuration(String nodeId, String workDir, Collection<String> addrs0, String ip, int idx) {
        DataRegionConfiguration dataRegionCfg = new DataRegionConfiguration()
            .setName("testRegion")
            .setInitialSize(1024L * 1024 * 1024)
            .setMaxSize(10L * 1024 * 1024 * 1024)
            .setPersistenceEnabled(true);

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi()
            .setLocalAddress("0.0.0.0")
            .setIpFinder(new TcpDiscoveryVmIpFinder().setAddresses(addrs0))
            // Short socket timeout: unreachable container-internal addresses must fail fast before the
            // host-reachable 127.0.0.1:<published-port> (advertised by the containers) is tried.
            .setSocketTimeout(1000)
            .setNetworkTimeout(20000)
            .setJoinTimeout(30000)
            .setLocalPort(48500 + idx);

        // On macOS communication binds to loopback (discovery stays on 0.0.0.0 to satisfy Ignite's non-loopback
        // join check) so the node advertises only 127.0.0.1 + the resolver-mapped Docker host address -- no
        // unreachable host LAN IPs for the containers to stall on. A short connect timeout makes the node's
        // own outgoing attempts to unreachable container-internal (172.x) addresses give up in ~1s (they
        // otherwise hang in SYN_SENT) and fall through to the reachable 127.0.0.1:<published-port>.
        TcpCommunicationSpi commSpi = new TcpCommunicationSpi()
            // macOS: bind comm to loopback (advertised to containers via the resolver as the Docker-host address).
            // Linux: bind to all interfaces so containers reach this host node at the Docker bridge gateway IP.
            .setLocalAddress(IgniteContainer.LINUX ? "0.0.0.0" : "127.0.0.1")
            .setLocalPort(49100 + idx)
            .setConnectTimeout(1000)
            .setMaxConnectTimeout(10000)
            // The NIO connect to a blackholed container-internal (172.x) address is not aborted by
            // connectTimeout on macOS (it hangs in SYN_SENT for the OS timeout, ~75s), stalling the exchange.
            // Pre-filter unreachable addresses so only the reachable 127.0.0.1:<published-port> is used.
            .setFilterReachableAddresses(true);

        return new IgniteConfiguration()
            .setIgniteInstanceName(nodeId)
            .setConsistentId(nodeId)
            .setWorkDirectory(workDir)
            .setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(dataRegionCfg))
            .setDiscoverySpi(discoverySpi)
            .setAddressResolver(addr -> {
                int port = addr.getPort();

                // Each sequentially started host node binds the next port in the discovery (48500+) and
                // communication (47100+) ranges; map them all to the Docker host address so the containers
                // can reach every host JVM node.
                if ((port >= 48500 && port < 48600) || (port >= 49100 && port < 49200))
                    return Set.of(new InetSocketAddress(ip, port));

                return Set.of(addr);
            })
            .setCommunicationSpi(commSpi);
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

    /** */
    private void stopLocalNodes() {
        for (IgniteEx node : nodes)
            if (node != null)
                Ignition.stop(node.name(), false);

        nodes.clear();
    }

    /**
     * Upgrade mode, overridable via {@code -Dru.upgrade.mode} (DOCKER|LOCAL).
     * <ul>
     *   <li>DOCKER — all nodes stay in Docker; in-place upgrade by swapping libs inside containers (default)</li>
     *   <li>LOCAL  — source cluster in Docker, upgraded to local host-JVM nodes</li>
     * </ul>
     */
    private enum UpgradeMode {
        /** */
        DOCKER,

        /** */
        LOCAL
    }
}
