/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteClientReconnectAbstractTest;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.lang.IgniteInClosureX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Test dynamic WAL mode change.
 */

public abstract class WalModeChangeCommonAbstractSelfTest extends GridCommonAbstractTest {
    /** Shared IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Node filter. */
    private static final IgnitePredicate<ClusterNode> FILTER = new CacheNodeFilter();

    /** Cache name. */
    protected static final String CACHE_NAME = "cache";

    /** Cache name 2. */
    protected static final String CACHE_NAME_2 = "cache_2";

    /** Cache name 2. */
    protected static final String CACHE_NAME_3 = "cache_3";

    /** Volatile data region. */
    protected static final String REGION_VOLATILE = "volatile";

    /** Server 1. */
    protected static final String SRV_1 = "srv_1";

    /** Server 2. */
    protected static final String SRV_2 = "srv_2";

    /** Server 3. */
    protected static final String SRV_3 = "srv_3";

    /** Client. */
    protected static final String CLI = "cli";

    /** Client 2. */
    protected static final String CLI_2 = "cli_2";

    /** Node attribute for filtering. */
    protected static final String FILTER_ATTR = "FILTER";

    /** Whether this is JDBC test. */
    protected final boolean jdbc;

    /**
     * Constructor.
     *
     * @param jdbc Whether this is JDBC test.
     */
    protected WalModeChangeCommonAbstractSelfTest(boolean jdbc) {
        this.jdbc = jdbc;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (Ignite node0 : Ignition.allGrids()) {
            Collection<String> cacheNames = node0.cacheNames();

            for (String cacheName : cacheNames)
                destroyCache(node0, cacheName);
        }

        awaitPartitionMapExchange();

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (Ignite node0 : Ignition.allGrids()) {
                    if (!node0.cacheNames().isEmpty())
                        return false;
                }

                return true;
            }
        }, 2000));
    }

    /**
     * Create cache.
     *
     * @param node Node.
     * @param ccfg Cache configuration.
     */
    @SuppressWarnings("unchecked")
    protected void createCache(Ignite node, CacheConfiguration ccfg) throws IgniteCheckedException {
        node.getOrCreateCache(ccfg);

        alignCacheTopologyVersion(node);
    }

    /**
     * Destroy cache.
     *
     * @param node Node.
     * @param cacheName Cache name.
     */
    protected void destroyCache(Ignite node, String cacheName) throws IgniteCheckedException {
        node.destroyCache(cacheName);

        alignCacheTopologyVersion(node);
    }

    /**
     * Waits for the topology version to be not less than one registered on source node.
     *
     * @param src Source node.
     * @throws IgniteCheckedException If failed to wait on affinity ready future.
     */
    protected void alignCacheTopologyVersion(Ignite src) throws IgniteCheckedException {
        AffinityTopologyVersion topVer = ((IgniteEx)src).context().cache().context().exchange().readyAffinityVersion();

        info("Will wait for topology version on all nodes: " + topVer);

        for (Ignite ignite : Ignition.allGrids()) {
            IgniteInternalFuture<?> ready = ((IgniteEx)ignite).context().cache().context().exchange()
                .affinityReadyFuture(topVer);

            if (ready != null)
                ready.get();
        }
    }

    /**
     * Enable WAL.
     *
     * @param node Node.
     * @param cacheName Cache name.
     * @return Result.
     */
    protected boolean walEnable(Ignite node, String cacheName) {
        return node.cluster().enableWal(cacheName);
    }

    /**
     * Disable WAL.
     *
     * @param node Node.
     * @param cacheName Cache name.
     * @return Result.
     */
    protected boolean walDisable(Ignite node, String cacheName) {
        return node.cluster().disableWal(cacheName);
    }

    /**
     * Enable WAL.
     *
     * @param node Node.
     * @param cacheName Cache name.
     * @param expRes Expected result.
     */
    protected void assertWalEnable(Ignite node, String cacheName, boolean expRes) {
        boolean res = walEnable(node, cacheName);

        assertEquals(expRes, res);
    }

    /**
     * Disable WAL.
     *
     * @param node Node.
     * @param cacheName Cache name.
     * @param expRes Expected result.
     */
    protected void assertWalDisable(Ignite node, String cacheName, boolean expRes) {
        boolean res = walDisable(node, cacheName);

        if (!jdbc)
            assertEquals(expRes, res);
    }

    /**
     * Assert WAL state on all nodes.
     *
     * @param cacheName Cache name.
     * @param expState Expected state.
     * @throws IgniteCheckedException If failed.
     */
    protected void assertForAllNodes(String cacheName, boolean expState) throws IgniteCheckedException {
        for (final Ignite node : Ignition.allGrids()) {
            info(">>> Checking WAL state on node: " + node.name());

            assert GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return node.cluster().isWalEnabled(cacheName) == expState;
                }
            }, 1000L);
        }
    }

    /**
     * Ensure exception is thrown.
     *
     * @param cmd Command.
     * @param errCls Expected error class.
     * @param errMsg Expected error message.
     */
    protected void assertThrows(Callable<Void> cmd, Class errCls, String errMsg) {
        try {
            cmd.call();

            fail("Exception is not thrown");
        }
        catch (Exception e) {
            if (jdbc) {
                e = (Exception) e.getCause();

                assert e instanceof SQLException : e.getClass().getName();
            }
            else
                assert F.eq(errCls, e.getClass());

            if (errMsg != null) {
                assert e.getMessage() != null;
                assert e.getMessage().startsWith(errMsg) : e.getMessage();
            }
        }
    }

    /**
     * Execute certain logic for all nodes.
     *
     * @param task Task.
     * @throws Exception If failed.
     */
    protected void forAllNodes(IgniteInClosureX<Ignite> task) throws Exception {
        for (Ignite node : Ignition.allGrids()) {
            try {
                info("");
                info(">>> Executing test on node: " + node.name());

                task.applyx(node);
            }
            finally {
                for (Ignite node0 : Ignition.allGrids()) {
                    Collection<String> cacheNames = node0.cacheNames();

                    for (String cacheName : cacheNames)
                        destroyCache(node0, cacheName);
                }
            }
        }
    }

    /**
     * Create node configuration.
     *
     * @param name Name.
     * @param cli Client flag.
     * @param filter Whether node should be filtered out.
     * @return Node configuration.
     */
    protected IgniteConfiguration config(String name, boolean cli, boolean filter) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setIgniteInstanceName(name);
        cfg.setClientMode(cli);
        cfg.setLocalHost("127.0.0.1");

        cfg.setDiscoverySpi(new IgniteClientReconnectAbstractTest.TestTcpDiscoverySpi().setIpFinder(IP_FINDER));

        DataRegionConfiguration regionCfg = new DataRegionConfiguration()
            .setPersistenceEnabled(true)
            .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE);

        DataRegionConfiguration volatileRegionCfg = new DataRegionConfiguration().setName(REGION_VOLATILE)
            .setPersistenceEnabled(false);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        storageCfg.setDefaultDataRegionConfiguration(regionCfg);
        storageCfg.setDataRegionConfigurations(volatileRegionCfg);

        cfg.setDataStorageConfiguration(storageCfg);

        if (filter)
            cfg.setUserAttributes(Collections.singletonMap(FILTER_ATTR, true));

        return cfg;
    }

    /**
     * Create common cache configuration (default name, transactional).
     *
     * @param mode Mode.
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfig(CacheMode mode) {
        return cacheConfig(CACHE_NAME, mode, TRANSACTIONAL);
    }

    /**
     * Create cache configuration.
     *
     * @param name Name.
     * @param mode Mode.
     * @param atomicityMode Atomicity mode.
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfig(String name, CacheMode mode, CacheAtomicityMode atomicityMode) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(name);
        ccfg.setCacheMode(mode);
        ccfg.setAtomicityMode(atomicityMode);

        ccfg.setNodeFilter(FILTER);

        return ccfg;
    }

    /**
     * Cache node filter.
     */
    protected static class CacheNodeFilter implements IgnitePredicate<ClusterNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            Object filterAttr = node.attribute(FILTER_ATTR);

            return filterAttr == null;
        }
    }
}
