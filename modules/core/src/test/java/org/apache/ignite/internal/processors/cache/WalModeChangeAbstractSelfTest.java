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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.lang.IgniteInClosureX;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 * Test dynamic WAL mode change.
 */
@SuppressWarnings({"unchecked", "ThrowableNotThrown"})
public abstract class WalModeChangeAbstractSelfTest extends GridCommonAbstractTest {

    // TODO: Test concurrent cache destroy (one thread send messages, another thread creates/destroys cache)

    // TODO: Test node join (info should be shared)

    // TODO: Test client disconnect/reconnect

    // TODO: Test node failures (one thread spans messages, another kills oldest node and create another one)

    // TODO: Test concurrent requests from different nodes, count success/error rates

    // TODO: Test with concurrent cache operations.

    // TODO: Duplicate tests from within JDBC.

    /** Shared IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Node filter. */
    private static final IgnitePredicate<ClusterNode> FILTER = new CacheNodeFilter();

    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Cache name 2. */
    private static final String CACHE_NAME_2 = "cache_2";

    /** Volatile data region. */
    private static final String REGION_VOLATILE = "volatile";

    /** Server 1. */
    private static final String SRV_1 = "srv_1";

    /** Server 2. */
    private static final String SRV_2 = "srv_2";

    /** Server 3. */
    private static final String SRV_3 = "srv_3";

    /** Client. */
    private static final String CLI = "cli";

    /** Node attribute for filtering. */
    private static final String FILTER_ATTR = "FILTER";

    /** Whether coordinator node should be filtered out. */
    private final boolean filterOnCrd;

    /** Whether this is JDBC test. */
    private final boolean jdbc;

    /**
     * Constructor.
     *
     * @param filterOnCrd Whether coordinator node should be filtered out.
     * @param jdbc Whether this is JDBC test.
     */
    protected WalModeChangeAbstractSelfTest(boolean filterOnCrd, boolean jdbc) {
        this.filterOnCrd = filterOnCrd;
        this.jdbc = jdbc;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        deleteWorkFiles();

        startGrid(config(SRV_1, false, filterOnCrd));
        startGrid(config(SRV_2, false, false));
        startGrid(config(SRV_3, false, !filterOnCrd));

        Ignite cli = startGrid(config(CLI, true, false));

        cli.active(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        deleteWorkFiles();
    }

    /**
     * Negative case: cache name is null.
     *
     * @throws Exception If failed.
     */
    public void testNullCacheName() throws Exception {
        forAllNodes(new IgniteInClosureX<Ignite>() {
            @Override public void applyx(Ignite ignite) throws IgniteCheckedException {
                GridTestUtils.assertThrows(log, new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        walEnable(ignite, null);

                        return null;
                    }
                }, NullPointerException.class, null);
            }
        });
    }

    /**
     * Negative case: no cache.
     *
     * @throws Exception If failed.
     */
    public void testNoCache() throws Exception {
        forAllNodes(new IgniteInClosureX<Ignite>() {
            @Override public void applyx(Ignite ignite) throws IgniteCheckedException {
                GridTestUtils.assertThrows(log, new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        walEnable(ignite, CACHE_NAME);

                        return null;
                    }
                }, IgniteException.class, "Cache doesn't exist");
            }
        });
    }

    /**
     * Negative case: trying to disable WAL for cache in a shared cache group.
     *
     * @throws Exception If failed.
     */
    public void testSharedCacheGroup() throws Exception {
        forAllNodes(new IgniteInClosureX<Ignite>() {
            @Override public void applyx(Ignite ignite) throws IgniteCheckedException {
                ignite.createCache(cacheConfig(CACHE_NAME, PARTITIONED, TRANSACTIONAL).setGroupName("grp"));
                ignite.createCache(cacheConfig(CACHE_NAME_2, PARTITIONED, TRANSACTIONAL).setGroupName("grp"));

                GridTestUtils.assertThrows(log, new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        walDisable(ignite, CACHE_NAME);

                        return null;
                    }
                }, IgniteException.class, "Cannot change WAL mode because not all cache names belonging to the " +
                    "group are provided");

                GridTestUtils.assertThrows(log, new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        walEnable(ignite, CACHE_NAME);

                        return null;
                    }
                }, IgniteException.class, "Cannot change WAL mode because not all cache names belonging to the " +
                    "group are provided");

                assert ignite.cluster().isWalEnabled(CACHE_NAME);
                assert ignite.cluster().isWalEnabled(CACHE_NAME_2);
            }
        });
    }

    /**
     * Negative test case: disabled persistence.
     *
     * @throws Exception If failed.
     */
    public void testPersistenceDisabled() throws Exception {
        forAllNodes(new IgniteInClosureX<Ignite>() {
            @Override public void applyx(Ignite ignite) throws IgniteCheckedException {
                ignite.createCache(cacheConfig(PARTITIONED).setDataRegionName(REGION_VOLATILE));

                GridTestUtils.assertThrows(log, new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        walDisable(ignite, CACHE_NAME);

                        return null;
                    }
                }, IgniteException.class, "Cannot change WAL mode because persistence is not enabled for cache(s)");

                GridTestUtils.assertThrows(log, new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        walEnable(ignite, CACHE_NAME);

                        return null;
                    }
                }, IgniteException.class, "Cannot change WAL mode because persistence is not enabled for cache(s)");

                assert !ignite.cluster().isWalEnabled(CACHE_NAME);
            }
        });
    }

    /**
     * Negative case: LOCAL cache.
     *
     * @throws Exception If failed.
     */
    public void testLocalCache() throws Exception {
        forAllNodes(new IgniteInClosureX<Ignite>() {
            @Override public void applyx(Ignite ignite) throws IgniteCheckedException {
                ignite.createCache(cacheConfig(LOCAL).setDataRegionName(REGION_VOLATILE));

                GridTestUtils.assertThrows(log, new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        walDisable(ignite, CACHE_NAME);

                        return null;
                    }
                }, IgniteException.class, "WAL mode cannot be changed for LOCAL cache(s)");

                GridTestUtils.assertThrows(log, new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        walEnable(ignite, CACHE_NAME);

                        return null;
                    }
                }, IgniteException.class, "WAL mode cannot be changed for LOCAL cache(s)");

                assert !ignite.cluster().isWalEnabled(CACHE_NAME);
            }
        });
    }

    /**
     * Test enable/disable for PARTITIONED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    public void testEnableDisablePartitionedAtomic() throws Exception {
        checkEnableDisable(PARTITIONED, ATOMIC);
    }

    /**
     * Test enable/disable for PARTITIONED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    public void testEnableDisablePartitionedTransactional() throws Exception {
        checkEnableDisable(PARTITIONED, TRANSACTIONAL);
    }

    /**
     * Test enable/disable for REPLICATED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    public void testEnableDisableReplicatedAtomic() throws Exception {
        checkEnableDisable(REPLICATED, ATOMIC);
    }

    /**
     * Test enable/disable for REPLICATED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    public void testEnableDisableReplicatedTransactional() throws Exception {
        checkEnableDisable(REPLICATED, TRANSACTIONAL);
    }

    /**
     * Check normal disable-enable flow.
     *
     * @throws Exception If failed.
     */
    private void checkEnableDisable(CacheMode mode, CacheAtomicityMode atomicityMode) throws Exception {
        forAllNodes(new IgniteInClosureX<Ignite>() {
            @Override public void applyx(Ignite ignite) throws IgniteCheckedException {
                ignite.createCache(cacheConfig(CACHE_NAME, mode, atomicityMode));

                for (int i = 0; i < 2; i++) {
                    assertForAllNodes(CACHE_NAME, true);

                    assertWalEnable(ignite, CACHE_NAME, false);
                    assertWalDisable(ignite, CACHE_NAME, true);

                    assertForAllNodes(CACHE_NAME, false);

                    assertWalDisable(ignite, CACHE_NAME, false);
                    assertWalEnable(ignite, CACHE_NAME, true);
                }
            }
        });
    }

    /**
     * Enable WAL.
     *
     * @param node Node.
     * @param cacheName Cache name.
     * @return Result.
     */
    protected boolean walEnable(Ignite node, String cacheName) {
        return node.cluster().walEnable(cacheName);
    }

    /**
     * Disable WAL.
     *
     * @param node Node.
     * @param cacheName Cache name.
     * @return Result.
     */
    protected boolean walDisable(Ignite node, String cacheName) {
        return node.cluster().walDisable(cacheName);
    }

    /**
     * Enable WAL.
     *
     * @param node Node.
     * @param cacheName Cache name.
     * @param expRes Expected result.
     */
    private void assertWalEnable(Ignite node, String cacheName, boolean expRes) {
        boolean res = node.cluster().walEnable(cacheName);

        if (!jdbc)
            assertEquals(expRes, res);
    }

    /**
     * Disable WAL.
     *
     * @param node Node.
     * @param cacheName Cache name.
     * @param expRes Expected result.
     */
    private void assertWalDisable(Ignite node, String cacheName, boolean expRes) {
        boolean res = node.cluster().walDisable(cacheName);

        if (!jdbc)
            assertEquals(expRes, res);
    }

    /**
     * Assert WAL state on all nodes.
     *
     * @param cacheName Cache name.
     * @param expState Expected state.
     * @throws IgniteCheckedException If fialed.
     */
    private void assertForAllNodes(String cacheName, boolean expState) throws IgniteCheckedException {
        info("");

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
     * Execute certain logic for all nodes.
     *
     * @param task Task.
     * @throws Exception If failed.
     */
    private void forAllNodes(IgniteInClosureX<Ignite> task) throws Exception {
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
                        node0.destroyCache(cacheName);
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
    private IgniteConfiguration config(String name, boolean cli, boolean filter) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName(name);
        cfg.setClientMode(cli);
        cfg.setLocalHost("127.0.0.1");

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        DataRegionConfiguration regionCfg = new DataRegionConfiguration().setPersistenceEnabled(true);

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
    private CacheConfiguration cacheConfig(CacheMode mode) {
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
    private CacheConfiguration cacheConfig(String name, CacheMode mode, CacheAtomicityMode atomicityMode) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(name);
        ccfg.setCacheMode(mode);
        ccfg.setAtomicityMode(atomicityMode);

        ccfg.setNodeFilter(FILTER);

        return ccfg;
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void deleteWorkFiles() throws IgniteCheckedException {
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
    }

    /**
     * Cache node filter.
     */
    private static class CacheNodeFilter implements IgnitePredicate<ClusterNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            Object filterAttr = node.attribute(FILTER_ATTR);

            return filterAttr == null;
        }
    }
}
