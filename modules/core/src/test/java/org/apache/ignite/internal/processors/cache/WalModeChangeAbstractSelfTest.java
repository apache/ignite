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

import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.lang.IgniteInClosureX;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Test dynamic WAL mode change.
 */

public abstract class WalModeChangeAbstractSelfTest extends WalModeChangeCommonAbstractSelfTest {
    /** Whether coordinator node should be filtered out. */
    private final boolean filterOnCrd;

    /**
     * Constructor.
     *
     * @param filterOnCrd Whether coordinator node should be filtered out.
     * @param jdbc Whether this is JDBC test.
     */
    protected WalModeChangeAbstractSelfTest(boolean filterOnCrd, boolean jdbc) {
        super(jdbc);

        this.filterOnCrd = filterOnCrd;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();

        startGrid(config(SRV_1, false, filterOnCrd));
        startGrid(config(SRV_2, false, false));
        startGrid(config(SRV_3, false, !filterOnCrd));

        Ignite cli = startGrid(config(CLI, true, false));

        cli.cluster().active(true);
    }

    /**
     * Negative case: cache name is null.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNullCacheName() throws Exception {
        forAllNodes(new IgniteInClosureX<Ignite>() {
            @Override public void applyx(Ignite ignite) throws IgniteCheckedException {
                assertThrows(new Callable<Void>() {
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
    @Test
    public void testNoCache() throws Exception {
        forAllNodes(new IgniteInClosureX<Ignite>() {
            @Override public void applyx(Ignite ignite) throws IgniteCheckedException {
                assertThrows(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        walEnable(ignite, CACHE_NAME);

                        return null;
                    }
                }, IgniteException.class, (jdbc ? "Table doesn't exist" : "Cache doesn't exist"));
            }
        });
    }


    /**
     * Negative case: trying to disable WAL for cache in a shared cache group.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSharedCacheGroup() throws Exception {
        forAllNodes(new IgniteInClosureX<Ignite>() {
            @Override public void applyx(Ignite ignite) throws IgniteCheckedException {
                createCache(ignite, cacheConfig(CACHE_NAME, PARTITIONED, TRANSACTIONAL).setGroupName("grp"));
                createCache(ignite, cacheConfig(CACHE_NAME_2, PARTITIONED, TRANSACTIONAL).setGroupName("grp"));

                assertThrows(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        walDisable(ignite, CACHE_NAME);

                        return null;
                    }
                }, IgniteException.class, "Cannot change WAL mode because not all cache names belonging to the " +
                    "group are provided");

                assertThrows(new Callable<Void>() {
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
    @Test
    public void testPersistenceDisabled() throws Exception {
        forAllNodes(new IgniteInClosureX<Ignite>() {
            @Override public void applyx(Ignite ignite) throws IgniteCheckedException {
                U.sleep(500);

                CacheConfiguration ccfg = cacheConfig(CACHE_NAME_3, PARTITIONED, TRANSACTIONAL)
                    .setDataRegionName(REGION_VOLATILE);

                createCache(ignite, ccfg);

                assertThrows(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        walDisable(ignite, CACHE_NAME_3);

                        return null;
                    }
                }, IgniteException.class, "Cannot change WAL mode because persistence is not enabled for cache(s)");

                assertThrows(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        walEnable(ignite, CACHE_NAME_3);

                        return null;
                    }
                }, IgniteException.class, "Cannot change WAL mode because persistence is not enabled for cache(s)");

                assert !ignite.cluster().isWalEnabled(CACHE_NAME_3);
            }
        });
    }

    /**
     * Negative case: LOCAL cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLocalCache() throws Exception {
        if (jdbc)
            // Doesn't make sense for JDBC.
            return;

        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.LOCAL_CACHE);

        forAllNodes(new IgniteInClosureX<Ignite>() {
            @Override public void applyx(Ignite ignite) throws IgniteCheckedException {
                createCache(ignite, cacheConfig(LOCAL).setDataRegionName(REGION_VOLATILE));

                assertThrows(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        walDisable(ignite, CACHE_NAME);

                        return null;
                    }
                }, IgniteException.class, "WAL mode cannot be changed for LOCAL cache(s)");

                assertThrows(new Callable<Void>() {
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
    @Test
    public void testEnableDisablePartitionedAtomic() throws Exception {
        checkEnableDisable(PARTITIONED, ATOMIC);
    }

    /**
     * Test enable/disable for PARTITIONED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testEnableDisablePartitionedTransactional() throws Exception {
        checkEnableDisable(PARTITIONED, TRANSACTIONAL);
    }

    /**
     * Test enable/disable for REPLICATED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testEnableDisableReplicatedAtomic() throws Exception {
        checkEnableDisable(REPLICATED, ATOMIC);
    }

    /**
     * Test enable/disable for REPLICATED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    @Test
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
                createCache(ignite, cacheConfig(CACHE_NAME, mode, atomicityMode));

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
     * Test {@link WalStateManager#prohibitWALDisabling(boolean)} feature.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDisablingProhibition() throws Exception {
        forAllNodes(new IgniteInClosureX<Ignite>() {
            @Override public void applyx(Ignite ig) throws IgniteCheckedException {
                assert ig instanceof IgniteEx;

                IgniteEx ignite = (IgniteEx)ig;

                createCache(ignite, cacheConfig(CACHE_NAME, PARTITIONED, TRANSACTIONAL));

                WalStateManager stateMgr = ignite.context().cache().context().walState();

                assertFalse(stateMgr.prohibitWALDisabling());

                stateMgr.prohibitWALDisabling(true);
                assertTrue(stateMgr.prohibitWALDisabling());

                try {
                    walDisable(ignite, CACHE_NAME);

                    fail();
                }
                catch (Exception e) {
                    // No-op.
                }

                stateMgr.prohibitWALDisabling(false);
                assertFalse(stateMgr.prohibitWALDisabling());

                createCache(ignite, cacheConfig(CACHE_NAME, PARTITIONED, TRANSACTIONAL));

                assertWalDisable(ignite, CACHE_NAME, true);
                assertWalEnable(ignite, CACHE_NAME, true);
            }
        });
    }
}
