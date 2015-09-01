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

package org.apache.ignite.internal.processors.cache;

import java.io.Externalizable;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheInterceptorAdapter;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityNodeIdHashResolver;
import org.apache.ignite.cache.affinity.fair.FairAffinityFunction;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.eviction.EvictionFilter;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.cache.eviction.random.RandomEvictionPolicy;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheRebalanceMode.NONE;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.DeploymentMode.CONTINUOUS;
import static org.apache.ignite.configuration.DeploymentMode.SHARED;

/**
 *
 */
@SuppressWarnings("unchecked")
public class GridCacheConfigurationConsistencySelfTest extends GridCommonAbstractTest {
    /** */
    private boolean cacheEnabled;

    /** */
    private String cacheName;

    /** */
    private CacheMode cacheMode = REPLICATED;

    /** */
    private DeploymentMode depMode = SHARED;

    /** */
    private C1<CacheConfiguration, Void> initCache;

    /** */
    private boolean useStrLog;

    /** */
    private GridStringLogger strLog;

    /** */
    private AffinityFunction aff;

    /** */
    private int backups;

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (useStrLog) {
            strLog = new GridStringLogger(false, cfg.getGridLogger());
            cfg.setGridLogger(strLog);
        }

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setDeploymentMode(depMode);

        if (cacheEnabled) {
            CacheConfiguration cacheCfg = defaultCacheConfiguration();

            cacheCfg.setName(cacheName);
            cacheCfg.setCacheMode(cacheMode);
            cacheCfg.setAffinity(aff);
            cacheCfg.setBackups(backups);
            cacheCfg.setAtomicityMode(TRANSACTIONAL);

            if (initCache != null)
                initCache.apply(cacheCfg);

            cfg.setCacheConfiguration(cacheCfg);
        }
        else
            cfg.setCacheConfiguration();

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheUtilsCheckAttributeMismatch() throws Exception {
        Ignite ignite = startGrid(1);

        final ClusterNode node = ignite.cluster().localNode();

        final GridStringLogger strLog = new GridStringLogger(false, log);

        CU.checkAttributeMismatch(strLog, "cache", node.id(), "cacheMode", "Cache mode", LOCAL, PARTITIONED, false);

        assertTrue("No expected message in log: " + strLog.toString(),
            strLog.toString().contains("Cache mode mismatch"));

        strLog.reset();

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            /** {@inheritDoc} */
            @Override public Void call() throws Exception {
                CU.checkAttributeMismatch(strLog, "cache", node.id(), "cacheMode", "Cache mode", LOCAL, PARTITIONED, true);
                return null;
            }
        }, IgniteCheckedException.class, "Cache mode mismatch");

        final CacheConfiguration cfg1 = defaultCacheConfiguration();

        cfg1.setCacheMode(LOCAL);

        final CacheConfiguration cfg2 = defaultCacheConfiguration();

        cfg2.setCacheMode(PARTITIONED);

        CU.checkAttributeMismatch(strLog, cfg1, cfg2, node.id(), new T2<>("cacheMode", "Cache mode"), false);

        assertTrue("No expected message in log: " + strLog.toString(),
            strLog.toString().contains("Cache mode mismatch"));

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            /** {@inheritDoc} */
            @Override public Void call() throws Exception {
                CU.checkAttributeMismatch(strLog, cfg1, cfg2, node.id(), new T2<>("cacheMode", "Cache mode"), true);
                return null;
            }
        }, IgniteCheckedException.class, "Cache mode mismatch");
    }

    /**
     * @throws Exception If failed.
     */
    public void testNullCacheMode() throws Exception {
        // Grid with null cache mode.
        // This is a legal case. The default cache mode should be used.
        cacheEnabled = true;
        cacheName = "myCache";
        cacheMode = null;
        depMode = SHARED;

        assert startGrid(1).cache("myCache").getConfiguration(CacheConfiguration.class).getCacheMode() == CacheConfiguration.DFLT_CACHE_MODE;
    }

    /**
     * @throws Exception If failed.
     */
    public void testWithCacheAndWithoutCache() throws Exception {
        // 1st grid without cache.
        cacheEnabled = false;
        depMode = SHARED;

        startGrid(2);

        // 2nd grid with replicated cache on board.
        cacheEnabled = true;
        cacheName = "myCache";
        cacheMode = REPLICATED;
        depMode = SHARED;

        startGrid(1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSameCacheDifferentModes() throws Exception {
        // 1st grid with replicated cache.
        cacheEnabled = true;
        cacheName = "myCache";
        cacheMode = REPLICATED;
        depMode = SHARED;

        startGrid(1);

        // 2nd grid with partitioned cache.
        cacheEnabled = true;
        cacheName = "myCache";
        cacheMode = PARTITIONED;
        depMode = SHARED;

        try {
            startGrid(2);

            fail();
        }
        catch (IgniteCheckedException e) {
            info("Caught expected exception: " + e);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentCacheDifferentModes() throws Exception {
        // 1st grid with local cache.
        cacheEnabled = true;
        cacheName = "local";
        cacheMode = LOCAL;
        depMode = SHARED;

        startGrid(1);

        // 2nd grid with replicated cache.
        cacheEnabled = true;
        cacheName = "replicated";
        cacheMode = REPLICATED;
        depMode = SHARED;

        startGrid(2);

        // 3d grid with partitioned cache.
        cacheEnabled = true;
        cacheName = "partitioned";
        cacheMode = PARTITIONED;
        depMode = SHARED;

        startGrid(3);

        // 4th grid with null cache mode (legal case, it should turn to REPLICATED mode).
        cacheEnabled = true;
        cacheName = "partitioned";
        cacheMode = null;
        depMode = SHARED;

        startGrid(4);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentDeploymentModes() throws Exception {
        // 1st grid with SHARED mode.
        cacheEnabled = true;
        cacheName = "partitioned";
        cacheMode = PARTITIONED;
        depMode = SHARED;

        startGrid(1);

        // 2nd grid with CONTINUOUS mode.
        cacheEnabled = true;
        cacheName = "partitioned";
        cacheMode = PARTITIONED;
        depMode = CONTINUOUS;

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return startGrid(2);
            }
        }, IgniteCheckedException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentAffinities() throws Exception {
        cacheMode = PARTITIONED;

        checkSecondGridStartFails(
            new C1<CacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(CacheConfiguration cfg) {
                    cfg.setAffinity(new TestRendezvousAffinityFunction());
                    return null;
                }
            },
            new C1<CacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(CacheConfiguration cfg) {
                    cfg.setAffinity(new RendezvousAffinityFunction());
                    return null;
                }
            }
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentPreloadModes() throws Exception {
        checkSecondGridStartFails(
            new C1<CacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(CacheConfiguration cfg) {
                    cfg.setRebalanceMode(NONE);
                    return null;
                }
            },
            new C1<CacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(CacheConfiguration cfg) {
                    cfg.setRebalanceMode(ASYNC);
                    return null;
                }
            }
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentEvictionEnabled() throws Exception {
        checkSecondGridStartFails(
            new C1<CacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(CacheConfiguration cfg) {
                    cfg.setEvictionPolicy(new FifoEvictionPolicy());
                    return null;
                }
            },
            new C1<CacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(CacheConfiguration cfg) {
                    return null;
                }
            }
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentEvictionPolicies() throws Exception {
        checkSecondGridStartFails(
            new C1<CacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(CacheConfiguration cfg) {
                    cfg.setEvictionPolicy(new RandomEvictionPolicy());
                    return null;
                }
            },
            new C1<CacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(CacheConfiguration cfg) {
                    cfg.setEvictionPolicy(new FifoEvictionPolicy());
                    return null;
                }
            }
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentEvictionFilters() throws Exception {
        checkSecondGridStartFails(
            new C1<CacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(CacheConfiguration cfg) {
                    cfg.setEvictionFilter(new FirstCacheEvictionFilter());
                    return null;
                }
            },
            new C1<CacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(CacheConfiguration cfg) {
                    cfg.setEvictionFilter(new SecondCacheEvictionFilter());
                    return null;
                }
            }
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentAffinityMappers() throws Exception {
        checkSecondGridStartFails(
            new C1<CacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(CacheConfiguration cfg) {
                    cfg.setAffinityMapper(new TestCacheDefaultAffinityKeyMapper());
                    return null;
                }
            },
            new C1<CacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(CacheConfiguration cfg) {
                    cfg.setAffinityMapper(new GridCacheDefaultAffinityKeyMapper());
                    return null;
                }
            }
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentEvictSynchronized() throws Exception {
        cacheMode = PARTITIONED;

        checkSecondGridStartFails(
            new C1<CacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(CacheConfiguration cfg) {
                    cfg.setEvictSynchronized(true);

                    FifoEvictionPolicy plc = new FifoEvictionPolicy();

                    plc.setMaxSize(100);

                    cfg.setEvictionPolicy(plc);
                    return null;
                }
            },
            new C1<CacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(CacheConfiguration cfg) {
                    cfg.setEvictSynchronized(false);

                    FifoEvictionPolicy plc = new FifoEvictionPolicy();

                    plc.setMaxSize(100);

                    cfg.setEvictionPolicy(plc);
                    return null;
                }
            }
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentAtomicity() throws Exception {
        cacheMode = PARTITIONED;

        checkSecondGridStartFails(
            new C1<CacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(CacheConfiguration cfg) {
                    cfg.setNearConfiguration(null);
                    cfg.setAtomicityMode(ATOMIC);
                    return null;
                }
            },
            new C1<CacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(CacheConfiguration cfg) {
                    cfg.setNearConfiguration(null);
                    cfg.setAtomicityMode(TRANSACTIONAL);
                    return null;
                }
            }
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentSynchronization() throws Exception {
        cacheMode = PARTITIONED;

        checkSecondGridStartFails(
            new C1<CacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(CacheConfiguration cfg) {
                    cfg.setWriteSynchronizationMode(FULL_SYNC);
                    return null;
                }
            },
            new C1<CacheConfiguration, Void>() {
                /** {@inheritDoc} */
                @Override public Void apply(CacheConfiguration cfg) {
                    cfg.setWriteSynchronizationMode(FULL_ASYNC);
                    return null;
                }
            }
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityFunctionConsistency() throws Exception {
        cacheEnabled = true;
        cacheMode = PARTITIONED;

        backups = 1;

        aff = new RendezvousAffinityFunction(false, 100);

        startGrid(1);

        // 2nd grid with another affinity.
        // Check include neighbors.
        aff = new RendezvousAffinityFunction(true, 100);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return startGrid(2);
            }
        }, IgniteCheckedException.class, "Affinity include neighbors mismatch");

        backups = 2;

        // Check backups.
        aff = new RendezvousAffinityFunction(false, 100);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return startGrid(2);
            }
        }, IgniteCheckedException.class, "Affinity key backups mismatch");

        backups = 1;

        // Partitions count.
        aff = new RendezvousAffinityFunction(false, 1000);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return startGrid(2);
            }
        }, IgniteCheckedException.class, "Affinity partitions count mismatch");

        // Different hash ID resolver.
        RendezvousAffinityFunction aff0 = new RendezvousAffinityFunction(false, 100);

        aff0.setHashIdResolver(new AffinityNodeIdHashResolver());

        aff = aff0;

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return startGrid(2);
            }
        }, IgniteCheckedException.class, "Partitioned cache affinity hash ID resolver class mismatch");
    }

    /**
     * @throws Exception If failed.
     */
    public void testAttributesWarnings() throws Exception {
        cacheEnabled = true;

        initCache = new C1<CacheConfiguration, Void>() {
            /** {@inheritDoc} */
            @Override public Void apply(CacheConfiguration cfg) {
                cfg.setDefaultLockTimeout(1000);

                return null;
            }
        };

        startGrid(1);

        useStrLog = true;

        initCache = new C1<CacheConfiguration, Void>() {
            /** {@inheritDoc} */
            @Override public Void apply(CacheConfiguration cfg) {
                cfg.setDefaultLockTimeout(2 * 1000);

                return null;
            }
        };

        startGrid(2);

        String log = strLog.toString();

        assertTrue(log.contains("Default lock timeout"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionedOnlyAttributesIgnoredForReplicated() throws Exception {
        cacheEnabled = true;

        cacheMode = REPLICATED;

        initCache = new C1<CacheConfiguration, Void>() {
            @Override public Void apply(CacheConfiguration cfg) {
                NearCacheConfiguration nearCfg = new NearCacheConfiguration();

                nearCfg.setNearEvictionPolicy(new RandomEvictionPolicy());

                cfg.setNearConfiguration(nearCfg);

                return null;
            }
        };

        startGrid(1);

        initCache = new C1<CacheConfiguration, Void>() {
            @Override public Void apply(CacheConfiguration cfg) {
                NearCacheConfiguration nearCfg = new NearCacheConfiguration();

                nearCfg.setNearEvictionPolicy(new FifoEvictionPolicy());

                cfg.setNearConfiguration(nearCfg);

                return null;
            }
        };

        startGrid(2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIgnoreMismatchForLocalCaches() throws Exception {
        cacheEnabled = true;

        cacheMode = LOCAL;

        initCache = new C1<CacheConfiguration, Void>() {
            /** {@inheritDoc} */
            @SuppressWarnings("unchecked")
            @Override public Void apply(CacheConfiguration cfg) {
                cfg.setAffinity(new TestRendezvousAffinityFunction());

                cfg.setEvictionPolicy(new FifoEvictionPolicy());

                cfg.setCacheStoreFactory(new IgniteCacheAbstractTest.TestStoreFactory());
                cfg.setReadThrough(true);
                cfg.setWriteThrough(true);
                cfg.setLoadPreviousValue(true);

                return null;
            }
        };

        startGrid(1);

        initCache = new C1<CacheConfiguration, Void>() {
            /** {@inheritDoc} */
            @Override public Void apply(CacheConfiguration cfg) {
                cfg.setAffinity(new RendezvousAffinityFunction());

                cfg.setEvictionPolicy(new LruEvictionPolicy());

                cfg.setCacheStoreFactory(null);

                return null;
            }
        };

        startGrid(2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStoreCheckAtomic() throws Exception {
        cacheEnabled = true;

        cacheMode = PARTITIONED;

        initCache = new C1<CacheConfiguration, Void>() {
            @SuppressWarnings("unchecked")
            @Override public Void apply(CacheConfiguration cc) {
                cc.setAtomicityMode(ATOMIC);

                cc.setNearConfiguration(null);

                cc.setCacheStoreFactory(new IgniteCacheAbstractTest.TestStoreFactory());
                cc.setReadThrough(true);
                cc.setWriteThrough(true);
                cc.setLoadPreviousValue(true);

                return null;
            }
        };

        startGrid(1);

        initCache = new C1<CacheConfiguration, Void>() {
            @Override public Void apply(CacheConfiguration cc) {
                cc.setAtomicityMode(ATOMIC);
                cc.setNearConfiguration(null);
                cc.setCacheStoreFactory(null);

                return null;
            }
        };

        GridTestUtils.assertThrows(log, new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                startGrid(2);

                return null;
            }
        }, IgniteCheckedException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStoreCheckTransactional() throws Exception {
        cacheEnabled = true;

        cacheMode = PARTITIONED;

        initCache = new C1<CacheConfiguration, Void>() {
            @SuppressWarnings("unchecked")
            @Override public Void apply(CacheConfiguration cc) {
                cc.setAtomicityMode(TRANSACTIONAL);

                cc.setNearConfiguration(null);

                cc.setCacheStoreFactory(new IgniteCacheAbstractTest.TestStoreFactory());
                cc.setReadThrough(true);
                cc.setWriteThrough(true);
                cc.setLoadPreviousValue(true);

                return null;
            }
        };

        startGrid(1);

        initCache = new C1<CacheConfiguration, Void>() {
            @Override public Void apply(CacheConfiguration cc) {
                cc.setAtomicityMode(TRANSACTIONAL);

                cc.setNearConfiguration(null);

                cc.setCacheStoreFactory(null);

                return null;
            }
        };

        GridTestUtils.assertThrows(log, new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                startGrid(2);

                return null;
            }
        }, IgniteCheckedException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityForReplicatedCache() throws Exception {
        cacheEnabled = true;

        aff = new FairAffinityFunction(); // Check cannot use FairAffinityFunction.

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return startGrid(1);
            }
        }, IgniteCheckedException.class, null);

        aff = new RendezvousAffinityFunction(true); // Check cannot set 'excludeNeighbors' flag.
        backups = Integer.MAX_VALUE;

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return startGrid(1);
            }
        }, IgniteCheckedException.class, null);

        aff = new RendezvousAffinityFunction(false, 100);

        startGrid(1);

        // Try to start node with  different number of partitions.
        aff = new RendezvousAffinityFunction(false, 200);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return startGrid(2);
            }
        }, IgniteCheckedException.class, "Affinity partitions count mismatch");
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentInterceptors() throws Exception {
        cacheMode = PARTITIONED;

        checkSecondGridStartFails(new C1<CacheConfiguration, Void>() {
                @Override public Void apply(CacheConfiguration cfg) {
                    cfg.setInterceptor(new TestCacheInterceptor());

                    return null;
                }
            }, new C1<CacheConfiguration, Void>() {
                @Override public Void apply(CacheConfiguration cfg) {
                    return null;
                }
            }
        );
    }

    /**
     * @param initCache1 Closure.
     * @param initCache2 Closure.
     * @throws Exception If failed.
     */
    private void checkSecondGridStartFails(C1<CacheConfiguration, Void> initCache1,
                                           C1<CacheConfiguration, Void> initCache2) throws Exception {
        cacheEnabled = true;

        initCache = initCache1;

        startGrid(1);

        initCache = initCache2;

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return startGrid(2);
            }
        }, IgniteCheckedException.class, null);
    }

    /** */
    private static class TestStore implements CacheStore<Object,Object> {
        /** {@inheritDoc} */
        @Nullable @Override public Object load(Object key) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Object, Object> clo, @Nullable Object... args) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Map<Object, Object> loadAll(Iterable<?> keys) throws CacheLoaderException {
            return Collections.emptyMap();
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<?, ?> entry) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void writeAll(Collection<Cache.Entry<?, ?>> entries) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void deleteAll(Collection<?> keys) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void sessionEnd(boolean commit) {
            // No-op.
        }
    }

    private static class TestRendezvousAffinityFunction extends RendezvousAffinityFunction {
        /**
         * Empty constructor required by {@link Externalizable}.
         */
        public TestRendezvousAffinityFunction() {
            // No-op.
        }
    }

    /**
     *
     */
    private static class FirstCacheEvictionFilter implements EvictionFilter<Object, Object> {
        /** {@inheritDoc} */
        @Override public boolean evictAllowed(Cache.Entry<Object, Object> entry) {
            return false;
        }
    }

    /**
     *
     */
    private static class SecondCacheEvictionFilter implements EvictionFilter<Object, Object> {
        /** {@inheritDoc} */
        @Override public boolean evictAllowed(Cache.Entry<Object, Object> entry) {
            return true;
        }
    }

    /**
     *
     */
    private static class TestCacheInterceptor extends CacheInterceptorAdapter implements Serializable {
        // No-op, just different class.
    }

    private static class TestCacheDefaultAffinityKeyMapper extends GridCacheDefaultAffinityKeyMapper {
    }
}