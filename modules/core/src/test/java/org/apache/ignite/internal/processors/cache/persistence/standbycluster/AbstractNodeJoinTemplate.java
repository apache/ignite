/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.standbycluster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

import static org.apache.ignite.internal.util.IgniteUtils.field;

/**
 *
 */
public abstract class AbstractNodeJoinTemplate extends GridCommonAbstractTest {
    /** Cache 1. */
    protected static final String cache1 = "cache1";

    /** Cache 2. */
    protected static final String cache2 = "cache2";

    //Todo Cache with node filter.
    protected static final String cache3 = "cache3";

    protected static final String cache4 = "cache4";

    protected static final String cache5 = "cache5";

    /** Caches info. */
    public static final String CACHES_INFO = "cachesInfo";

    /** Registered caches. */
    public static final String REGISTERED_CACHES = "registeredCaches";

    /** Caches. */
    public static final String CACHES = "caches";

    /**
     * @param ig Ig.
     */
    protected static Map<String, DynamicCacheDescriptor> cacheDescriptors(IgniteEx ig) {
        return field((Object)field(ig.context().cache(), CACHES_INFO), REGISTERED_CACHES);
    }

    /**
     * @param ig Ig.
     */
    protected static Map<String, GridCacheAdapter> caches(IgniteEx ig){
        return field(ig.context().cache(), CACHES);
    }

    /**
     *
     */
    public abstract JoinNodeTestPlanBuilder withOutConfigurationTemplate() throws Exception;

    /**
     *
     */
    public abstract JoinNodeTestPlanBuilder staticCacheConfigurationOnJoinTemplate() throws Exception;

    /**
     *
     */
    public abstract JoinNodeTestPlanBuilder staticCacheConfigurationInClusterTemplate() throws Exception;

    /**
     *
     */
    public abstract JoinNodeTestPlanBuilder staticCacheConfigurationSameOnBothTemplate() throws Exception;

    /**
     *
     */
    public abstract JoinNodeTestPlanBuilder staticCacheConfigurationDifferentOnBothTemplate() throws Exception;

    // Client node join.

    public abstract JoinNodeTestPlanBuilder joinClientWithOutConfigurationTemplate() throws Exception;

    public abstract JoinNodeTestPlanBuilder joinClientStaticCacheConfigurationOnJoinTemplate() throws Exception;

    public abstract JoinNodeTestPlanBuilder joinClientStaticCacheConfigurationInClusterTemplate() throws Exception;

    public abstract JoinNodeTestPlanBuilder joinClientStaticCacheConfigurationSameOnBothTemplate() throws Exception;

    public abstract JoinNodeTestPlanBuilder joinClientStaticCacheConfigurationDifferentOnBothTemplate() throws Exception;

    /**
     *
     */
    public abstract void testJoinWithOutConfiguration() throws Exception;

    /**
     *
     */
    public abstract void testStaticCacheConfigurationOnJoin() throws Exception;

    /**
     *
     */
    public abstract void testStaticCacheConfigurationInCluster() throws Exception;

    /**
     *
     */
    public abstract void testStaticCacheConfigurationSameOnBoth() throws Exception;

    /**
     *
     */
    public abstract void testStaticCacheConfigurationDifferentOnBoth() throws Exception;

    /**
     *
     */
    public abstract void testJoinClientWithOutConfiguration() throws Exception;

    /**
     *
     */
    public abstract void testJoinClientStaticCacheConfigurationOnJoin() throws Exception;

    /**
     *
     */
    public abstract void testJoinClientStaticCacheConfigurationInCluster() throws Exception;

    /**
     *
     */
    public abstract void testJoinClientStaticCacheConfigurationSameOnBoth() throws Exception;

    /**
     *
     */
    public abstract void testJoinClientStaticCacheConfigurationDifferentOnBoth() throws Exception;

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false));
    }

    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false));
    }

    /**
     * @param idx Index.
     */
    protected String name(int idx) {
        return getTestIgniteInstanceName(idx);
    }

    /**
     * @param name Name.
     */
    protected IgniteConfiguration cfg(String name) throws Exception {
        try {
            return getConfiguration(name);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     */
    protected JoinNodeTestPlanBuilder builder() {
        return JoinNodeTestPlanBuilder.builder();
    }

    /**
     * @param cfgs Cfgs.
     */
    protected static <T> T[] buildConfiguration(T... cfgs) {
        return cfgs;
    }

    /**
     *
     */
    protected CacheConfiguration atomicCfg() {
        return new CacheConfiguration(cache1)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC);
    }

    /**
     *
     */
    protected CacheConfiguration transactionCfg() {
        return new CacheConfiguration(cache2)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
    }

    /**
     *
     */
    protected CacheConfiguration[] allCacheConfigurations() {
        return buildConfiguration(atomicCfg(), transactionCfg());
    }

    /** Set client. */
    protected final IgniteClosure<IgniteConfiguration, IgniteConfiguration> setClient =
        new IgniteClosure<IgniteConfiguration, IgniteConfiguration>() {
            @Override public IgniteConfiguration apply(IgniteConfiguration cfg) {
                return cfg.setClientMode(true);
            }
        };

    /** Ip finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        return super.getConfiguration(name)
            .setDiscoverySpi(
                new TcpDiscoverySpi()
                    .setIpFinder(ipFinder)
            );
    }

    /** {@inheritDoc} */
    protected IgniteConfiguration persistentCfg(IgniteConfiguration cfg) throws Exception {
        cfg.setPersistentStoreConfiguration(new PersistentStoreConfiguration());

        return cfg;
    }

    /**
     *
     */
    public static class JoinNodeTestPlanBuilder extends GridCommonAbstractTest {
        /** String plan builder. */
        private final StringBuilder strPlanBuilder = new StringBuilder().append("**** Execution plan ****\n");

        /** Nodes. */
        protected List<String> nodes = new ArrayList<>(4);

        /** Cluster config. */
        private IgniteConfiguration[] clusterCfg;

        /** Node config. */
        private IgniteConfiguration nodeCfg;

        /** State default. */
        private static final Boolean stateDefault = new Boolean(true);

        /** State. */
        private Boolean state = stateDefault;

        /** Noop. */
        private static final Runnable Noop = new Runnable() {
            @Override public void run() {
            }
        };

        /** After cluster started. */
        private Runnable afterClusterStarted = Noop;

        /** After node join. */
        private Runnable afterNodeJoin = Noop;

        /** After activate. */
        private Runnable afterActivate = Noop;

        /** After de activate. */
        private Runnable afterDeActivate = Noop;

        private IgniteCallable<List<CacheConfiguration>> dynamicCacheStart =
            new IgniteCallable<List<CacheConfiguration>>() {
                @Override public List<CacheConfiguration> call() throws Exception {
                    return Arrays.asList(new CacheConfiguration(cache4), new CacheConfiguration(cache5));
                }
            };

        private IgniteCallable<List<String>> dynamicCacheStop =
            new IgniteCallable<List<String>>() {
                @Override public List<String> call() throws Exception {
                    return Arrays.asList(cache4, cache5);
                }
            };

        private Runnable afterDynamicCacheStarted = Noop;

        private Runnable afterDynamicCacheStopped = Noop;

        /** End. */
        private Runnable end = Noop;

        /**
         *
         */
        public JoinNodeTestPlanBuilder clusterConfiguration(IgniteConfiguration... cfgs) throws Exception {
            clusterCfg = cfgs;

            strPlanBuilder.append("Start cluster:\n");

            for (IgniteConfiguration cfg : cfgs) {
                strPlanBuilder.append("node: ")
                    .append(cfg.getIgniteInstanceName())
                    .append(" activeOnStart - ")
                    .append(cfg.isActiveOnStart())
                    .append("\n");

                CacheConfiguration[] ccfgs = cfg.getCacheConfiguration();

                if (ccfgs != null) {
                    for (CacheConfiguration ccfg : ccfgs)
                        strPlanBuilder.append("  cache - ")
                            .append(ccfg.getName())
                            .append("\n");
                }
            }

            return this;
        }

        /**
         *
         */
        public JoinNodeTestPlanBuilder nodeConfiguration(IgniteConfiguration cfg) {
            nodeCfg = cfg;

            strPlanBuilder.append("Join node:\n")
                .append(cfg.getIgniteInstanceName())
                .append(cfg.isClientMode() != null && cfg.isClientMode() ? " (client)" : "")
                .append(" activeOnStart - ")
                .append(cfg.isActiveOnStart())
                .append("\n");

            CacheConfiguration[] ccfgs = cfg.getCacheConfiguration();

            if (ccfgs != null)
                for (CacheConfiguration ccfg : ccfgs)
                    strPlanBuilder.append("  cache - ").append(ccfg.getName()).append("\n");

            return this;
        }

        /**
         * @param func Func.
         */
        public JoinNodeTestPlanBuilder nodeConfiguration(
            IgniteClosure<IgniteConfiguration, IgniteConfiguration> func
        ) {

            nodeCfg = func.apply(nodeCfg);

            return this;
        }

        /**
         *
         */
        public JoinNodeTestPlanBuilder afterClusterStarted(Runnable r) {
            strPlanBuilder.append("Check after cluster start\n");

            afterClusterStarted = r;

            return this;
        }

        /**
         *
         */
        public JoinNodeTestPlanBuilder afterNodeJoin(Runnable r) {
            strPlanBuilder.append("Check after node join")
                .append("\n");

            afterNodeJoin = r;

            return this;
        }

        /**
         *
         */
        public JoinNodeTestPlanBuilder stateAfterJoin(boolean state) {
            strPlanBuilder.append("Check state on all nodes after join, must be ")
                .append(state ? "<<active>>" : "<<inactive>>")
                .append(" \n");

            this.state = state;

            return this;
        }

        /**
         *
         */
        public JoinNodeTestPlanBuilder afterActivate(Runnable r) {
            strPlanBuilder.append("Check after activate")
                .append("\n");

            afterActivate = r;

            return this;
        }

        /**
         *
         */
        public JoinNodeTestPlanBuilder afterDeActivate(Runnable r) {
            strPlanBuilder.append("Check after deActivate")
                .append("\n");

            afterDeActivate = r;

            return this;
        }

        public JoinNodeTestPlanBuilder dynamicCacheStart(IgniteCallable<List<CacheConfiguration>> caches){
            strPlanBuilder.append("Dynamic caches start")
                .append("\n");

            dynamicCacheStart = caches;

            return this;
        }

        public JoinNodeTestPlanBuilder afterDynamicCacheStarted(Runnable r){
            strPlanBuilder.append("Check after dynamic caches start")
                .append("\n");

            afterDynamicCacheStarted = r;

            return this;
        }

        public JoinNodeTestPlanBuilder dynamicCacheStop(IgniteCallable<List<String>> caches){
            strPlanBuilder.append("Dynamic caches stop")
                .append("\n");

            dynamicCacheStop = caches;

            return this;
        }

        public JoinNodeTestPlanBuilder afterDynamicCacheStopped(Runnable r){
            strPlanBuilder.append("Check after dynamic caches stop")
                .append("\n");

            afterDynamicCacheStopped = r;

            return this;
        }

        /**
         * @param end End.
         */
        public JoinNodeTestPlanBuilder setEnd(Runnable end) {
            strPlanBuilder.append("Check before stop")
                .append("\n");

            this.end = end;

            return this;
        }

        /**
         *
         */
        public void execute() throws Exception {
            try {
                if (state == stateDefault)
                    fail("State after join must be specific. See JoinNodeTestPlanBuilder.stateAfterJoin(boolean).");

                System.out.println(strPlanBuilder.append("********************").toString());

                IgniteConfiguration[] cfgs = clusterCfg;

                System.out.println(">>> Start cluster");

                for (IgniteConfiguration cfg : cfgs) {
                    startGrid(cfg);

                    nodes.add(cfg.getIgniteInstanceName());
                }

                System.out.println(">>> Check after cluster started");

                afterClusterStarted.run();

                System.out.println(">>> Start new node");

                startGrid(nodeCfg);

                nodes.add(nodeCfg.getIgniteInstanceName());

                System.out.println(">>> Check after new node join in cluster");

                afterNodeJoin.run();

                System.out.println(">>> Check cluster state on all nodes");

                IgniteEx crd = grid(nodes.get(0));

                for (IgniteEx ig : grids())
                    assertEquals((boolean)state, ig.active());

                if (!state) {
                    System.out.println(">>> Activate cluster");

                    crd.active(true);

                    System.out.println(">>> Check after cluster activated");

                    afterActivate.run();
                }
                else {
                    System.out.println(">>> DeActivate cluster");

                    crd.active(false);

                    System.out.println(">>> Check after cluster deActivated");

                    afterDeActivate.run();

                    System.out.println(">>> Activate cluster");

                    crd.active(true);
                }

                AffinityTopologyVersion next0Ver = nextMinorVersion(crd);

                crd.createCaches(dynamicCacheStart.call());

                awaitTopologyVersion(next0Ver);

                afterDynamicCacheStarted.run();

                onAllNode(new CI1<IgniteEx>() {
                    @Override public void apply(IgniteEx ig) {
                        if (ig.context().discovery().localNode().isClient())
                            return;

                        Assert.assertNotNull(ig.context().cache().cache(cache4));
                        Assert.assertNotNull(ig.context().cache().cache(cache5));

                    }
                });

                AffinityTopologyVersion next1Ver = nextMinorVersion(crd);

                crd.destroyCaches(dynamicCacheStop.call());

                afterDynamicCacheStopped.run();

                awaitTopologyVersion(next1Ver);

                onAllNode(new CI1<IgniteEx>() {
                    @Override public void apply(IgniteEx ig) {
                        if (ig.context().discovery().localNode().isClient())
                            return;

                        Assert.assertNull(ig.context().cache().cache(cache4));
                        Assert.assertNull(ig.context().cache().cache(cache5));

                    }
                });

                System.out.println(">>> Finish check");

                end.run();
            }
            finally {
                stopAllGrids();
            }
        }

        private AffinityTopologyVersion nextMinorVersion(IgniteEx ig){
            AffinityTopologyVersion cur = ig.context().discovery().topologyVersionEx();

           return new AffinityTopologyVersion(cur.topologyVersion(), cur.minorTopologyVersion() + 1);
        }

        private void awaitTopologyVersion(final AffinityTopologyVersion ver){
            onAllNode(new CI1<IgniteEx>() {
                @Override public void apply(IgniteEx ig) {
                    while (true) {
                        AffinityTopologyVersion locTopVer = ig.context().cache().context()
                            .exchange().readyAffinityVersion();

                        if (locTopVer.compareTo(ver) < 0){
                            System.out.println("Top ready " + locTopVer + " on " + ig.localNode().id());

                            try {
                                Thread.sleep(100);
                            }
                            catch (InterruptedException e) {
                                break;
                            }
                        }
                        else
                            break;
                    }
                }
            }).run();

        }

        /**
         *
         */
        protected List<IgniteEx> grids() {
            List<IgniteEx> res = new ArrayList<>();

            for (String name : nodes)
                res.add(grid(name));

            return res;
        }

        /**
         *
         */
        public static JoinNodeTestPlanBuilder builder() {
            return new JoinNodeTestPlanBuilder();
        }

        /**
         *
         */
        public Runnable checkCacheOnlySystem() {
            return onAllNode(new IgniteInClosure<IgniteEx>() {
                @Override public void apply(IgniteEx ig) {
                    Map<String, DynamicCacheDescriptor> desc = cacheDescriptors(ig);

                    Assert.assertEquals(2, desc.size());

                    Assert.assertNull(ig.context().cache().cache(cache1));
                    Assert.assertNull(ig.context().cache().cache(cache2));

                    Map<String, GridCacheAdapter> caches = caches(ig);

                    Assert.assertEquals(2, caches.size());
                }
            });
        }

        /**
         *
         */
        public Runnable checkCacheEmpty() {
            return onAllNode(new IgniteInClosure<IgniteEx>() {
                @Override public void apply(IgniteEx ig) {
                    Map<String, DynamicCacheDescriptor> desc = cacheDescriptors(ig);

                    Assert.assertTrue(desc.isEmpty());

                    Assert.assertNull(ig.context().cache().cache(cache1));
                    Assert.assertNull(ig.context().cache().cache(cache2));

                    Map<String, GridCacheAdapter> caches = caches(ig);

                    Assert.assertEquals(0, caches.size());
                }
            });
        }

        /**
         *
         */
        public Runnable checkCacheNotEmpty() {
            return onAllNode(new IgniteInClosure<IgniteEx>() {
                @Override public void apply(IgniteEx ig) {
                    Map<String, DynamicCacheDescriptor> desc = cacheDescriptors(ig);

                    Assert.assertEquals(4, desc.size());

                    Assert.assertNotNull(ig.context().cache().cache(cache1));
                    Assert.assertNotNull(ig.context().cache().cache(cache2));

                    Map<String, GridCacheAdapter> caches = caches(ig);

                    Assert.assertEquals(4, caches.size());
                }
            });
        }

        /**
         * @param cls Closure.
         */
        private Runnable onAllNode(final IgniteInClosure<IgniteEx> cls) {
            return new Runnable() {
                @Override public void run() {
                    for (IgniteEx ig : grids()) {
                        try {
                            cls.apply(ig);
                        }
                        catch (AssertionError e) {
                            System.out.println("Assertion on " + ig.name());

                            throw e;
                        }
                    }
                }
            };
        }
    }
}
