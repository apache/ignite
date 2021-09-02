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

package org.apache.ignite.testframework.configvariations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javassist.ClassClassPath;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtNewConstructor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.IgniteCacheConfigVariationsAbstractTest;
import org.apache.ignite.testframework.junits.IgniteConfigVariationsAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 * Configuration variations test suite builder.
 */
public class ConfigVariationsTestSuiteBuilder {
    /** */
    private static final AtomicInteger cntr = new AtomicInteger(0);

    /** */
    private static final Map<String, VariationsTestsConfig> cfgs = new ConcurrentHashMap<>();

    /** */
    private ConfigParameter<IgniteConfiguration>[][] igniteParams =
        ConfigVariations.igniteBasicSet();

    /** */
    private ConfigParameter<CacheConfiguration>[][] cacheParams;

    /** */
    private CacheStartMode cacheStartMode = CacheStartMode.DYNAMIC;

    /** */
    private boolean withClients;

    /** */
    private int gridsCnt = 3;

    /** */
    private int testedNodeCnt = 1;

    /** */
    private Class<? extends IgniteConfigVariationsAbstractTest> cls;

    /** */
    private int[] specificIgniteParam;

    /** */
    private int[] specificCacheParam;

    /** */
    private int backups = -1;

    /** */
    private IgnitePredicate<IgniteConfiguration>[] igniteCfgFilters;

    /** */
    private IgnitePredicate<CacheConfiguration>[] cacheCfgFilters;

    /** */
    private boolean skipWaitPartMapExchange;

    /**
     * @param cls Test class.
     */
    public ConfigVariationsTestSuiteBuilder(Class<? extends IgniteConfigVariationsAbstractTest> cls) {
        this.cls = cls;
    }

    /** Invoked by reflection in {@link #makeTestClass(String, VariationsTestsConfig)}. */
    @SuppressWarnings("unused")
    public static VariationsTestsConfig getCfg(String key) {
        return cfgs.get(key);
    }

    /**
     * Returns lists of test classes to execute with config variations.
     *
     * @return List of classes.
     */
    public List<Class<?>> classes() {
        assert testedNodeCnt > 0;
        assert gridsCnt > 0;

        VariationsIterator igniteCfgIter = specificIgniteParam == null ? new VariationsIterator(igniteParams)
            : new OneElementVariationsIterator(specificIgniteParam, igniteParams);

        final List<VariationsTestsConfig> cfgsToTest = new ArrayList<>();

        for (; igniteCfgIter.hasNext(); ) {
            final int[] igniteCfgVariation = igniteCfgIter.next();

            if (!passIgniteConfigFilter(igniteCfgVariation))
                continue;

            if (cacheParams == null) {
                cfgsToTest.addAll(build(igniteCfgVariation, null, true));
                continue;
            }

            VariationsIterator cacheCfgIter = specificCacheParam == null ? new VariationsIterator(cacheParams)
                : new OneElementVariationsIterator(specificCacheParam, cacheParams);

            for (; cacheCfgIter.hasNext(); ) {
                int[] cacheCfgVariation = cacheCfgIter.next();

                if (!passCacheConfigFilter(cacheCfgVariation))
                    continue;

                // Stop all grids before starting new ignite configuration.
                boolean stopNodes = !cacheCfgIter.hasNext();

                cfgsToTest.addAll(build(igniteCfgVariation, cacheCfgVariation, stopNodes));
            }
        }

        String pkg = "org.apache.ignite.testframework.configvariations.generated";

        return cfgsToTest.stream().map(cfg -> makeTestClass(pkg, cfg)).collect(Collectors.toList());
    }

    /**
     * @param variation Variation.
     * @return {@code True} if variation pass filters.
     */
    private boolean passIgniteConfigFilter(int[] variation) {
        ConfigVariationsFactory factory = new ConfigVariationsFactory(igniteParams, variation, null, null);

        IgniteConfiguration cfg = factory.getConfiguration(null, null);

        if (igniteCfgFilters != null) {
            for (IgnitePredicate<IgniteConfiguration> filter : igniteCfgFilters) {
                if (!filter.apply(cfg))
                    return false;
            }
        }

        return true;
    }

    /**
     * @param variation Variation.
     * @return {@code True} if variation pass filters.
     */
    private boolean passCacheConfigFilter(int[] variation) {
        ConfigVariationsFactory factory = new ConfigVariationsFactory(null, null, cacheParams, variation);

        CacheConfiguration cfg = factory.cacheConfiguration(null);

        if (cacheCfgFilters != null) {
            for (IgnitePredicate<CacheConfiguration> filter : cacheCfgFilters) {
                if (!filter.apply(cfg))
                    return false;
            }
        }

        return true;
    }

    /**
     * @param igniteCfgVariation Ignite Variation.
     * @param cacheCfgVariation Cache Variation.
     * @param stopNodes Stop nodes.
     * @return List of config variations.
     */
    private List<VariationsTestsConfig> build(int[] igniteCfgVariation, @Nullable int[] cacheCfgVariation,
        boolean stopNodes) {
        ConfigVariationsFactory factory = new ConfigVariationsFactory(igniteParams,
            igniteCfgVariation, cacheParams, cacheCfgVariation);

        factory.backups(backups);

        String clsNameSuffix = "[igniteCfgVariation=" + Arrays.toString(igniteCfgVariation)
            + ", cacheCfgVariation=" + Arrays.toString(cacheCfgVariation)
            + ", igniteCfg=" + factory.getIgniteConfigurationDescription()
            + ", cacheCfg=" + factory.getCacheConfigurationDescription() + "]";

        VariationsTestsConfig testCfg = new VariationsTestsConfig(factory, clsNameSuffix, stopNodes, cacheStartMode,
            gridsCnt, !skipWaitPartMapExchange);

        return testedNodeCnt > 1 ? createMultiNodeTestSuite(
            testCfg, testedNodeCnt, withClients, skipWaitPartMapExchange) : Collections.singletonList(testCfg);
    }

    /**
     * @param cfg Configuration.
     * @param testedNodeCnt Count of tested nodes.
     */
    private static List<VariationsTestsConfig> createMultiNodeTestSuite(
        VariationsTestsConfig cfg, int testedNodeCnt, boolean withClients, boolean skipWaitParMapExchange) {
        List<VariationsTestsConfig> suite = new ArrayList<>();

        if (cfg.gridCount() < testedNodeCnt)
            throw new IllegalArgumentException("Failed to initialize test suite [nodeCnt=" + testedNodeCnt
                + ", cfgGridCnt=" + cfg.gridCount() + "]");

        for (int i = 0; i < testedNodeCnt; i++) {
            boolean stopNodes = cfg.isStopNodes() && i + 1 == testedNodeCnt;
            boolean startCache = i == 0;
            boolean stopCache = i + 1 == testedNodeCnt;

            VariationsTestsConfig cfg0 = new VariationsTestsConfig(cfg.configurationFactory(), cfg.description(),
                stopNodes, startCache, stopCache, cfg.cacheStartMode(), cfg.gridCount(), i, withClients,
                !skipWaitParMapExchange);

            suite.add(cfg0);
        }

        return suite;
    }

    /**
     * @return {@code this} for chaining.
     */
    public ConfigVariationsTestSuiteBuilder withClients() {
        if (testedNodeCnt < 2)
            throw new IllegalStateException("Tested node count should be more than 1: " + testedNodeCnt);

        withClients = true;

        return this;
    }

    /**
     * All tests will be run for first {@code testedNodeCnt} grids. For {@code grid(0)}, {@code grid(1)}, ... ,
     * {@code grid(testedNodeCnt - 1)}.
     * <p>
     * Usually it needs if you want to execute tests for both client and data nodes (see {@link #withClients()}).
     * <ul>
     * <li> If test-class extends {@link IgniteConfigVariationsAbstractTest} then use {@code testedNodesCount(2)}. </li>
     * <li> if test-class extends {@link IgniteCacheConfigVariationsAbstractTest} then use {@code testedNodesCount(3)}. </li>
     *</ul>
     *
     * @param testedNodeCnt Tested node count.
     * @return {@code this} for chaining.
     */
    public ConfigVariationsTestSuiteBuilder testedNodesCount(int testedNodeCnt) {
        this.testedNodeCnt = testedNodeCnt;

        return this;
    }

    /**
     * @param cnt Count.
     * @return {@code this} for chaining.
     */
    public ConfigVariationsTestSuiteBuilder gridsCount(int cnt) {
        assert cnt > 0;

        gridsCnt = cnt;

        return this;
    }

    /**
     * @param igniteParams New ignite params.
     * @return {@code this} for chaining.
     */
    public ConfigVariationsTestSuiteBuilder igniteParams(
        ConfigParameter<IgniteConfiguration>[][] igniteParams) {
        this.igniteParams = igniteParams.clone();

        return this;
    }

    /**
     * @param cacheParams New cache params.
     * @return {@code this} for chaining.
     */
    @SuppressWarnings("unused")
    public ConfigVariationsTestSuiteBuilder cacheParams(ConfigParameter<CacheConfiguration>[][] cacheParams) {
        this.cacheParams = cacheParams.clone();

        return this;
    }

    /**
     * Sets basic cache params and basic count of backups.
     *
     * @return {@code this} for chaining.
     */
    public ConfigVariationsTestSuiteBuilder withBasicCacheParams() {
        cacheParams = ConfigVariations.cacheBasicSet();
        backups = 1;

        return this;
    }

    /**
     * @param backups Backups.
     * @return {@code this} for chaining.
     */
    public ConfigVariationsTestSuiteBuilder backups(int backups) {
        assert backups > 0 : backups;

        this.backups = backups;

        return this;
    }

    /**
     * @param singleIgniteParam Param.
     * @return {@code this} for chaining.
     */
    @SuppressWarnings("unused")
    public ConfigVariationsTestSuiteBuilder specifyIgniteParam(int... singleIgniteParam) {
        specificIgniteParam = singleIgniteParam.clone();

        return this;
    }

    /**
     * @param singleParam Param.
     * @return {@code this} for chaining.
     */
    @SuppressWarnings("unused")
    public ConfigVariationsTestSuiteBuilder specifyCacheParam(int... singleParam) {
        specificCacheParam = singleParam.clone();

        return this;
    }

    /**
     * @param filters Ignite configuration filters.
     * @return {@code this} for chaining.
     */
    public ConfigVariationsTestSuiteBuilder withIgniteConfigFilters(IgnitePredicate<IgniteConfiguration>... filters) {
        igniteCfgFilters = filters.clone();

        return this;
    }

    /** */
    public ConfigVariationsTestSuiteBuilder skipWaitPartitionMapExchange() {
        skipWaitPartMapExchange = true;

        return this;
    }

    /**
     * @param filters Ignite configuration filters.
     * @return {@code this} for chaining.
     */
    public ConfigVariationsTestSuiteBuilder withCacheConfigFilters(IgnitePredicate<CacheConfiguration>... filters) {
        cacheCfgFilters = filters.clone();

        return this;
    }

    /** */
    private static class OneElementVariationsIterator extends VariationsIterator {
        /** */
        private int[] elem;

        /** */
        private boolean hasNext = true;

        /**
         * @param elem Element.
         */
        OneElementVariationsIterator(int[] elem, Object[][] params) {
            super(params);

            this.elem = elem;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return hasNext;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("IteratorNextCanNotThrowNoSuchElementException")
        @Override public int[] next() {
            hasNext = false;

            return elem.clone();
        }
    }

    /**
     * Creates test class for given config variation.
     *
     * @param pkg Package to put class in.
     * @param cfg Config variation.
     * @return Test class.
     */
    private Class<?> makeTestClass(String pkg, VariationsTestsConfig cfg) {
        int idx = cntr.getAndIncrement();

        String clsName = cls.getSimpleName() + "_" + idx;

        cfgs.put(clsName, cfg);

        ClassPool cp = ClassPool.getDefault();
        cp.insertClassPath(new ClassClassPath(ConfigVariationsTestSuiteBuilder.class));

        CtClass cl = cp.makeClass(pkg + "." + clsName);

        try {
            cl.setSuperclass(cp.get(cls.getName()));

            cl.addConstructor(CtNewConstructor.make("public " + clsName + "() { "
                    + "this.testsCfg = "
                    + ConfigVariationsTestSuiteBuilder.class.getName()
                    + ".getCfg(\"" + clsName + "\"); "
                    + "}", cl));

            return cl.toClass();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
