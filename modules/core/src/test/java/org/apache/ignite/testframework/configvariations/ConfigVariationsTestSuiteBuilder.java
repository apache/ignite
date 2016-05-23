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

import java.util.Arrays;
import junit.framework.TestSuite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.IgniteCacheConfigVariationsAbstractTest;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.IgniteConfigVariationsAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 * Configuration variations test suite builder.
 */
public class ConfigVariationsTestSuiteBuilder {
    /** */
    private final TestSuite suite;

    /** */
    @SuppressWarnings("unchecked")
    private ConfigParameter<IgniteConfiguration>[][] igniteParams =
        ConfigVariations.igniteBasicSet();

    /** */
    @SuppressWarnings("unchecked")
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
     * @param name Name.
     * @param cls Test class.
     */
    public ConfigVariationsTestSuiteBuilder(String name, Class<? extends IgniteConfigVariationsAbstractTest> cls) {
        suite = new TestSuite(name);
        this.cls = cls;
    }

    /**
     * @return Test suite.
     */
    public TestSuite build() {
        assert testedNodeCnt > 0;
        assert gridsCnt > 0;

        VariationsIterator igniteCfgIter;

        if (specificIgniteParam == null)
            igniteCfgIter = new VariationsIterator(igniteParams);
        else
            igniteCfgIter = new OneElementVariationsIterator(specificIgniteParam, igniteParams);

        for (; igniteCfgIter.hasNext(); ) {
            final int[] igniteCfgVariation = igniteCfgIter.next();

            if (!passIgniteConfigFilter(igniteCfgVariation))
                continue;

            if (cacheParams == null) {
                TestSuite addedSuite = build(igniteCfgVariation, null, true);

                suite.addTest(addedSuite);
            }
            else {
                VariationsIterator cacheCfgIter;

                if (specificCacheParam == null)
                    cacheCfgIter = new VariationsIterator(cacheParams);
                else
                    cacheCfgIter = new OneElementVariationsIterator(specificCacheParam, cacheParams);

                for (; cacheCfgIter.hasNext(); ) {
                    int[] cacheCfgVariation = cacheCfgIter.next();

                    if (!passCacheConfigFilter(cacheCfgVariation))
                        continue;

                    // Stop all grids before starting new ignite configuration.
                    boolean stopNodes = !cacheCfgIter.hasNext();

                    TestSuite addedSuite = build(igniteCfgVariation, cacheCfgVariation, stopNodes);

                    suite.addTest(addedSuite);
                }
            }
        }

        return suite;
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
     * @return Test suite.
     */
    private TestSuite build(int[] igniteCfgVariation, @Nullable int[] cacheCfgVariation, boolean stopNodes) {
        ConfigVariationsFactory factory = new ConfigVariationsFactory(igniteParams,
            igniteCfgVariation, cacheParams, cacheCfgVariation);

        factory.backups(backups);

        String clsNameSuffix = "[igniteCfgVariation=" + Arrays.toString(igniteCfgVariation)
            + ", cacheCfgVariation=" + Arrays.toString(cacheCfgVariation)
            + ", igniteCfg=" + factory.getIgniteConfigurationDescription()
            + ", cacheCfg=" + factory.getCacheConfigurationDescription() + "]";

        VariationsTestsConfig testCfg = new VariationsTestsConfig(factory, clsNameSuffix, stopNodes, cacheStartMode,
            gridsCnt, !skipWaitPartMapExchange);

        TestSuite addedSuite;

        if (testedNodeCnt > 1)
            addedSuite = createMultiNodeTestSuite((Class<? extends IgniteCacheConfigVariationsAbstractTest>)cls,
                testCfg, testedNodeCnt, withClients, skipWaitPartMapExchange);
       else
            addedSuite = new IgniteConfigVariationsTestSuite(cls, testCfg);

        return addedSuite;
    }

    /**
     * @param cls Test class.
     * @param cfg Configuration.
     * @param testedNodeCnt Count of tested nodes.
     */
    private static TestSuite createMultiNodeTestSuite(Class<? extends IgniteCacheConfigVariationsAbstractTest> cls,
        VariationsTestsConfig cfg, int testedNodeCnt, boolean withClients, boolean skipWaitParMapExchange) {
        TestSuite suite = new TestSuite();

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

            suite.addTest(new IgniteConfigVariationsTestSuite(cls, cfg0));
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
        this.igniteParams = igniteParams;

        return this;
    }

    /**
     * @param cacheParams New cache params.
     * @return {@code this} for chaining.
     */
    public ConfigVariationsTestSuiteBuilder cacheParams(ConfigParameter<CacheConfiguration>[][] cacheParams) {
        this.cacheParams = cacheParams;

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
    public ConfigVariationsTestSuiteBuilder specifyIgniteParam(int... singleIgniteParam) {
        specificIgniteParam = singleIgniteParam;

        return this;
    }

    /**
     * @param singleParam Param.
     * @return {@code this} for chaining.
     */
    public ConfigVariationsTestSuiteBuilder specifyCacheParam(int... singleParam) {
        specificCacheParam = singleParam;

        return this;
    }

    /**
     * @param filters Ignite configuration filters.
     * @return {@code this} for chaining.
     */
    public ConfigVariationsTestSuiteBuilder withIgniteConfigFilters(IgnitePredicate<IgniteConfiguration>... filters) {
        igniteCfgFilters = filters;

        return this;
    }

    public ConfigVariationsTestSuiteBuilder skipWaitPartitionMapExchange() {
        skipWaitPartMapExchange = true;

        return this;
    }

    /**
     * @param filters Ignite configuration filters.
     * @return {@code this} for chaining.
     */
    public ConfigVariationsTestSuiteBuilder withCacheConfigFilters(IgnitePredicate<CacheConfiguration>... filters) {
        cacheCfgFilters = filters;

        return this;
    }

    /**
     *
     */
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
        @Override public int[] next() {
            hasNext = false;

            return elem;
        }
    }
}
