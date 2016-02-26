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

package org.apache.ignite.testframework.config;

import java.util.Arrays;
import junit.framework.TestSuite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteCacheConfigPermutationsAbstractTest;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.IgniteConfigPermutationsTestSuite;
import org.apache.ignite.testframework.TestsConfiguration;
import org.apache.ignite.testframework.junits.IgniteConfigPermutationsAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 * Configuration permutations test suite builder.
 */
public class ConfigPermutationsTestSuiteBuilder {
    /** */
    private final TestSuite suite;

    /** */
    @SuppressWarnings("unchecked")
    private ConfigurationParameter<IgniteConfiguration>[][] igniteParams =
        ConfigurationPermutations.igniteBasicSet();

    /** */
    @SuppressWarnings("unchecked")
    private ConfigurationParameter<CacheConfiguration>[][] cacheParams;

    /** */
    private CacheStartMode cacheStartMode = CacheStartMode.NODES_THEN_CACHES;

    /** */
    private boolean withClients;

    /** */
    private int gridsCnt = 3;

    /** */
    private int testedNodeCnt = 1;

    /** */
    private Class<? extends IgniteConfigPermutationsAbstractTest> cls;

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

    /**
     * @param name Name.
     * @param cls Test class.
     */
    public ConfigPermutationsTestSuiteBuilder(String name, Class<? extends IgniteConfigPermutationsAbstractTest> cls) {
        suite = new TestSuite(name);
        this.cls = cls;
    }

    /**
     * @return Test suite.
     */
    public TestSuite build() {
        assert testedNodeCnt > 0;
        assert gridsCnt > 0;

        PermutationsIterator igniteCfgIter;

        if (specificIgniteParam == null)
            igniteCfgIter = new PermutationsIterator(igniteParams);
        else
            igniteCfgIter = new OneElementPermutationsIterator(specificIgniteParam, igniteParams);

        for (; igniteCfgIter.hasNext(); ) {
            final int[] igniteCfgPermutation = igniteCfgIter.next();

            if (!passIgniteConfigFilter(igniteCfgPermutation))
                continue;

            if (cacheParams == null) {
                TestSuite addedSuite = build(igniteCfgPermutation, null, true);

                suite.addTest(addedSuite);
            }
            else {
                PermutationsIterator cacheCfgIter;

                if (specificCacheParam == null)
                    cacheCfgIter = new PermutationsIterator(cacheParams);
                else
                    cacheCfgIter = new OneElementPermutationsIterator(specificCacheParam, cacheParams);

                for (; cacheCfgIter.hasNext(); ) {
                    int[] cacheCfgPermutation = cacheCfgIter.next();

                    if (!passCacheConfigFilter(cacheCfgPermutation))
                        continue;

                    // Stop all grids before starting new ignite configuration.
                    boolean stopNodes = !cacheCfgIter.hasNext();

                    TestSuite addedSuite = build(igniteCfgPermutation, cacheCfgPermutation, stopNodes);

                    suite.addTest(addedSuite);
                }
            }
        }

        return suite;
    }

    /**
     * @param permutation Permutation.
     * @return {@code True} if permutation pass filters.
     */
    private boolean passIgniteConfigFilter(int[] permutation) {
        ConfigPermutationsFactory factory = new ConfigPermutationsFactory(igniteParams, permutation, null, null);

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
     * @param permutation Permutation.
     * @return {@code True} if permutation pass filters.
     */
    private boolean passCacheConfigFilter(int[] permutation) {
        ConfigPermutationsFactory factory = new ConfigPermutationsFactory(null, null, cacheParams, permutation);

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
     * @param igniteCfgPermutation Ignite permutation.
     * @param cacheCfgPermutation Cache permutation.
     * @param stopNodes Stop nodes.
     * @return Test suite.
     */
    private TestSuite build(int[] igniteCfgPermutation, @Nullable int[] cacheCfgPermutation, boolean stopNodes) {
        ConfigPermutationsFactory factory = new ConfigPermutationsFactory(igniteParams,
            igniteCfgPermutation, cacheParams, cacheCfgPermutation);

        factory.backups(backups);

        String clsNameSuffix = "[igniteCfgPermutation=" + Arrays.toString(igniteCfgPermutation)
            + ", cacheCfgPermutation=" + Arrays.toString(cacheCfgPermutation)
            + ", igniteCfg=" + factory.getIgniteConfigurationDescription()
            + ", cacheCfg=" + factory.getCacheConfigurationDescription() + "]";

        TestsConfiguration testCfg = new TestsConfiguration(factory, clsNameSuffix, stopNodes, cacheStartMode, gridsCnt);

        TestSuite addedSuite;

        if (testedNodeCnt > 1)
            addedSuite = IgniteConfigPermutationsTestSuite.createMultiNodeTestSuite(
                (Class<? extends IgniteCacheConfigPermutationsAbstractTest>)cls, testCfg, testedNodeCnt, withClients);
        else
            addedSuite = new IgniteConfigPermutationsTestSuite(cls, testCfg);

        return addedSuite;
    }

    /**
     * @return {@code this} for chaining.
     */
    public ConfigPermutationsTestSuiteBuilder withClients() {
        if (testedNodeCnt < 2)
            throw new IllegalStateException("Tested node count should be more than 1: " + testedNodeCnt);

        withClients = true;

        return this;
    }

    /**
     * @param testedNodeCnt Tested node count.
     * @return {@code this} for chaining.
     */
    public ConfigPermutationsTestSuiteBuilder testedNodesCount(int testedNodeCnt) {
        this.testedNodeCnt = testedNodeCnt;

        return this;
    }

    /**
     * @param cnt Count.
     * @return {@code this} for chaining.
     */
    public ConfigPermutationsTestSuiteBuilder gridsCount(int cnt) {
        assert cnt > 0;

        gridsCnt = cnt;

        return this;
    }

    /**
     * @param igniteParams New ignite params.
     * @return {@code this} for chaining.
     */
    public ConfigPermutationsTestSuiteBuilder igniteParams(
        ConfigurationParameter<IgniteConfiguration>[][] igniteParams) {
        this.igniteParams = igniteParams;

        return this;
    }

    /**
     * @param cacheParams New cache params.
     * @return {@code this} for chaining.
     */
    public ConfigPermutationsTestSuiteBuilder cacheParams(ConfigurationParameter<CacheConfiguration>[][] cacheParams) {
        this.cacheParams = cacheParams;

        return this;
    }

    /**
     * Sets basic cache params and basic count of backups.
     *
     * @return {@code this} for chaining.
     */
    public ConfigPermutationsTestSuiteBuilder withBasicCacheParams() {
        cacheParams = ConfigurationPermutations.cacheBasicSet();
        backups = 1;

        return this;
    }

    /**
     * @param backups Backups.
     * @return {@code this} for chaining.
     */
    public ConfigPermutationsTestSuiteBuilder backups(int backups) {
        assert backups > 0 : backups;

        this.backups = backups;

        return this;
    }

    /**
     * @param singleIgniteParam Param.
     * @return {@code this} for chaining.
     */
    public ConfigPermutationsTestSuiteBuilder specifyIgniteParam(int... singleIgniteParam) {
        specificIgniteParam = singleIgniteParam;

        return this;
    }

    /**
     * @param singleParam Param.
     * @return {@code this} for chaining.
     */
    public ConfigPermutationsTestSuiteBuilder specifyCacheParam(int... singleParam) {
        specificCacheParam = singleParam;

        return this;
    }

    /**
     * @param filters Ignite configuration filters.
     * @return {@code this} for chaining.
     */
    public ConfigPermutationsTestSuiteBuilder withIgniteConfigFilters(IgnitePredicate<IgniteConfiguration>... filters) {
        igniteCfgFilters = filters;

        return this;
    }

    /**
     * @param filters Ignite configuration filters.
     * @return {@code this} for chaining.
     */
    public ConfigPermutationsTestSuiteBuilder withCacheConfigFilters(IgnitePredicate<CacheConfiguration>... filters) {
        cacheCfgFilters = filters;

        return this;
    }

    /**
     *
     */
    private static class OneElementPermutationsIterator extends PermutationsIterator {
        /** */
        private int[] elem;

        /** */
        private boolean hasNext = true;

        /**
         * @param elem Element.
         */
        OneElementPermutationsIterator(int[] elem, Object[][] params) {
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
