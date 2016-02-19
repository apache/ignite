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

package org.apache.ignite.testframework.config.generator;

import java.util.Arrays;
import junit.framework.TestSuite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteCacheConfigPermutationsAbstractTest;
import org.apache.ignite.testframework.CacheStartMode;
import org.apache.ignite.testframework.GridTestSuite;
import org.apache.ignite.testframework.TestsConfiguration;
import org.apache.ignite.testframework.config.ConfigurationPermutations;
import org.apache.ignite.testframework.config.StateConfigurationFactory;
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

        StateIterator igniteCfgIter;

        if (specificIgniteParam == null)
            igniteCfgIter = new StateIterator(igniteParams);
        else
            igniteCfgIter = new OneElementStateIterator(specificIgniteParam, igniteParams);

        for (; igniteCfgIter.hasNext(); ) {
            final int[] igniteCfgState = igniteCfgIter.next();

            if (cacheParams == null)
                suite.addTest(build(igniteCfgState, null, true));
            else {
                StateIterator cacheCfgIter;

                if (specificCacheParam == null)
                    cacheCfgIter = new StateIterator(cacheParams);
                else
                    cacheCfgIter = new OneElementStateIterator(specificCacheParam, cacheParams);

                for (; cacheCfgIter.hasNext(); ) {
                    int[] cacheCfgState = cacheCfgIter.next();

                    // Stop all grids before starting new ignite configuration.
                    boolean stopNodes = !cacheCfgIter.hasNext();

                    TestSuite addedSuite = build(igniteCfgState, cacheCfgState, stopNodes);

                    suite.addTest(addedSuite);
                }
            }
        }

        return suite;
    }

    /**
     * @param igniteCfgState Ignite permutation.
     * @param cacheCfgState Cache permutation.
     * @param stopNodes Stop nodes.
     * @return Test suite.
     */
    private TestSuite build(int[] igniteCfgState, @Nullable int[] cacheCfgState, boolean stopNodes) {
        StateConfigurationFactory factory = new StateConfigurationFactory(withClients, igniteParams,
            igniteCfgState, cacheParams, cacheCfgState);

        factory.backups(backups);

        String clsNameSuffix = "[igniteCfg=" + Arrays.toString(igniteCfgState)
            + ", cacheCfgState=" + Arrays.toString(cacheCfgState)
            + ", igniteCfg=" + factory.getIgniteConfigurationDescription()
            + ", cacheCfg=" + factory.getCacheConfigurationDescription() + "]";

        TestsConfiguration testCfg = new TestsConfiguration(factory, clsNameSuffix, stopNodes, cacheStartMode, gridsCnt);

        TestSuite addedSuite;

        if (withClients)
            addedSuite = GridTestSuite.createMultiNodeTestSuite(
                (Class<? extends IgniteCacheConfigPermutationsAbstractTest>)cls, testCfg, testedNodeCnt);
        else
            addedSuite = new GridTestSuite(cls, testCfg);

        return addedSuite;
    }

    /**
     * @return {@code this} for chaining.
     */
    public ConfigPermutationsTestSuiteBuilder withClients() {
        assert IgniteCacheConfigPermutationsAbstractTest.class.isAssignableFrom(cls) : "'WithClients' mode supported " +
            "only for instances of " + IgniteCacheConfigPermutationsAbstractTest.class.getSimpleName() + ": " + cls;

        withClients = true;
        testedNodeCnt = 3;

        return this;
    }

    /**
     * @return {@code this} for chaining.
     */
    public ConfigPermutationsTestSuiteBuilder gridsCount(int cnt) {
        assert cnt > 0;

        gridsCnt = cnt;

        return this;
    }

    /**
     * @param igniteParams New ignite params.
     */
    public ConfigPermutationsTestSuiteBuilder igniteParams(
        ConfigurationParameter<IgniteConfiguration>[][] igniteParams) {
        this.igniteParams = igniteParams;

        return this;
    }

    /**
     * @param cacheParams New cache params.
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
        assert backups > 0: backups;

        this.backups = backups;

        return this;
    }

    /**
     * @param singleIgniteParam Param
     * @return {@code this} for chaining.
     */
    public ConfigPermutationsTestSuiteBuilder specifyIgniteParam(int... singleIgniteParam) {
        this.specificIgniteParam = singleIgniteParam;

        return this;
    }

    /**
     * @param singleParam Param
     * @return {@code this} for chaining.
     */
    public ConfigPermutationsTestSuiteBuilder specifyCacheParam(int... singleParam) {
        specificCacheParam = singleParam;

        return this;
    }

    /**
     *
     */
    private static class OneElementStateIterator extends StateIterator {
        /** */
        private int[] elem;

        /** */
        private boolean hasNext = true;

        /**
         * @param elem Element.
         */
        OneElementStateIterator(int[] elem, Object[][] params) {
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
