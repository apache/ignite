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

package org.apache.ignite.testsuites;

import java.util.Arrays;
import junit.framework.TestSuite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.CacheFullApiNewSelfTest;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.spi.swapspace.inmemory.GridTestSwapSpaceSpi;
import org.apache.ignite.testframework.CacheStartMode;
import org.apache.ignite.testframework.GridTestSuite;
import org.apache.ignite.testframework.TestsConfiguration;
import org.apache.ignite.testframework.config.CacheConfigurationPermutations;
import org.apache.ignite.testframework.config.FullApiStateConfigurationFactory;
import org.apache.ignite.testframework.config.StateConfigurationFactory;
import org.apache.ignite.testframework.config.generator.ConfigurationParameter;
import org.apache.ignite.testframework.config.generator.StateIterator;

import static org.apache.ignite.testframework.config.params.Parameters.booleanParameters;
import static org.apache.ignite.testframework.config.params.Parameters.objectParameters;

/**
 * Test suite for cache API.
 */
public class CacheFullApiBasicCfgNewTestSuite extends TestSuite {
    /** */
    @SuppressWarnings("unchecked")
    private static final ConfigurationParameter<IgniteConfiguration>[][] igniteParams = new ConfigurationParameter[][] {
        objectParameters("setMarshaller", new BinaryMarshaller(), new OptimizedMarshaller(true)),
        booleanParameters("setPeerClassLoadingEnabled"),
        objectParameters("setSwapSpaceSpi", new GridTestSwapSpaceSpi()),
    };

    /** */
    @SuppressWarnings("unchecked")
    private static final ConfigurationParameter<CacheConfiguration>[][] cacheParams =
        CacheConfigurationPermutations.basicSet();

    /** */
    private static CacheStartMode cacheStartMode = CacheStartMode.NODES_THEN_CACHES;

    /**
     * @param cacheStartMode Cache start mode.
     */
    public static void setCacheStartMode(CacheStartMode cacheStartMode) {
        CacheFullApiBasicCfgNewTestSuite.cacheStartMode = cacheStartMode;
    }

    /**
     * @return Cache API test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Cache New Full API Test Suite");

        addTestSuites(suite, 5, true);

        return suite;
    }

    /**
     *  @param suite Suite.
     * @param gridsCnt Grids count.
     * @param withClient With client.
     */
    private static void addTestSuites(TestSuite suite, int gridsCnt, boolean withClient) {
        for (StateIterator igniteCfgIter = new StateIterator(igniteParams); igniteCfgIter.hasNext();) {
            final int[] igniteCfgState = igniteCfgIter.next();

            for (StateIterator cacheCfgIter = new StateIterator(cacheParams); cacheCfgIter.hasNext();) {
                int[] cacheCfgState = cacheCfgIter.next();

                // Stop all grids before starting new ignite configuration.
                addTestSuite(suite, igniteCfgState, cacheCfgState, gridsCnt, !cacheCfgIter.hasNext(), withClient);
            }
        }
    }

    /**
     * @param suite Suite.
     * @param igniteCfgState Ignite config state.
     * @param cacheCfgState Cache config state.
     * @param gridsCnt Grids count.
     * @param stop Stop.
     */
    private static void addTestSuite(TestSuite suite, int[] igniteCfgState, int[] cacheCfgState, int gridsCnt,
        boolean stop, boolean withClient) {
        int testedNodeCnt = 2;

        StateConfigurationFactory factory = new FullApiStateConfigurationFactory(withClient, igniteParams, igniteCfgState,
            cacheParams, cacheCfgState);

        String clsNameSuffix = "[igniteCfg=" + Arrays.toString(igniteCfgState)
            + ", cacheCfgState=" + Arrays.toString(cacheCfgState) + "]"
            + "-[igniteCfg=" + factory.getIgniteConfigurationDescription()
            + ", cacheCfg=" + factory.getCacheConfigurationDescription() + "]";

        TestsConfiguration testCfg = new TestsConfiguration(factory, clsNameSuffix, stop, cacheStartMode, gridsCnt, null);

        suite.addTest(GridTestSuite.createMultiNodeTestSuite(CacheFullApiNewSelfTest.class, testCfg, testedNodeCnt));
    }
}
