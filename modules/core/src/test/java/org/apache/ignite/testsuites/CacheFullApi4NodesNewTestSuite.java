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
import org.apache.ignite.internal.processors.cache.IgniteCacheConfigPermutationsFullApiTest;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.spi.swapspace.inmemory.GridTestSwapSpaceSpi;
import org.apache.ignite.testframework.CacheStartMode;
import org.apache.ignite.testframework.GridTestSuite;
import org.apache.ignite.testframework.TestsConfiguration;
import org.apache.ignite.testframework.config.ConfigurationPermutations;
import org.apache.ignite.testframework.config.ConfigPermutationsFactory;
import org.apache.ignite.testframework.config.generator.ConfigurationParameter;
import org.apache.ignite.testframework.config.generator.PermutationsIterator;

import static org.apache.ignite.testframework.config.params.Parameters.booleanParameters;
import static org.apache.ignite.testframework.config.params.Parameters.objectParameters;

/**
 * Test suite for cache API.
 */
public class CacheFullApi4NodesNewTestSuite extends TestSuite {
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
        ConfigurationPermutations.cacheDefaultSet();


    /**
     * @return Cache API test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        return suite(CacheStartMode.NODES_THEN_CACHES);
    }

    /**
     * @param cacheStartMode Cache start mode.
     * @return Cache API test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite(CacheStartMode cacheStartMode) throws Exception {
        TestSuite suite = new TestSuite("Cache New Full API Test Suite");

        final int[] igniteCfgState = new int[] {0, 0, 0}; // Default configuration.
        final int gridsCnt = 4;

        for (PermutationsIterator cacheIter = new PermutationsIterator(cacheParams); cacheIter.hasNext();) {
            int[] cacheCfgState = cacheIter.next();

            // Stop all grids before starting new ignite configuration.
            addTestSuite(suite, igniteCfgState, cacheCfgState, gridsCnt, !cacheIter.hasNext(), cacheStartMode);
        }

        return suite;
    }

    /**
     * @param suite Suite.
     * @param igniteCfgState Ignite config state.
     * @param cacheCfgState Cache config state.
     * @param gridsCnt Grids count.
     * @param stop Stop.
     * @param cacheStartMode Cache start mode.
     */
    private static void addTestSuite(TestSuite suite, int[] igniteCfgState, int[] cacheCfgState, int gridsCnt,
        boolean stop, CacheStartMode cacheStartMode) {
        ConfigPermutationsFactory factory = new ConfigPermutationsFactory(false, igniteParams, igniteCfgState,
            cacheParams, cacheCfgState);

        String clsNameSuffix = "[igniteCfg=" + Arrays.toString(igniteCfgState)
            + ", cacheCfgState=" + Arrays.toString(cacheCfgState) + "]"
            + "-[igniteCfg=" + factory.getIgniteConfigurationDescription()
            + ", cacheCfg=" + factory.getCacheConfigurationDescription() + "]";

        TestsConfiguration testCfg = new TestsConfiguration(factory, clsNameSuffix, stop, cacheStartMode, gridsCnt);

        suite.addTest(new GridTestSuite(IgniteCacheConfigPermutationsFullApiTest.class, testCfg));
    }
}
