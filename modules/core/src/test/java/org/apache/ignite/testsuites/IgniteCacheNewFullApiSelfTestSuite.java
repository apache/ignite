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
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.GridCacheNewFullApiSelfTest;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.testframework.GridTestSuite;
import org.apache.ignite.testframework.TestsConfiguration;
import org.apache.ignite.testframework.config.ConfigurationFactory;
import org.apache.ignite.testframework.config.StateConfigurationFactory;
import org.apache.ignite.testframework.config.generator.StateIterator;
import org.apache.ignite.testframework.config.params.Variants;
import org.apache.ignite.testframework.config.params.MarshallerProcessor;

/**
 * Test suite for cache API.
 */
public class IgniteCacheNewFullApiSelfTestSuite extends TestSuite {
    /** */
    @SuppressWarnings("unchecked")
    private static final IgniteClosure<IgniteConfiguration, Void>[][] igniteParams = new IgniteClosure[][] {
        {new MarshallerProcessor(new BinaryMarshaller()), new MarshallerProcessor(new OptimizedMarshaller(true))},
        Variants.booleanVariants("setPeerClassLoadingEnabled"),
    };

    /** */
    @SuppressWarnings("unchecked")
    private static final IgniteClosure<CacheConfiguration, Void>[][] cacheParams = new IgniteClosure[][] {
        Variants.enumVariants(CacheMode.class, "setCacheMode"),
        Variants.enumVariants(CacheAtomicityMode.class, "setAtomicityMode"),
        Variants.enumVariants(CacheMemoryMode.class, "setMemoryMode"),
    };

    /**
     * @return Cache API test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Cache New Full API Test Suite");

        final int[] igniteCfgState = new int[] {0, 0}; // Default configuration.
        final int gridsCnt = 1;

        for (StateIterator cacheIter = new StateIterator(cacheParams); cacheIter.hasNext();) {
            int[] cacheCfgState = cacheIter.next();

            ConfigurationFactory factory = new StateConfigurationFactory(igniteParams, igniteCfgState,
                cacheParams, cacheCfgState);

            // Stop all grids before starting new ignite configuration.
            boolean stop = !cacheIter.hasNext();

            String clsNameSuffix = "[igniteCfg=" + Arrays.toString(igniteCfgState)
                + ", cacheCfgState=" + Arrays.toString(cacheCfgState) + "]";

            TestsConfiguration testCfg = new TestsConfiguration(factory, clsNameSuffix, stop, gridsCnt);

            suite.addTest(new GridTestSuite(GridCacheNewFullApiSelfTest.class, testCfg));
        }

        return suite;
    }
}
