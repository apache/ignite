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

import junit.framework.TestSuite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.compress.CompressionProcessorTest;
import org.apache.ignite.internal.processors.compress.FileSystemUtilsTest;
import org.apache.ignite.internal.processors.compress.PageCompressionIntegrationAsyncTest;
import org.apache.ignite.internal.processors.compress.PageCompressionIntegrationTest;

import static org.apache.ignite.configuration.PageCompression.ZSTD;
import static org.apache.ignite.testframework.config.GridTestProperties.IGNITE_CFG_PREPROCESSOR_CLS;

/**
 */
public class IgnitePdsCompressionTestSuite {
    /**
     * @return Suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite Persistent Store Test Suite (with page compression).");

        suite.addTestSuite(CompressionProcessorTest.class);
        suite.addTestSuite(FileSystemUtilsTest.class);
        suite.addTestSuite(PageCompressionIntegrationTest.class);
        suite.addTestSuite(PageCompressionIntegrationAsyncTest.class);

        System.setProperty(IGNITE_CFG_PREPROCESSOR_CLS, IgnitePdsCompressionTestSuite.class.getName());

        IgnitePdsTestSuite.addRealPageStoreTests(suite);

        return suite;
    }

    /**
     * @param cfg Enable page compression for all the persistent caches..
     */
    public static void preprocessConfiguration(IgniteConfiguration cfg) {
        for (CacheConfiguration ccfg : cfg.getCacheConfiguration()) {
            if (ccfg.getPageCompression() == null) {
                System.err.println(" >>> Enabling disk page compression for cache: " + ccfg.getName());

                ccfg.setPageCompression(ZSTD);
            }
        }
    }
}
