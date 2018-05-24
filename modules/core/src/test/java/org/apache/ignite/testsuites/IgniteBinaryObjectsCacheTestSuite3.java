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
import org.apache.ignite.internal.processors.cache.binary.GridCacheBinaryAtomicEntryProcessorDeploymentSelfTest;
import org.apache.ignite.internal.processors.cache.binary.GridCacheBinaryTransactionalEntryProcessorDeploymentSelfTest;
import org.apache.ignite.testframework.config.GridTestProperties;

/**
 *  IgniteBinaryObjectsCacheTestSuite3 is kept together with {@link IgniteCacheTestSuite3}
 *  for backward compatibility.
 *
 *  In Ignite 2.0 tests
 *  -  http://ci.ignite.apache.org/viewType.html?buildTypeId=Ignite20Tests_IgniteCache3
 *  IgniteBinaryObjectsCacheTestSuite3 is used,
 *
 *  and in Ignite tests
 *  http://ci.ignite.apache.org/viewType.html?buildTypeId=IgniteTests_IgniteCache3
 *  - IgniteCacheTestSuite3.
 *  And if someone runs old run configs then most test will be executed anyway.
 *
 *  In future this suite may be merged with {@link IgniteCacheTestSuite3}
 *
 */
public class IgniteBinaryObjectsCacheTestSuite3 {
    /**
     * @return Test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        GridTestProperties.setProperty(GridTestProperties.ENTRY_PROCESSOR_CLASS_NAME,
            "org.apache.ignite.tests.p2p.CacheDeploymentBinaryEntryProcessor");

        TestSuite suite = IgniteCacheTestSuite3.suite();

        suite.addTestSuite(GridCacheBinaryAtomicEntryProcessorDeploymentSelfTest.class);
        suite.addTestSuite(GridCacheBinaryTransactionalEntryProcessorDeploymentSelfTest.class);

        return suite;
    }
}
