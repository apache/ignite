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
import org.apache.ignite.cache.store.jdbc.CacheJdbcBlobStoreFactorySelfTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactorySelfTest;
import org.apache.ignite.cache.store.spring.CacheSpringStoreSessionListenerSelfTest;
import org.apache.ignite.internal.GridFactorySelfTest;
import org.apache.ignite.internal.GridSpringBeanSerializationSelfTest;
import org.apache.ignite.internal.IgniteDynamicCacheConfigTest;
import org.apache.ignite.p2p.GridP2PUserVersionChangeSelfTest;
import org.apache.ignite.spring.GridSpringCacheManagerSelfTest;
import org.apache.ignite.spring.IgniteExcludeInConfigurationTest;
import org.apache.ignite.spring.IgniteStartFromStreamConfigurationTest;

/**
 * Spring tests.
 */
public class IgniteSpringTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Spring Test Suite");

        suite.addTestSuite(GridSpringBeanSerializationSelfTest.class);
        suite.addTestSuite(GridFactorySelfTest.class);

        suite.addTest(IgniteResourceSelfTestSuite.suite());

        suite.addTest(new TestSuite(IgniteExcludeInConfigurationTest.class));

        // Tests moved to this suite since they require Spring functionality.
        suite.addTest(new TestSuite(GridP2PUserVersionChangeSelfTest.class));

        suite.addTest(new TestSuite(GridSpringCacheManagerSelfTest.class));

        suite.addTest(new TestSuite(IgniteDynamicCacheConfigTest.class));

        suite.addTest(new TestSuite(IgniteStartFromStreamConfigurationTest.class));

        suite.addTestSuite(CacheSpringStoreSessionListenerSelfTest.class);

        suite.addTestSuite(CacheJdbcBlobStoreFactorySelfTest.class);

        suite.addTestSuite(CacheJdbcPojoStoreFactorySelfTest.class);

        return suite;
    }
}