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
import org.apache.ignite.cache.spring.GridSpringCacheManagerMultiJvmSelfTest;
import org.apache.ignite.cache.spring.GridSpringCacheManagerSelfTest;
import org.apache.ignite.cache.spring.GridSpringCacheManagerSpringBeanSelfTest;
import org.apache.ignite.cache.spring.SpringCacheManagerContextInjectionTest;
import org.apache.ignite.cache.spring.SpringCacheTest;
import org.apache.ignite.encryption.SpringEncryptedCacheRestartClientTest;
import org.apache.ignite.encryption.SpringEncryptedCacheRestartTest;
import org.apache.ignite.spring.injection.IgniteSpringBeanSpringResourceInjectionTest;
import org.apache.ignite.internal.IgniteSpringBeanTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcBlobStoreFactorySelfTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactorySelfTest;
import org.apache.ignite.cache.store.jdbc.CachePojoStoreXmlSelfTest;
import org.apache.ignite.cache.store.jdbc.CachePojoStoreXmlWithSqlEscapeSelfTest;
import org.apache.ignite.cache.store.spring.CacheSpringStoreSessionListenerSelfTest;
import org.apache.ignite.internal.GridFactorySelfTest;
import org.apache.ignite.internal.GridSpringBeanSerializationSelfTest;
import org.apache.ignite.internal.IgniteDynamicCacheConfigTest;
import org.apache.ignite.internal.processors.resource.GridTransformSpringInjectionSelfTest;
import org.apache.ignite.p2p.GridP2PUserVersionChangeSelfTest;
import org.apache.ignite.spring.IgniteExcludeInConfigurationTest;
import org.apache.ignite.spring.IgniteStartFromStreamConfigurationTest;
import org.apache.ignite.spring.injection.GridServiceInjectionSpringResourceTest;
import org.apache.ignite.startup.cmdline.GridCommandLineLoaderTest;
import org.apache.ignite.testframework.IgniteTestSuite;
import org.apache.ignite.transactions.spring.GridSpringTransactionManagerSelfTest;
import org.apache.ignite.transactions.spring.GridSpringTransactionManagerSpringBeanSelfTest;
import org.apache.ignite.transactions.spring.SpringTransactionManagerContextInjectionTest;

/**
 * Spring tests.
 */
public class IgniteSpringTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new IgniteTestSuite("Spring Test Suite");

        suite.addTestSuite(GridSpringBeanSerializationSelfTest.class);
        suite.addTestSuite(IgniteSpringBeanTest.class);
        suite.addTestSuite(GridFactorySelfTest.class);

        suite.addTest(IgniteResourceSelfTestSuite.suite());

        suite.addTestSuite(IgniteExcludeInConfigurationTest.class);

        // Tests moved to this suite since they require Spring functionality.
        suite.addTestSuite(GridP2PUserVersionChangeSelfTest.class);

        suite.addTestSuite(GridSpringCacheManagerSelfTest.class);
        suite.addTestSuite(GridSpringCacheManagerSpringBeanSelfTest.class);

        suite.addTestSuite(IgniteDynamicCacheConfigTest.class);

        suite.addTestSuite(IgniteStartFromStreamConfigurationTest.class);

        suite.addTestSuite(CacheSpringStoreSessionListenerSelfTest.class);

        suite.addTestSuite(CacheJdbcBlobStoreFactorySelfTest.class);
        suite.addTestSuite(CacheJdbcPojoStoreFactorySelfTest.class);
        suite.addTestSuite(CachePojoStoreXmlSelfTest.class);
        suite.addTestSuite(CachePojoStoreXmlWithSqlEscapeSelfTest.class);

        suite.addTestSuite(GridSpringTransactionManagerSelfTest.class);
        suite.addTestSuite(GridSpringTransactionManagerSpringBeanSelfTest.class);

        suite.addTestSuite(GridServiceInjectionSpringResourceTest.class);
        suite.addTestSuite(IgniteSpringBeanSpringResourceInjectionTest.class);

        suite.addTestSuite(GridTransformSpringInjectionSelfTest.class);

        suite.addTestSuite(SpringCacheManagerContextInjectionTest.class);
        suite.addTestSuite(SpringTransactionManagerContextInjectionTest.class);

        suite.addTestSuite(SpringCacheTest.class);

        suite.addTestSuite(SpringEncryptedCacheRestartTest.class);
        suite.addTestSuite(SpringEncryptedCacheRestartClientTest.class);

        //suite.addTestSuite(GridSpringCacheManagerMultiJvmSelfTest.class);
        suite.addTestSuite(GridSpringCacheManagerMultiJvmSelfTest.class);

        suite.addTestSuite(GridCommandLineLoaderTest.class);

        return suite;
    }
}
