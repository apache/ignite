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

import junit.framework.JUnit4TestAdapter;
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

        suite.addTest(new JUnit4TestAdapter(GridSpringBeanSerializationSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteSpringBeanTest.class));
        suite.addTest(new JUnit4TestAdapter(GridFactorySelfTest.class));

        suite.addTest(IgniteResourceSelfTestSuite.suite());

        suite.addTest(new JUnit4TestAdapter(IgniteExcludeInConfigurationTest.class));

        // Tests moved to this suite since they require Spring functionality.
        suite.addTest(new JUnit4TestAdapter(GridP2PUserVersionChangeSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridSpringCacheManagerSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSpringCacheManagerSpringBeanSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteDynamicCacheConfigTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteStartFromStreamConfigurationTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheSpringStoreSessionListenerSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheJdbcBlobStoreFactorySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheJdbcPojoStoreFactorySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CachePojoStoreXmlSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CachePojoStoreXmlWithSqlEscapeSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridSpringTransactionManagerSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSpringTransactionManagerSpringBeanSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridServiceInjectionSpringResourceTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteSpringBeanSpringResourceInjectionTest.class));

        suite.addTest(new JUnit4TestAdapter(GridTransformSpringInjectionSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(SpringCacheManagerContextInjectionTest.class));
        suite.addTest(new JUnit4TestAdapter(SpringTransactionManagerContextInjectionTest.class));

        suite.addTest(new JUnit4TestAdapter(SpringCacheTest.class));

        suite.addTest(new JUnit4TestAdapter(SpringEncryptedCacheRestartTest.class));
        suite.addTest(new JUnit4TestAdapter(SpringEncryptedCacheRestartClientTest.class));

        //suite.addTest(new JUnit4TestAdapter(GridSpringCacheManagerMultiJvmSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSpringCacheManagerMultiJvmSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCommandLineLoaderTest.class));

        return suite;
    }
}
