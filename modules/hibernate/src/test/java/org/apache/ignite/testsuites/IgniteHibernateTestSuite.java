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
import org.apache.ignite.cache.hibernate.HibernateL2CacheConfigurationSelfTest;
import org.apache.ignite.cache.hibernate.HibernateL2CacheSelfTest;
import org.apache.ignite.cache.hibernate.HibernateL2CacheTransactionalSelfTest;
import org.apache.ignite.cache.store.hibernate.CacheHibernateBlobStoreNodeRestartTest;
import org.apache.ignite.cache.store.hibernate.CacheHibernateBlobStoreSelfTest;
import org.apache.ignite.cache.store.hibernate.CacheHibernateStoreFactorySelfTest;
import org.apache.ignite.cache.store.hibernate.CacheHibernateStoreSessionListenerSelfTest;

/**
 * Hibernate integration tests.
 */
public class IgniteHibernateTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Hibernate Integration Test Suite");

        // Hibernate L2 cache.
        suite.addTestSuite(HibernateL2CacheSelfTest.class);
        suite.addTestSuite(HibernateL2CacheTransactionalSelfTest.class);
        suite.addTestSuite(HibernateL2CacheConfigurationSelfTest.class);

        suite.addTestSuite(CacheHibernateBlobStoreSelfTest.class);

        suite.addTestSuite(CacheHibernateBlobStoreNodeRestartTest.class);

        suite.addTestSuite(CacheHibernateStoreSessionListenerSelfTest.class);

        suite.addTestSuite(CacheHibernateStoreFactorySelfTest.class);

        return suite;
    }
}