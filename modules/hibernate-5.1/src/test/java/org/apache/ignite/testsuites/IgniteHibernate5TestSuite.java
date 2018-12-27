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
import org.apache.ignite.cache.hibernate.HibernateL2CacheConfigurationSelfTest;
import org.apache.ignite.cache.hibernate.HibernateL2CacheMultiJvmTest;
import org.apache.ignite.cache.hibernate.HibernateL2CacheSelfTest;
import org.apache.ignite.cache.hibernate.HibernateL2CacheStrategySelfTest;
import org.apache.ignite.cache.hibernate.HibernateL2CacheTransactionalSelfTest;
import org.apache.ignite.cache.hibernate.HibernateL2CacheTransactionalUseSyncSelfTest;
import org.apache.ignite.cache.store.hibernate.CacheHibernateBlobStoreNodeRestartTest;
import org.apache.ignite.cache.store.hibernate.CacheHibernateBlobStoreSelfTest;
import org.apache.ignite.cache.store.hibernate.CacheHibernateStoreFactorySelfTest;
import org.apache.ignite.cache.store.hibernate.CacheHibernateStoreSessionListenerSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Hibernate integration tests.
 */
@RunWith(AllTests.class)
public class IgniteHibernate5TestSuite {
    /**
     * @return Test suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Hibernate5 Integration Test Suite");

        // Hibernate L2 cache.
        suite.addTest(new JUnit4TestAdapter(HibernateL2CacheSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(HibernateL2CacheTransactionalSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(HibernateL2CacheTransactionalUseSyncSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(HibernateL2CacheConfigurationSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(HibernateL2CacheStrategySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(HibernateL2CacheMultiJvmTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheHibernateBlobStoreSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheHibernateBlobStoreNodeRestartTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheHibernateStoreSessionListenerSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheHibernateStoreFactorySelfTest.class));

        return suite;
    }
}
