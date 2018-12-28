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
import org.junit.runners.Suite;

/**
 * Hibernate integration tests.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    HibernateL2CacheSelfTest.class,
    HibernateL2CacheTransactionalSelfTest.class,
    HibernateL2CacheTransactionalUseSyncSelfTest.class,
    HibernateL2CacheConfigurationSelfTest.class,
    HibernateL2CacheStrategySelfTest.class,
    HibernateL2CacheMultiJvmTest.class,
    CacheHibernateBlobStoreSelfTest.class,
    CacheHibernateBlobStoreNodeRestartTest.class,
    CacheHibernateStoreSessionListenerSelfTest.class,
    CacheHibernateStoreFactorySelfTest.class
})
public class IgniteHibernate53TestSuite {
}
