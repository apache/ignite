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

import org.apache.ignite.cache.spring.GridSpringCacheManagerMultiJvmSelfTest;
import org.apache.ignite.cache.spring.GridSpringCacheManagerSelfTest;
import org.apache.ignite.cache.spring.GridSpringCacheManagerSpringBeanSelfTest;
import org.apache.ignite.cache.spring.SpringCacheManagerContextInjectionTest;
import org.apache.ignite.cache.spring.SpringCacheTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcBlobStoreFactorySelfTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactorySelfTest;
import org.apache.ignite.cache.store.jdbc.CachePojoStoreXmlSelfTest;
import org.apache.ignite.cache.store.jdbc.CachePojoStoreXmlWithSqlEscapeSelfTest;
import org.apache.ignite.cache.store.spring.CacheSpringStoreSessionListenerSelfTest;
import org.apache.ignite.cluster.ClusterStateXmlPropertiesTest;
import org.apache.ignite.encryption.SpringEncryptedCacheRestartClientTest;
import org.apache.ignite.encryption.SpringEncryptedCacheRestartTest;
import org.apache.ignite.internal.GridFactorySelfTest;
import org.apache.ignite.internal.GridSpringBeanSerializationSelfTest;
import org.apache.ignite.internal.IgniteDynamicCacheConfigTest;
import org.apache.ignite.internal.IgniteSpringBeanTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtMultiBackupTest;
import org.apache.ignite.internal.processors.resource.GridTransformSpringInjectionSelfTest;
import org.apache.ignite.p2p.GridP2PUserVersionChangeSelfTest;
import org.apache.ignite.spring.IgniteExcludeInConfigurationTest;
import org.apache.ignite.spring.IgniteStartFromStreamConfigurationTest;
import org.apache.ignite.spring.injection.GridServiceInjectionSpringResourceTest;
import org.apache.ignite.spring.injection.IgniteSpringBeanSpringResourceInjectionTest;
import org.apache.ignite.startup.cmdline.GridCommandLineLoaderTest;
import org.apache.ignite.transactions.spring.GridSpringTransactionManagerSelfTest;
import org.apache.ignite.transactions.spring.GridSpringTransactionManagerSpringBeanSelfTest;
import org.apache.ignite.transactions.spring.SpringTransactionManagerContextInjectionTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Spring tests.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    GridSpringBeanSerializationSelfTest.class,
    IgniteSpringBeanTest.class,
    GridFactorySelfTest.class,

    IgniteResourceSelfTestSuite.class,

    IgniteExcludeInConfigurationTest.class,

    // Tests moved to this suite since they require Spring functionality.
    GridP2PUserVersionChangeSelfTest.class,

    GridSpringCacheManagerSelfTest.class,
    GridSpringCacheManagerSpringBeanSelfTest.class,

    IgniteDynamicCacheConfigTest.class,

    IgniteStartFromStreamConfigurationTest.class,

    CacheSpringStoreSessionListenerSelfTest.class,

    CacheJdbcBlobStoreFactorySelfTest.class,
    CacheJdbcPojoStoreFactorySelfTest.class,
    CachePojoStoreXmlSelfTest.class,
    CachePojoStoreXmlWithSqlEscapeSelfTest.class,

    GridSpringTransactionManagerSelfTest.class,
    GridSpringTransactionManagerSpringBeanSelfTest.class,

    GridServiceInjectionSpringResourceTest.class,
    IgniteSpringBeanSpringResourceInjectionTest.class,

    GridTransformSpringInjectionSelfTest.class,

    SpringCacheManagerContextInjectionTest.class,
    SpringTransactionManagerContextInjectionTest.class,

    SpringCacheTest.class,

    SpringEncryptedCacheRestartTest.class,
    SpringEncryptedCacheRestartClientTest.class,

    GridSpringCacheManagerMultiJvmSelfTest.class,

    GridCommandLineLoaderTest.class,

    GridCacheDhtMultiBackupTest.class,

    ClusterStateXmlPropertiesTest.class
})
public class IgniteSpringTestSuite {
}
