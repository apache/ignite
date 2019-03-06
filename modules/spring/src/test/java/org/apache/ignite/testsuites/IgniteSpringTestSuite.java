/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.testsuites;

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

    //GridSpringCacheManagerMultiJvmSelfTest.class,
    GridSpringCacheManagerMultiJvmSelfTest.class,

    GridCommandLineLoaderTest.class,
})
public class IgniteSpringTestSuite {
}
