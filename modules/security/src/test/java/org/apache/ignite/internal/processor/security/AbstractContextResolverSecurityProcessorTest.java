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

package org.apache.ignite.internal.processor.security;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 *
 */
public class AbstractContextResolverSecurityProcessorTest extends GridCommonAbstractTest {
    /** Cache name for tests. */
    protected static final String CACHE_NAME = "TEST_CACHE";

    /** Cache name for tests. */
    protected static final String SEC_CACHE_NAME = "SECOND_TEST_CACHE";

    /** Values. */
    protected AtomicInteger values = new AtomicInteger(0);

    /** */
    protected IgniteEx succsessSrv;

    /** */
    protected IgniteEx succsessClnt;

    /** */
    protected IgniteEx failSrv;

    /** */
    protected IgniteEx failClnt;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        setupSecurityPermissions();

        System.setProperty(
            TestSecurityProcessorProvider.TEST_SECURITY_PROCESSOR_CLS,
            "org.apache.ignite.internal.processor.security.TestSecurityProcessor"
        );

        succsessSrv = startGrid("success_server");

        succsessClnt = startGrid(getConfiguration("success_client").setClientMode(true));

        failSrv = startGrid("fail_server");

        failClnt = startGrid(getConfiguration("fail_client").setClientMode(true));

        grid("success_server").cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        System.clearProperty(TestSecurityProcessorProvider.TEST_SECURITY_PROCESSOR_CLS);

        stopAllGrids();

        SecurityPermissionProvider.clear();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        Map<String, Object> attrs = new HashMap<>();

        attrs.put(TestSecurityProcessor.USER_SECURITY_TOKEN, igniteInstanceName);

        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration().setPersistenceEnabled(true)
                    )
            )
            .setAuthenticationEnabled(true)
            .setUserAttributes(attrs)
            .setCacheConfiguration(getCacheConfigurations());
    }

    /**
     *
     */
    private void setupSecurityPermissions(){
        SecurityPermissionProvider.add(
            SecurityPermissionSetBuilder.create()
                .defaultAllowAll(true)
                .build(), "success_server", "success_client"
        );
        SecurityPermissionProvider.add(
            SecurityPermissionSetBuilder.create()
                .defaultAllowAll(true)
                .appendCachePermissions(CACHE_NAME, SecurityPermission.CACHE_READ)
                .build(), "fail_server", "fail_client"
        );
    }

    /**
     * Getting array of cache configurations.
     */
    protected CacheConfiguration[] getCacheConfigurations() {
        return new CacheConfiguration[] {
            new CacheConfiguration<>()
                .setName(CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setReadFromBackup(false),
            new CacheConfiguration<>()
                .setName(SEC_CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setReadFromBackup(false)
        };
    }

    /**
     * Getting the key that is contained on primary partition on passed node.
     *
     * @param ignite Node.
     * @return Key.
     */
    protected Integer primaryKey(IgniteEx ignite) {
        Affinity<Integer> affinity = ignite.affinity(SEC_CACHE_NAME);

        int i = 0;
        do {
            if (affinity.isPrimary(ignite.localNode(), ++i))
                return i;

        }
        while (i <= 1_000);

        throw new IllegalStateException(ignite.name() + " isn't primary node for any key.");
    }

    /**
     * Assert that the passed throwable contains a cause exception with given type and message.
     *
     * @param throwable Throwable.
     */
    protected void assertCauseMessage(Throwable throwable) {
        assertThat(X.cause(throwable, SecurityException.class), notNullValue());
    }
}
