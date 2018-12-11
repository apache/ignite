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

import java.util.function.Consumer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.plugin.security.SecurityException;

import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 *
 */
public class AbstractResolveSecurityContextTest extends AbstractSecurityTest {
    /** Sever node that has all permissions for TEST_CACHE. */
    protected IgniteEx srvAllPerms;

    /** Client node that has all permissions for TEST_CACHE. */
    protected IgniteEx clntAllPerms;

    /** Sever node that has read only permission for TEST_CACHE. */
    protected IgniteEx srvReadOnlyPerm;

    /** Client node that has read only permission for TEST_CACHE. */
    protected IgniteEx clntReadOnlyPerm;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        srvAllPerms = startGrid("srv_all_perms", allowAllPermissionSet());

        clntAllPerms = startGrid("clnt_all_perms", allowAllPermissionSet(), true);

        srvReadOnlyPerm = startGrid("srv_read_only_perm",
            builder().defaultAllowAll(true)
                .appendCachePermissions(CACHE_NAME, CACHE_READ).build());

        clntReadOnlyPerm = startGrid("clnt_read_only_perm",
            builder().defaultAllowAll(true)
                .appendCachePermissions(CACHE_NAME, CACHE_READ).build(), true);

        grid(0).cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration<>()
                .setName(CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setReadFromBackup(false));
    }

    /**
     * @param c Consumer.
     */
    protected void assertAllowed(Consumer<T2<String, Integer>> c) {
        assertAllowed(srvAllPerms, CACHE_NAME, c);
    }

    /**
     * @param c Consumer.
     */
    protected void assertForbidden(Consumer<T2<String, Integer>> c) {
        assertForbidden(srvAllPerms, CACHE_NAME, c);
    }

    /**
     * Assert that the passed throwable contains a cause exception with given type.
     *
     * @param throwable Throwable.
     */
    protected void assertCauseSecurityException(Throwable throwable) {
        assertThat(X.cause(throwable, SecurityException.class), notNullValue());
    }
}