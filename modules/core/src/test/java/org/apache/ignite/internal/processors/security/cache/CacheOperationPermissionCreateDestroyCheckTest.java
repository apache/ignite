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

package org.apache.ignite.internal.processors.security.cache;

import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.plugin.security.SecurityPermission.*;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Test create and destroy cache permissions.
 */
@RunWith(Parameterized.class)
public class CacheOperationPermissionCreateDestroyCheckTest extends AbstractSecurityTest {
    /** Cache name. */
    private static final String TEST_CACHE = "TEST_CACHE";

    /** Unallowed cache. */
    private static final String UNALLOWED_CACHE = "UNALLOWED";

    /** Server node name. */
    private static final String SRV = "srv";

    /** Forbidden server node name. */
    private static final String FORBIDDEN_SRV = "forbidden_srv";

    /** Forbidden client node name. */
    private static final String FORBIDDEN_CLNT = "forbidden_clnt";

    /** Test node. */
    private static final String TEST_NODE = "test_node";

    /** Parameters. */
    @Parameterized.Parameters(name = "clientMode={0}")
    public static Iterable<Boolean[]> data() {
        return Arrays.asList(new Boolean[] {true}, new Boolean[] {false});
    }

    /** Client mode. */
    @Parameterized.Parameter()
    public boolean clientMode;

    /** */
    @Test
    public void createCacheWithCachePermissions() throws Exception {
        SecurityPermissionSet secPermSet = builder()
            .appendCachePermissions(TEST_CACHE, CACHE_CREATE)
            .build();

        try (Ignite node = startGrid(TEST_NODE, secPermSet, clientMode)) {
            assertThrowsWithCause(() -> node.createCache(UNALLOWED_CACHE), SecurityException.class);

            assertNull(grid(SRV).cache(UNALLOWED_CACHE));

            assertNotNull(node.createCache(TEST_CACHE));
        }
    }

    /** */
    @Test
    public void destroyCacheWithCachePermissions() throws Exception {
        SecurityPermissionSet secPermSet = builder()
            .appendCachePermissions(TEST_CACHE, CACHE_DESTROY)
            .build();

        grid(SRV).createCache(TEST_CACHE);
        grid(SRV).createCache(UNALLOWED_CACHE);

        try (Ignite node = startGrid(TEST_NODE, secPermSet, clientMode)) {
            node.destroyCache(TEST_CACHE);

            assertThrowsWithCause(() -> node.destroyCache(UNALLOWED_CACHE), SecurityException.class);

            assertNull(grid(SRV).cache(TEST_CACHE));

            assertNotNull(grid(SRV).cache(UNALLOWED_CACHE));
        }
    }

    /** */
    @Test
    public void createCacheWithSystemPermissions() throws Exception {
        SecurityPermissionSetBuilder builder = builder()
            .appendSystemPermissions(CACHE_CREATE);

        try (Ignite node = startGrid(TEST_NODE, builder.build(), clientMode)) {

            assertThrowsWithCause(() -> forbidden(clientMode).createCache(TEST_CACHE), SecurityException.class);

            assertNotNull(node.createCache(TEST_CACHE));
        }
    }

    /** */
    @Test
    public void destroyCacheWithSystemPermissions() throws Exception {
        SecurityPermissionSet securityPermissionSet = builder()
        .appendSystemPermissions(CACHE_DESTROY).build();

        grid(SRV).createCache(TEST_CACHE);

        try (Ignite node = startGrid(TEST_NODE, securityPermissionSet, clientMode)) {

            assertThrowsWithCause(() -> forbidden(clientMode).destroyCache(TEST_CACHE), SecurityException.class);

            node.destroyCache(TEST_CACHE);
        }

        assertNull(grid(SRV).cache(TEST_CACHE));
    }

    /** */
    private SecurityPermissionSetBuilder builder() {
        return SecurityPermissionSetBuilder.create()
            .defaultAllowAll(false)
            .appendSystemPermissions(JOIN_AS_SERVER);
    }

    /**
     * @param isClnt Is client.
     */
    private Ignite forbidden(boolean isClnt) {
        return isClnt ? grid(FORBIDDEN_CLNT) : grid(FORBIDDEN_SRV);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridAllowAll(SRV);

        startGrid(FORBIDDEN_CLNT, builder().build(), true);

        startGrid(FORBIDDEN_SRV, builder().build(), false);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        Ignite server = grid(SRV);

        server.cacheNames().forEach(server::destroyCache);
    }
}
