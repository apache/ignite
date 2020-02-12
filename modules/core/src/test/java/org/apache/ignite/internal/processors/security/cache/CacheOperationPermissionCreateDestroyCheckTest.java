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

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.plugin.security.SecurityPermission.*;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Test create and destroy cache permissions.
 */
@RunWith(JUnit4.class)
public class CacheOperationPermissionCreateDestroyCheckTest extends AbstractSecurityTest {
    /** Cache name. */
    private static final String TEST_CACHE = "TEST_CACHE";

    /** Forbidden cache. */
    private static final String FORBIDDEN_CACHE = "FORBIDDEN_CACHE";

    /** Server node name. */
    private static final String SERVER = "server";

    /** Forbid node name. */
    private static final String FORBID_NAME = "test_node";

    /** Allow node name. */
    private static final String ALLOW_NAME = "allow_test_node";

    /** */
    @Test
    public void testCreateCacheWithCachePermissionsOnServerNode() throws Exception {
        createCacheWithCachePermissions(false);
    }

    /** */
    @Test
    public void testDestroyCacheWithCachePermissionsOnServerNode() throws Exception {
        destroyCacheWithCachePermissions(false);
    }

    /** */
    @Test
    public void testCreateCacheWithCachePermissionsOnClientNode() throws Exception {
        createCacheWithCachePermissions(true);
    }

    /** */
    @Test
    public void testDestroyCacheWithCachePermissionsOnClientNode() throws Exception {
        destroyCacheWithCachePermissions(true);
    }

    /** */
    @Test
    public void testCreateCacheWithSystemPermissionsOnServerNode() throws Exception {
        createCacheWithSystemPermissions(false);
    }

    /** */
    @Test
    public void testCreateWithSystemPermissionsOnClientNode() throws Exception {
        createCacheWithSystemPermissions(true);
    }

    /** */
    @Test
    public void testDestroyCacheWithSystemPermissionsOnServerNode() throws Exception {
        destroyCacheWithSystemPermissions(false);
    }

    /** */
    @Test
    public void testDestroyCacheWithSystemPermissionsOnClientNode() throws Exception {
        destroyCacheWithSystemPermissions(true);
    }

    /**
     * @param isClient Is client.
     * @throws Exception If failed.
     */
    private void createCacheWithCachePermissions(boolean isClient) throws Exception {
        SecurityPermissionSet secPermSet = SecurityPermissionSetBuilder.create()
            .defaultAllowAll(false)
            .appendSystemPermissions(JOIN_AS_SERVER)
            .appendCachePermissions(TEST_CACHE, CACHE_CREATE)
            .appendCachePermissions(FORBIDDEN_CACHE, EMPTY_PERMS)
            .build();

        try(Ignite node = startGrid(ALLOW_NAME, secPermSet, isClient)) {
            node.createCache(TEST_CACHE);

            assertThrowsWithCause(() -> node.createCache(FORBIDDEN_CACHE), SecurityException.class);
        }
    }

    /**
     * @param isClient Is client.
     * @throws Exception If failed.
     */
    private void destroyCacheWithCachePermissions(boolean isClient) throws Exception {
        SecurityPermissionSet secPermSet = SecurityPermissionSetBuilder.create()
            .defaultAllowAll(false)
            .appendSystemPermissions(JOIN_AS_SERVER)
            .appendCachePermissions(TEST_CACHE, CACHE_CREATE, CACHE_DESTROY)
            .appendCachePermissions(FORBIDDEN_CACHE, CACHE_CREATE)
            .build();

        try(Ignite node = startGrid(ALLOW_NAME, secPermSet, isClient)) {
            node.createCache(TEST_CACHE);
            node.cache(TEST_CACHE).destroy();

            node.createCache(FORBIDDEN_CACHE);
            assertThrowsWithCause(() -> node.cache(FORBIDDEN_CACHE).destroy(), SecurityException.class);
        }
    }

    /**
     * @param isClient Is client.
     * @throws Exception If failed.
     */
    private void createCacheWithSystemPermissions(boolean isClient) throws Exception {
        SecurityPermissionSetBuilder builder = SecurityPermissionSetBuilder.create()
            .defaultAllowAll(false)
            .appendSystemPermissions(JOIN_AS_SERVER)
            .appendCachePermissions(TEST_CACHE, EMPTY_PERMS)
            .appendCachePermissions(FORBIDDEN_CACHE, EMPTY_PERMS);

        try(Ignite forbidden = startGrid(FORBID_NAME, builder.build(), isClient)) {
            assertThrowsWithCause(() -> forbidden.createCache(TEST_CACHE), SecurityException.class);
            assertThrowsWithCause(() -> forbidden.createCache(FORBIDDEN_CACHE), SecurityException.class);
        }

        builder.appendSystemPermissions(CACHE_CREATE);

        try(Ignite allow = startGrid(ALLOW_NAME, builder.build(), isClient)) {
            allow.createCache(FORBIDDEN_CACHE);
        }
    }

    /**
     * @param isClient Is client.
     * @throws Exception If failed.
     */
    private void destroyCacheWithSystemPermissions(boolean isClient) throws Exception {
        SecurityPermissionSetBuilder builder = SecurityPermissionSetBuilder.create()
            .defaultAllowAll(false)
            .appendSystemPermissions(JOIN_AS_SERVER, CACHE_CREATE)
            .appendCachePermissions(TEST_CACHE, EMPTY_PERMS)
            .appendCachePermissions(FORBIDDEN_CACHE, EMPTY_PERMS);

        try(Ignite forbidden = startGrid(FORBID_NAME, builder.build(), isClient)) {
            forbidden.createCache(TEST_CACHE);
            forbidden.createCache(FORBIDDEN_CACHE);

            assertThrowsWithCause(() -> forbidden.cache(TEST_CACHE).destroy(), SecurityException.class);
            assertThrowsWithCause(() -> forbidden.cache(FORBIDDEN_CACHE).destroy(), SecurityException.class);
        }

        builder.appendSystemPermissions(CACHE_DESTROY);

        try(Ignite allow = startGrid(ALLOW_NAME, builder.build(), isClient)) {
            allow.cache(TEST_CACHE).destroy();
            allow.cache(FORBIDDEN_CACHE).destroy();
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridAllowAll(SERVER);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        IgniteEx server = grid(SERVER);

        server.cacheNames().forEach(cacheName -> server.cache(cacheName).destroy());
    }
}
