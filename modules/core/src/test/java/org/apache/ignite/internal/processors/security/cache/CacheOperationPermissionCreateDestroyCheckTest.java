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
    /** New test cache. */
    private static final String NEW_TEST_CACHE = "NEW_CACHE";

    /** Cache name. */
    private static final String TEST_CACHE = "TEST_CACHE";

    /** Forbidden cache. */
    private static final String FORBIDDEN_CACHE = "FORBIDDEN_CACHE";

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

        Ignite node = startGrid(secPermSet, isClient);

        node.createCache(TEST_CACHE);

        assertThrowsWithCause(() -> node.createCache(FORBIDDEN_CACHE), SecurityException.class);
    }

    /**
     * @param isClient Is client.
     * @throws Exception If failed.
     */
    private void destroyCacheWithCachePermissions(boolean isClient) throws Exception {
        SecurityPermissionSet secPermSet = SecurityPermissionSetBuilder.create()
            .appendSystemPermissions(JOIN_AS_SERVER)
            .appendCachePermissions(TEST_CACHE, CACHE_CREATE, CACHE_DESTROY)
            .appendCachePermissions(FORBIDDEN_CACHE, CACHE_CREATE)
            .build();

        Ignite node = startGrid(secPermSet, isClient);

        node.createCache(TEST_CACHE);
        node.cache(TEST_CACHE).destroy();

        node.createCache(FORBIDDEN_CACHE);
        assertThrowsWithCause(() -> node.cache(FORBIDDEN_CACHE).destroy(), SecurityException.class);
    }

    /**
     * @param isClient Is client.
     * @throws Exception If failed.
     */
    private void createCacheWithSystemPermissions(boolean isClient) throws Exception {
        SecurityPermissionSetBuilder builder = SecurityPermissionSetBuilder.create()
            .appendSystemPermissions(JOIN_AS_SERVER)
            .appendCachePermissions(TEST_CACHE, EMPTY_PERMS)
            .appendCachePermissions(FORBIDDEN_CACHE, EMPTY_PERMS);

        Ignite node = startGrid(builder.build(), isClient);

        assertThrowsWithCause(() -> node.createCache(TEST_CACHE), SecurityException.class);
        assertThrowsWithCause(() -> node.createCache(FORBIDDEN_CACHE), SecurityException.class);

        node.close();

        builder.appendSystemPermissions(CACHE_CREATE);

        Ignite node1 = startGrid(builder.build(), isClient);

        // This won't fail becouse CACHE_CREATE is in systemPermissions.
        node1.createCache(NEW_TEST_CACHE);
        node1.createCache(FORBIDDEN_CACHE);
    }

    /**
     * @param isClient Is client.
     * @throws Exception If failed.
     */
    private void destroyCacheWithSystemPermissions(boolean isClient) throws Exception {
        SecurityPermissionSetBuilder builder = SecurityPermissionSetBuilder.create()
            .appendSystemPermissions(JOIN_AS_SERVER, CACHE_CREATE)
            .appendCachePermissions(TEST_CACHE, EMPTY_PERMS)
            .appendCachePermissions(FORBIDDEN_CACHE, EMPTY_PERMS);

        Ignite node = startGrid(builder.build(), isClient);

        node.createCache(TEST_CACHE);
        node.createCache(NEW_TEST_CACHE);
        node.createCache(FORBIDDEN_CACHE);

        assertThrowsWithCause(() -> node.cache(TEST_CACHE).destroy(), SecurityException.class);
        assertThrowsWithCause(() -> node.cache(FORBIDDEN_CACHE).destroy(), SecurityException.class);

        node.close();

        builder.appendSystemPermissions(CACHE_DESTROY);

        Ignite node1 = startGrid(builder.build(), isClient);

        // This won't fail becouse CACHE_DESTROY is in systemPermissions.
        node1.cache(TEST_CACHE).destroy();
        node1.cache(FORBIDDEN_CACHE).destroy();
    }

    /**
     * Starts new grid with given SecurityPermissionSet.
     *
     * @param secPermSet Sec perm set.
     * @param isClient Is client.
     * @return Started grid.
     * @throws Exception If anything failed.
     */
    private Ignite startGrid(SecurityPermissionSet secPermSet, boolean isClient) throws Exception {
        return startGrid("test_node", secPermSet, null, isClient);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGridAllowAll("server");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }
}
