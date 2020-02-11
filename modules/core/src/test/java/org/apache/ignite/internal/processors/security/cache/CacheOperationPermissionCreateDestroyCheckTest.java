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

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
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
    public void testCreateWithCachePermissionsOnServerNode() throws Exception {
        testCreateWithCachePermissions(false);
    }

    /** */
    @Test
    public void testDestroyWithCachePermissionsOnServerNode() throws Exception {
        testDestroyWithCachePermissions(false);
    }

    /** */
    @Test
    public void testCreateWithCachePermissionsOnClientNode() throws Exception {
        testCreateWithCachePermissions(true);
    }

    /** */
    @Test
    public void testDestroyWithCachePermissionsOnClientNode() throws Exception {
        testDestroyWithCachePermissions(true);
    }

    /** */
    @Test
    public void testCreateWithSystemPermissionsOnServerNode() throws Exception {
        testCreateWithSystemPermissions(false);
    }

    /** */
    @Test
    public void testCreateWithSystemPermissionsOnClientNode() throws Exception {
        testCreateWithSystemPermissions(true);
    }

    /** */
    @Test
    public void testDestroyWithSystemPermissionsOnServerNode() throws Exception {
        testDestroyWithSystemPermissions(false);
    }

    /** */
    @Test
    public void testDestroyWithSystemPermissionsOnClientNode() throws Exception {
        testDestroyWithSystemPermissions(true);
    }

    /**
     * @param isClient Is client.
     * @throws Exception If failed.
     */
    private void testCreateWithCachePermissions(boolean isClient) throws Exception {
        SecurityPermissionSet secPermSet = SecurityPermissionSetBuilder.create()
            .appendSystemPermissions(JOIN_AS_SERVER)
            .appendCachePermissions(TEST_CACHE, CACHE_CREATE)
            .appendCachePermissions(FORBIDDEN_CACHE, EMPTY_PERMS)
            .build();

        Ignite node = startGrid(secPermSet, isClient);

        node.createCache(TEST_CACHE);

        // This won't fail since defaultAllowAll is true.
        node.createCache(NEW_TEST_CACHE);

        assertThrowsWithCause(() -> node.createCache(FORBIDDEN_CACHE), SecurityException.class);
    }

    /**
     * @param isClient Is client.
     * @throws Exception If failed.
     */
    private void testDestroyWithCachePermissions(boolean isClient) throws Exception {
        SecurityPermissionSet secPermSet = SecurityPermissionSetBuilder.create()
            .appendSystemPermissions(JOIN_AS_SERVER)
            .appendCachePermissions(TEST_CACHE, CACHE_CREATE, CACHE_DESTROY)
            .appendCachePermissions(FORBIDDEN_CACHE, CACHE_CREATE)
            .build();

        Ignite node = startGrid(secPermSet, isClient);

        node.createCache(TEST_CACHE);
        node.cache(TEST_CACHE).destroy();

        // This won't fail since defaultAllowAll is true.
        node.createCache(NEW_TEST_CACHE);
        node.cache(NEW_TEST_CACHE).destroy();

        node.createCache(FORBIDDEN_CACHE);
        assertThrowsWithCause(() -> node.cache(FORBIDDEN_CACHE).destroy(), SecurityException.class);
    }

    /**
     * @param isClient Is client.
     * @throws Exception If failed.
     */
    private void testCreateWithSystemPermissions(boolean isClient) throws Exception {
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
    private void testDestroyWithSystemPermissions(boolean isClient) throws Exception {
        SecurityPermissionSetBuilder builder = SecurityPermissionSetBuilder.create()
            .appendSystemPermissions(JOIN_AS_SERVER, CACHE_CREATE)
            .appendCachePermissions(TEST_CACHE, EMPTY_PERMS)
            .appendCachePermissions(FORBIDDEN_CACHE, EMPTY_PERMS);

        Ignite node = startGrid(builder.build(), isClient);

        node.createCache(TEST_CACHE);
        node.createCache(NEW_TEST_CACHE);
        node.createCache(FORBIDDEN_CACHE);

        // This won't fail since defaultAllowAll is true.
        node.cache(NEW_TEST_CACHE).destroy();

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
        return startGrid(loginPrefix(isClient) + "_test_node", secPermSet, null, isClient);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGridAllowAll("server").cluster().state(ACTIVE);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @param isClient Is client.
     */
    protected String loginPrefix(boolean isClient) {
        return isClient ? "client" : "server";
    }
}
