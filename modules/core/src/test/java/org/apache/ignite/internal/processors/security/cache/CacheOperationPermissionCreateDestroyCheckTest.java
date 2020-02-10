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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
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

    /** Cache permissions. */
    private Map<String, SecurityPermission[]> cachePerms = new HashMap<>();

    /** Security permission set. */
    private Set<SecurityPermission> sysPermSet = new HashSet<>();

    /** */
    @Test
    public void testCreateCachePermissionsOnServerNode() throws Exception {
        testCreateCachePermissions(false);
    }

    /** */
    @Test
    public void testDestroyCachePermissionsOnServerNode() throws Exception {
        testDestroyCachePermissions(false);
    }

    /** */
    @Test
    public void testCreateCachePermissionsOnClientNode() throws Exception {
        testCreateCachePermissions(true);
    }

    /** */
    @Test
    public void testDestroyCachePermissionsOnClientNode() throws Exception {
        testDestroyCachePermissions(true);
    }

    /** */
    @Test
    public void testCreateSystemPermissionsOnServerNode() throws Exception {
        testCreateSystemPermissions(false);
    }

    /** */
    @Test
    public void testCreateSystemPermissionsOnClientNode() throws Exception {
        testCreateSystemPermissions(true);
    }

    /** */
    @Test
    public void testDestroySystemPermissionsOnServerNode() throws Exception {
        testDestroySystemPermissions(false);
    }

    /** */
    @Test
    public void testDestroySystemPermissionsOnClientNode() throws Exception {
        testDestroySystemPermissions(true);
    }

    /**
     * @param isClient Is client.
     * @throws Exception If failed.
     */
    private void testCreateCachePermissions(boolean isClient) throws Exception {
        cachePerms.put(TEST_CACHE, new SecurityPermission[] {CACHE_CREATE});
        cachePerms.put(FORBIDDEN_CACHE, EMPTY_PERMS);

        Ignite node = startGrid(loginPrefix(isClient) + "_test_node", isClient);

        node.createCache(TEST_CACHE);

        // This won't fail since defaultAllowAll is true.
        node.createCache(NEW_TEST_CACHE);

        assertThrowsWithCause(() -> node.createCache(FORBIDDEN_CACHE), SecurityException.class);
    }

    /**
     * @param isClient Is client.
     * @throws Exception If failed.
     */
    private void testDestroyCachePermissions(boolean isClient) throws Exception {
        cachePerms.put(TEST_CACHE, new SecurityPermission[] {CACHE_CREATE, CACHE_DESTROY});
        cachePerms.put(FORBIDDEN_CACHE, new SecurityPermission[] {CACHE_CREATE});

        Ignite node = startGrid(loginPrefix(isClient) + "_test_node", isClient);

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
    private void testCreateSystemPermissions(boolean isClient) throws Exception {
        cachePerms.put(TEST_CACHE, EMPTY_PERMS);
        cachePerms.put(FORBIDDEN_CACHE, EMPTY_PERMS);

        Ignite node = startGrid(loginPrefix(isClient) + "_test_node", isClient);

        assertThrowsWithCause(() -> node.createCache(TEST_CACHE), SecurityException.class);
        assertThrowsWithCause(() -> node.createCache(FORBIDDEN_CACHE), SecurityException.class);

        node.close();

        sysPermSet.add(CACHE_CREATE);

        Ignite node1 = startGrid(loginPrefix(isClient) + "_test_node", isClient);

        // This won't fail becouse CACHE_CREATE is in systemPermissions.
        node1.createCache(NEW_TEST_CACHE);
        node1.createCache(FORBIDDEN_CACHE);
    }

    /**
     * @param isClient Is client.
     * @throws Exception If failed.
     */
    private void testDestroySystemPermissions(boolean isClient) throws Exception {
        cachePerms.put(TEST_CACHE, EMPTY_PERMS);
        cachePerms.put(FORBIDDEN_CACHE, EMPTY_PERMS);

        sysPermSet.add(CACHE_CREATE);

        Ignite node = startGrid(loginPrefix(isClient) + "_test_node", isClient);

        node.createCache(TEST_CACHE);
        node.createCache(NEW_TEST_CACHE);
        node.createCache(FORBIDDEN_CACHE);

        // This won't fail since defaultAllowAll is true.
        node.cache(NEW_TEST_CACHE).destroy();

        assertThrowsWithCause(() -> node.cache(TEST_CACHE).destroy(), SecurityException.class);
        assertThrowsWithCause(() -> node.cache(FORBIDDEN_CACHE).destroy(), SecurityException.class);

        node.close();

        sysPermSet.add(CACHE_DESTROY);

        Ignite node1 = startGrid(loginPrefix(isClient) + "_test_node", isClient);

        // This won't fail becouse CACHE_DESTROY is in systemPermissions.
        node1.cache(TEST_CACHE).destroy();
        node1.cache(FORBIDDEN_CACHE).destroy();
    }

    /**
     * @param login Login.
     * @param isClient Is client.
     */
    protected IgniteEx startGrid(String login, boolean isClient) throws Exception {
        SecurityPermissionSetBuilder builder = SecurityPermissionSetBuilder.create();

        builder.defaultAllowAll(true);

        cachePerms.forEach((builder::appendCachePermissions));
        sysPermSet.forEach(builder::appendSystemPermissions);

        return startGrid(login, builder.build(), null, isClient);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        sysPermSet.clear();
        sysPermSet.add(JOIN_AS_SERVER);

        cachePerms.clear();

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
