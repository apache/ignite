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
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.security.AbstractCacheOperationPermissionCheckTest;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static java.util.Collections.singletonMap;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_DESTROY;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_PUT;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_REMOVE;
import static org.apache.ignite.plugin.security.SecurityPermission.JOIN_AS_SERVER;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Test CRUD cache permissions.
 */
@RunWith(JUnit4.class)
public class CacheOperationPermissionCheckTest extends AbstractCacheOperationPermissionCheckTest {
    /** New cache. */
    protected static final String NEW_CACHE = "NEW_CACHE";

    /** Cache with all permission. */
    protected static final String ALL_PERM_TEST_CACHE = "ALL_PERM_TEST_CACHE";

    /** Cache only with CACHE_CREATE permission. */
    protected static final String CREATE_PERM_TEST_CACHE = "CREATE_TEST_CACHE";

    /** Cache without permission. */
    protected static final String EMPTY_PERM_TEST_CACHE = "EMPTY_PERM_TEST_CACHE";

    /**
     *
     */
    @Test
    public void testCrudWithCachePermissionsOnServerNode() throws Exception {
        testCrudWithCachePermissions(false);
    }

    /**
     *
     */
    @Test
    public void testCrudWithCachePermissionsOnClientNode() throws Exception {
        testCrudWithCachePermissions(true);
    }

    /**
     *
     */
    @Test
    public void testCrudWithSystemPermissionsOnServerNode() throws Exception {
        testCrudWithSystemPermissions(false);
    }

    /**
     *
     */
    @Test
    public void testCrudWithSystemPermissionsOnClientNode() throws Exception {
        testCrudWithSystemPermissions(true);
    }

    /**
     * @param isClient True if is client mode.
     * @throws Exception If failed.
     */
    private void testCrudWithCachePermissions(boolean isClient) throws Exception {
        Ignite node = startGrid(loginPrefix(isClient) + "_test_node",
            SecurityPermissionSetBuilder.create() // defaultAllowAll is true.
                .appendCachePermissions(ALL_PERM_TEST_CACHE, CACHE_READ, CACHE_PUT, CACHE_REMOVE, CACHE_CREATE, CACHE_DESTROY)
                .appendCachePermissions(CREATE_PERM_TEST_CACHE, CACHE_CREATE)
                .appendCachePermissions(EMPTY_PERM_TEST_CACHE, EMPTY_PERMS)
                .appendSystemPermissions(JOIN_AS_SERVER)
                .build(),
            isClient);

        node.createCache(NEW_CACHE); // This won't fail since defaultAllowAll is true.
        node.createCache(ALL_PERM_TEST_CACHE);
        node.createCache(CREATE_PERM_TEST_CACHE);
        assertThrowsWithCause(() -> node.createCache(EMPTY_PERM_TEST_CACHE), SecurityException.class);

        checkOperations(node);

        node.cache(NEW_CACHE).destroy();
        node.cache(ALL_PERM_TEST_CACHE).destroy();
        assertThrowsWithCause(() -> node.cache(CREATE_PERM_TEST_CACHE).destroy(), SecurityException.class);
        assertThrowsWithCause(() -> node.cache(EMPTY_PERM_TEST_CACHE).destroy(), NullPointerException.class);
    }

    /**
     * @param isClient Is client.
     * @throws Exception If failed.
     */
    private void testCrudWithSystemPermissions(boolean isClient) throws Exception {
        Ignite node = startGrid(loginPrefix(isClient) + "_test_node",
            SecurityPermissionSetBuilder.create() // defaultAllowAll is true.
                .appendCachePermissions(ALL_PERM_TEST_CACHE, CACHE_READ, CACHE_PUT, CACHE_REMOVE, CACHE_CREATE, CACHE_DESTROY)
                .appendCachePermissions(CREATE_PERM_TEST_CACHE, CACHE_CREATE)
                .appendCachePermissions(EMPTY_PERM_TEST_CACHE, EMPTY_PERMS)
                .appendSystemPermissions(JOIN_AS_SERVER, CACHE_CREATE, CACHE_DESTROY)
                .build(),
            isClient);

        node.createCache(NEW_CACHE);
        node.createCache(ALL_PERM_TEST_CACHE);
        node.createCache(CREATE_PERM_TEST_CACHE);
        node.createCache(EMPTY_PERM_TEST_CACHE);

        checkOperations(node);

        node.cache(NEW_CACHE).destroy();
        node.cache(ALL_PERM_TEST_CACHE).destroy();
        node.cache(EMPTY_PERM_TEST_CACHE).destroy();
        node.cache(CREATE_PERM_TEST_CACHE).destroy();
    }

    /**
     * @return Collection of operations to invoke a cache operation.
     */
    private List<Consumer<IgniteCache<String, String>>> operations() {
        return Arrays.asList(
            c -> c.put("key", "value"),
            c -> c.putAll(singletonMap("key", "value")),
            c -> c.get("key"),
            c -> c.getAll(Collections.singleton("key")),
            c -> c.containsKey("key"),
            c -> c.remove("key"),
            c -> c.removeAll(Collections.singleton("key")),
            c -> c.replace("key", "value"),
            c -> c.putIfAbsent("key", "value"),
            c -> c.getAndPut("key", "value"),
            c -> c.getAndRemove("key"),
            c -> c.getAndReplace("key", "value"),
            IgniteCache::clear
        );
    }

    /**
     * @param node Node.
     */
    protected void checkOperations(Ignite node) {
        for (Consumer<IgniteCache<String, String>> c : operations()) {
            c.accept(node.cache(ALL_PERM_TEST_CACHE));
            c.accept(node.cache(NEW_CACHE)); // If defaultAllowAll is false, there will be exeption.

            assertThrowsWithCause(() -> c.accept(node.cache(CREATE_PERM_TEST_CACHE)), SecurityException.class);
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid("server",
            SecurityPermissionSetBuilder.create()
                .defaultAllowAll(false).build(),
            false)
            .cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration[] getCacheConfigurations() {
        return new CacheConfiguration[]{};
    }
}
