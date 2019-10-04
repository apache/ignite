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
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
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
public class CacheOperationPermissionCheckTest extends AbstractSecurityTest {
    /** New cache for tests. */
    protected static final String NEW_CACHE = "NEW_CACHE";

    /** Cache name for tests. */
    protected static final String ALL_PERM_TEST_CACHE = "ALL_PERM_TEST_CACHE";

    /** Forbidden caches. */
    protected static final String CREATE_TEST_CACHE = "CREATE_TEST_CACHE";
    protected static final String EMPTY_PERM_TEST_CACHE = "EMPTY_PERM_TEST_CACHE";

    /**
     *
     */
    @Test
    public void testServerCrudCacheNode() throws Exception {
        testCrudCachePermissions(false);
    }

    /**
     *
     */
    @Test
    public void testClientCrudCacheNode() throws Exception {
        testCrudCachePermissions(true);
    }

    /**
     *
     */
    @Test
    public void testServerCrudCacheSystemNode() throws Exception {
        testCrudCacheSystemPermissions(false);
    }

    /**
     *
     */
    @Test
    public void testClientCrudCacheSystemNode() throws Exception {
        testCrudCacheSystemPermissions(true);
    }

    /**
     * @param isClient True if is client mode.
     * @throws Exception If failed.
     */
    private void testCrudCachePermissions(boolean isClient) throws Exception {
        String login = isClient ? "client" : "server";
        Ignite node = startGrid(login + "_test_node",
            getSecurityPermissionSet(JOIN_AS_SERVER),
            isClient);

        node.createCache(NEW_CACHE); // if defaultAllowAll == false, there will be exeption
        node.createCache(ALL_PERM_TEST_CACHE);
        node.createCache(CREATE_TEST_CACHE);

        assertThrowsWithCause(() -> node.createCache(EMPTY_PERM_TEST_CACHE), SecurityException.class);

        checkOperations(node);

        assertThrowsWithCause(() -> node.cache(CREATE_TEST_CACHE).destroy(), SecurityException.class);
    }

    private void testCrudCacheSystemPermissions(boolean isClient) throws Exception {
        String login = isClient ? "client" : "server";
        Ignite node = startGrid(login + "_test_node",
            getSecurityPermissionSet(JOIN_AS_SERVER, CACHE_CREATE, CACHE_DESTROY),
            isClient);

        node.createCache(NEW_CACHE);
        node.createCache(ALL_PERM_TEST_CACHE);
        node.createCache(CREATE_TEST_CACHE);
        node.createCache(EMPTY_PERM_TEST_CACHE);

        checkOperations(node);

        node.cache(CREATE_TEST_CACHE).destroy();
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

    protected void checkOperations(Ignite node) {
        for (Consumer<IgniteCache<String, String>> c : operations()) {
            c.accept(node.cache(ALL_PERM_TEST_CACHE));
            c.accept(node.cache(NEW_CACHE)); // if defaultAllowAll == false, there will be exeption

            assertThrowsWithCause(() -> c.accept(node.cache(CREATE_TEST_CACHE)), SecurityException.class);
        }
    }

    protected SecurityPermissionSet getSecurityPermissionSet(SecurityPermission... systemPerms) {
        return SecurityPermissionSetBuilder.create() // defaultAllowAll == true
            .appendCachePermissions(ALL_PERM_TEST_CACHE, CACHE_READ, CACHE_PUT, CACHE_REMOVE, CACHE_CREATE, CACHE_DESTROY)
            .appendCachePermissions(CREATE_TEST_CACHE, CACHE_CREATE)
            .appendCachePermissions(EMPTY_PERM_TEST_CACHE, EMPTY_PERMS)
            .appendSystemPermissions(systemPerms)
            .build();
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
}
