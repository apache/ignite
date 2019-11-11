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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Collections.singletonMap;
import static org.apache.ignite.plugin.security.SecurityPermission.*;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Test create and destroy cache permissions.
 */
@RunWith(JUnit4.class)
public class CacheOperationPermissionCreateDestroyCheckTest extends AbstractSecurityTest {
    /** New test cache. */
    protected static final String NEW_TEST_CACHE = "NEW_CACHE";
    /** Cache name. */
    protected static final String TEST_CACHE = "TEST_CACHE";
    /** Forbidden cache. */
    protected static final String FORBIDDEN_CACHE = "FORBIDDEN_CACHE";

    /** */
    @Test
    public void testCreateDestroyCachePermissionsOnServerNode() throws Exception {
        testCreateDestroyCachePermissions(false);
    }

    /** */
    @Test
    public void testCreateDestroyCachePermissionsOnClientNode() throws Exception {
        testCreateDestroyCachePermissions(true);
    }

    /** */
    @Test
    public void testCreateDestroysystemPermissionsOnServerNode() throws Exception {
        testCreateDestroySystemPermissions(false);
    }

    /** */
    @Test
    public void testCreateDestroySystemPermissionsOnClientNode() throws Exception {
        testCreateDestroySystemPermissions(true);
    }

    /**
     * @param isClient Is client.
     * @throws Exception If failed.
     */
    private void testCreateDestroyCachePermissions(boolean isClient) throws Exception {
        Ignite node = startGrid(loginPrefix(isClient) + "_test_node",
            SecurityPermissionSetBuilder.create()
                    .defaultAllowAll(true)
                .appendCachePermissions(TEST_CACHE, CACHE_CREATE, CACHE_DESTROY)
                .appendCachePermissions(FORBIDDEN_CACHE, EMPTY_PERMS)
                .appendSystemPermissions(JOIN_AS_SERVER)
                .build(),
            isClient);

        // This won't fail since defaultAllowAll is true.
        node.createCache(NEW_TEST_CACHE);
        node.cache(NEW_TEST_CACHE).destroy();

        node.createCache(TEST_CACHE);
        node.cache(TEST_CACHE).destroy();

        assertThrowsWithCause(() -> node.createCache(FORBIDDEN_CACHE), SecurityException.class);
        assertThrowsWithCause(() -> node.cache(FORBIDDEN_CACHE).destroy(), NullPointerException.class);
    }

    /**
     * @param isClient Is client.
     * @throws Exception If failed.
     */
    private void testCreateDestroySystemPermissions(boolean isClient) throws Exception {
        Ignite node = startGrid(loginPrefix(isClient) + "_test_node",
                SecurityPermissionSetBuilder.create()
                        .defaultAllowAll(false)
                        .appendCachePermissions(FORBIDDEN_CACHE, EMPTY_PERMS)
                        .appendSystemPermissions(JOIN_AS_SERVER, CACHE_CREATE, CACHE_DESTROY)
                        .build(),
                isClient);

        // This won't fail becouse CACHE_CREATE is in systemPermissions.
        node.createCache(NEW_TEST_CACHE);
        node.createCache(FORBIDDEN_CACHE);

        // This won't fail becouse CACHE_DESTROY is in systemPermissions.
        node.cache(NEW_TEST_CACHE).destroy();
        node.cache(FORBIDDEN_CACHE).destroy();
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

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGridAllowAll("server").cluster().active(true);
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
