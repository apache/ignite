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

import java.security.Permissions;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePermission;
import org.apache.ignite.compute.ComputePermission;
import org.apache.ignite.internal.processors.security.AbstractCacheOperationPermissionCheckTest;
import org.apache.ignite.plugin.security.SecurityException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static java.util.Collections.singletonMap;
import static org.apache.ignite.internal.processors.security.IgniteSecurityConstants.JOIN_AS_SERVER;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Test CRUD cache permissions.
 */
@RunWith(JUnit4.class)
public class CacheOperationPermissionCheckTest extends AbstractCacheOperationPermissionCheckTest {
    /** */
    @Test
    public void testServerNode() throws Exception {
        testCrudCachePermissions(false);
    }

    /** */
    @Test
    public void testClientNode() throws Exception {
        testCrudCachePermissions(true);
    }

    /**
     * @param isClient True if is client mode.
     * @throws Exception If failed.
     */
    private void testCrudCachePermissions(boolean isClient) throws Exception {
        Permissions perms = new Permissions();

        perms.add(JOIN_AS_SERVER);
        perms.add(new ComputePermission("org.apache.ignite.internal.*", "execute"));
        perms.add(new CachePermission("*", "create"));
        perms.add(new CachePermission(CACHE_NAME, "get,put,remove"));

        Ignite node = startGrid(loginPrefix(isClient) + "_test_node", perms, isClient);

        for (Consumer<IgniteCache<String, String>> c : operations()) {
            c.accept(node.cache(CACHE_NAME));

            assertThrowsWithCause(() -> c.accept(node.cache(FORBIDDEN_CACHE)), SecurityException.class);
        }
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
            IgniteCache::clear,
            c -> c.replace("key", "value"),
            c -> c.putIfAbsent("key", "value"),
            c -> c.getAndPut("key", "value"),
            c -> c.getAndRemove("key"),
            c -> c.getAndReplace("key", "value")
        );
    }
}
