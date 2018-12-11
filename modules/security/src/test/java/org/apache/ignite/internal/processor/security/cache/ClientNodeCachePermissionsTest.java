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

package org.apache.ignite.internal.processor.security.cache;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processor.security.AbstractSecurityTest;

import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_PUT;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_REMOVE;

/**
 * Test CRUD cache permissions for client node.
 */
public class ClientNodeCachePermissionsTest extends AbstractSecurityTest {
    /** Cache. */
    private static final String CACHE = "TEST_CACHE";

    /** Forbidden cache. */
    private static final String FORBIDDEN_CACHE = "FORBIDDEN_TEST_CACHE";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(
                new CacheConfiguration().setName(CACHE),
                new CacheConfiguration().setName(FORBIDDEN_CACHE)
            );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid("sever", allowAllPermissionSet()).cluster().active(true);
    }

    /**
     * @return Node client mode.
     */
    protected boolean isClient() {
        return true;
    }

    /**
     *
     */
    public void testCrudCachePermissions() throws Exception {
        Ignite node = startGrid("test_node",
            builder().defaultAllowAll(true)
                .appendCachePermissions(CACHE, CACHE_READ, CACHE_PUT, CACHE_REMOVE)
                .appendCachePermissions(FORBIDDEN_CACHE, EMPTY_PERMS).build(), isClient());

        node.cache(CACHE).put("key", "value");
        forbiddenRun(() -> node.cache(FORBIDDEN_CACHE).put("key", "value"));

        Map<String, String> map = new HashMap<>();

        map.put("key", "value");
        node.cache(CACHE).putAll(map);
        forbiddenRun(() -> node.cache(FORBIDDEN_CACHE).putAll(map));

        node.cache(CACHE).get("key");
        forbiddenRun(() -> node.cache(FORBIDDEN_CACHE).get("key"));

        node.cache(CACHE).getAll(Collections.singleton("key"));
        forbiddenRun(() -> node.cache(FORBIDDEN_CACHE).getAll(Collections.singleton("key")));

        node.cache(CACHE).containsKey("key");
        forbiddenRun(() -> node.cache(FORBIDDEN_CACHE).containsKey("key"));

        node.cache(CACHE).remove("key");
        forbiddenRun(() -> node.cache(FORBIDDEN_CACHE).remove("key"));

        node.cache(CACHE).removeAll(Collections.singleton("key"));
        forbiddenRun(() -> node.cache(FORBIDDEN_CACHE).removeAll(Collections.singleton("key")));

        node.cache(CACHE).clear();
        forbiddenRun(() -> node.cache(FORBIDDEN_CACHE).clear());

        node.cache(CACHE).replace("key", "value");
        forbiddenRun(() -> node.cache(FORBIDDEN_CACHE).replace("key", "value"));

        node.cache(CACHE).putIfAbsent("key", "value");
        forbiddenRun(() -> node.cache(FORBIDDEN_CACHE).putIfAbsent("key", "value"));

        node.cache(CACHE).getAndPut("key", "value");
        forbiddenRun(() -> node.cache(FORBIDDEN_CACHE).getAndPut("key", "value"));

        node.cache(CACHE).getAndRemove("key");
        forbiddenRun(() -> node.cache(FORBIDDEN_CACHE).getAndRemove("key"));

        node.cache(CACHE).getAndReplace("key", "value");
        forbiddenRun(() -> node.cache(FORBIDDEN_CACHE).getAndReplace("key", "value"));
    }
}
