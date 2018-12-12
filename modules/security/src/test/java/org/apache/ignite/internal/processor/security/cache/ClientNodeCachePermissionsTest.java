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
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.processor.security.AbstractCachePermissionTest;

import static java.util.Collections.singletonMap;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_PUT;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_REMOVE;

/**
 * Test CRUD cache permissions for client node.
 */
public class ClientNodeCachePermissionsTest extends AbstractCachePermissionTest {
    /**
     *
     */
    public void testCrudCachePermissions() throws Exception {
        Ignite node = startGrid("test_node",
            builder().defaultAllowAll(true)
                .appendCachePermissions(CACHE_NAME, CACHE_READ, CACHE_PUT, CACHE_REMOVE)
                .appendCachePermissions(FORBIDDEN_CACHE, EMPTY_PERMS).build(), isClient());

        node.cache(CACHE_NAME).put("key", "value");
        forbiddenRun(() -> node.cache(FORBIDDEN_CACHE).put("key", "value"));

        node.cache(CACHE_NAME).putAll(singletonMap("key", "value"));
        forbiddenRun(() -> node.cache(FORBIDDEN_CACHE).putAll(singletonMap("key", "value")));

        node.cache(CACHE_NAME).get("key");
        forbiddenRun(() -> node.cache(FORBIDDEN_CACHE).get("key"));

        node.cache(CACHE_NAME).getAll(Collections.singleton("key"));
        forbiddenRun(() -> node.cache(FORBIDDEN_CACHE).getAll(Collections.singleton("key")));

        node.cache(CACHE_NAME).containsKey("key");
        forbiddenRun(() -> node.cache(FORBIDDEN_CACHE).containsKey("key"));

        node.cache(CACHE_NAME).remove("key");
        forbiddenRun(() -> node.cache(FORBIDDEN_CACHE).remove("key"));

        node.cache(CACHE_NAME).removeAll(Collections.singleton("key"));
        forbiddenRun(() -> node.cache(FORBIDDEN_CACHE).removeAll(Collections.singleton("key")));

        node.cache(CACHE_NAME).clear();
        forbiddenRun(() -> node.cache(FORBIDDEN_CACHE).clear());

        node.cache(CACHE_NAME).replace("key", "value");
        forbiddenRun(() -> node.cache(FORBIDDEN_CACHE).replace("key", "value"));

        node.cache(CACHE_NAME).putIfAbsent("key", "value");
        forbiddenRun(() -> node.cache(FORBIDDEN_CACHE).putIfAbsent("key", "value"));

        node.cache(CACHE_NAME).getAndPut("key", "value");
        forbiddenRun(() -> node.cache(FORBIDDEN_CACHE).getAndPut("key", "value"));

        node.cache(CACHE_NAME).getAndRemove("key");
        forbiddenRun(() -> node.cache(FORBIDDEN_CACHE).getAndRemove("key"));

        node.cache(CACHE_NAME).getAndReplace("key", "value");
        forbiddenRun(() -> node.cache(FORBIDDEN_CACHE).getAndReplace("key", "value"));
    }
}
