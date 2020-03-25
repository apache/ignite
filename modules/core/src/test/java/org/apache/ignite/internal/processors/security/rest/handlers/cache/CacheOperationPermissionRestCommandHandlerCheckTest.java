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

package org.apache.ignite.internal.processors.security.rest.handlers.cache;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.cache.GridCacheCommandHandler;
import org.apache.ignite.internal.processors.rest.request.GridRestCacheRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.plugin.security.SecurityException;
import org.junit.Test;

import static java.util.Collections.singletonMap;
import static org.apache.ignite.plugin.security.SecurityPermission.*;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Test CRUD, create and destroy cache permissions with rest commands handler.
 */
public class CacheOperationPermissionRestCommandHandlerCheckTest extends GridCommonAbstractTest {
    /** Empty permission. */
    private static final SecurityPermission[] EMPTY_PERMS = new SecurityPermission[0];

    /** Cache name for tests. */
    private static final String CACHE_NAME = "TEST_CACHE";

    /** Forbidden cache. */
    private static final String FORBIDDEN_CACHE_NAME = "FORBIDDEN_TEST_CACHE";

    /** New cache. */
    private static final String NEW_TEST_CACHE = "NEW_TEST_CACHE";

    /** Key. */
    private String key = "key";

    /** Value. */
    private String val = "value";

    /** New value. */
    private String newVal = "newValue";

    /** Cache permissions. */
    private Map<String, SecurityPermission[]> cachePerms = new HashMap<>();

    /** Security permission set. */
    private Set<SecurityPermission> sysPermSet = new HashSet<>();

    /** Handler. */
    private GridRestCommandHandler hnd;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        SecurityPermissionSetBuilder builder = SecurityPermissionSetBuilder.create();

        builder.defaultAllowAll(false);

        cachePerms.forEach((builder::appendCachePermissions));
        sysPermSet.forEach(builder::appendSystemPermissions);

        cfg.setPluginProviders(
            new TestSecurityPluginProvider("login", "password", builder.build(), true));

        return cfg;
    }

    /** */
    @Test
    public void testCacheCreate() throws Exception {
        cachePerms.put(CACHE_NAME, new SecurityPermission[] {CACHE_CREATE});
        cachePerms.put(FORBIDDEN_CACHE_NAME, EMPTY_PERMS);

        startGrid(getConfiguration());

        checkCacheCreate();
    }

    /** */
    private void checkCacheCreate() throws Exception {
        assertTrue(grid().cacheNames().isEmpty());

        cacheCreate(CACHE_NAME);

        assertTrue(grid().cacheNames().contains(CACHE_NAME));

        if (sysPermSet.contains(CACHE_CREATE) || cachePermsContains(FORBIDDEN_CACHE_NAME, CACHE_CREATE)) {
            cacheCreate(FORBIDDEN_CACHE_NAME);
            assertTrue(grid().cacheNames().contains(FORBIDDEN_CACHE_NAME));
        }
        else {
            assertThrowsWithCause(() -> cacheCreate(FORBIDDEN_CACHE_NAME), IgniteCheckedException.class);

            assertFalse(grid().cacheNames().contains(FORBIDDEN_CACHE_NAME));
        }

        if (sysPermSet.contains(CACHE_CREATE)) {
            cacheCreate(NEW_TEST_CACHE);

            assertTrue(grid().cacheNames().contains(NEW_TEST_CACHE));
        }
        else {
            assertThrowsWithCause(() -> cacheCreate(NEW_TEST_CACHE), IgniteCheckedException.class);

            assertFalse(grid().cacheNames().contains(NEW_TEST_CACHE));
        }
    }

    /** */
    @Test
    public void testCacheCreateWithSystemPermissionsCacheCreate() throws Exception {
        sysPermSet.add(CACHE_CREATE);

        testCacheCreate();
    }

    /** */
    @Test
    public void testCachePut() throws Exception {
        cachePerms.put(CACHE_NAME, new SecurityPermission[] {CACHE_CREATE, CACHE_PUT});
        cachePerms.put(FORBIDDEN_CACHE_NAME, new SecurityPermission[] {CACHE_CREATE});

        startGrid(getConfiguration());

        checkCacheCreate();

        checkCachePut();
    }

    /** */
    private void checkCachePut() throws IgniteCheckedException {
        assertTrue(cachePutIfAbsent(CACHE_NAME, key, val));
        assertFalse(cachePutIfAbsent(CACHE_NAME, key, val));

        assertTrue(cachePut(CACHE_NAME, key, val));
        assertTrue(cachePutAll(CACHE_NAME, singletonMap(key, val)));
        assertTrue(cacheReplace(CACHE_NAME, key, val));

        if (cachePermsContains(FORBIDDEN_CACHE_NAME, CACHE_PUT))
            assertTrue(cachePut(FORBIDDEN_CACHE_NAME, key, val));
        else
            assertThrowsWithCause(() -> cachePut(FORBIDDEN_CACHE_NAME, key, val), IgniteCheckedException.class);

        assertThrowsWithCause(() -> cachePutIfAbsent(NEW_TEST_CACHE, key, val), IgniteCheckedException.class);
        assertThrowsWithCause(() -> cachePut(NEW_TEST_CACHE, key, val), IgniteCheckedException.class);
        assertThrowsWithCause(() -> cachePutAll(NEW_TEST_CACHE, singletonMap(key, val)),
            IgniteCheckedException.class);
        assertThrowsWithCause(() -> cacheReplace(NEW_TEST_CACHE, key, val), IgniteCheckedException.class);
    }

    /** */
    @Test
    public void testCacheRead() throws Exception {
        cachePerms.put(CACHE_NAME, new SecurityPermission[] {CACHE_CREATE, CACHE_PUT, CACHE_READ});
        cachePerms.put(FORBIDDEN_CACHE_NAME, new SecurityPermission[] {CACHE_CREATE, CACHE_PUT});

        startGrid(getConfiguration());

        checkCacheCreate();

        checkCachePut();

        assertTrue(cacheContainsKey(CACHE_NAME, key));

        assertEquals(cacheGet(CACHE_NAME, key), val);
        assertEquals(cacheGetAndPut(CACHE_NAME, key, newVal), val);

        assertTrue(cacheGetAll(CACHE_NAME, singletonMap(key, null)).containsValue(newVal));

        if (cachePermsContains(FORBIDDEN_CACHE_NAME, CACHE_READ))
            assertEquals(cacheGet(FORBIDDEN_CACHE_NAME, key), val);
        else
            assertThrowsWithCause(() -> cacheGet(FORBIDDEN_CACHE_NAME, key), SecurityException.class);

        assertThrowsWithCause(() -> cacheContainsKey(NEW_TEST_CACHE, key), IgniteCheckedException.class);
        assertThrowsWithCause(() -> cacheGet(NEW_TEST_CACHE, key), IgniteCheckedException.class);
    }

    /** */
    @Test
    public void testCacheRemove() throws Exception {
        cachePerms.put(CACHE_NAME, new SecurityPermission[] {CACHE_CREATE, CACHE_PUT, CACHE_READ, CACHE_REMOVE});
        cachePerms.put(FORBIDDEN_CACHE_NAME, new SecurityPermission[] {CACHE_CREATE, CACHE_PUT, CACHE_READ});

        startGrid(getConfiguration());

        checkCacheCreate();

        checkCachePut();

        assertTrue(grid().cache(CACHE_NAME).containsKey(key));

        cacheRemove(CACHE_NAME, key);

        cachePut(CACHE_NAME, key, newVal);

        assertEquals(cacheGetAndRemove(CACHE_NAME, key), newVal);

        assertFalse(grid().cache(CACHE_NAME).containsKey(key));

        cachePut(CACHE_NAME, key, val);

        assertTrue(cacheRemoveAll(CACHE_NAME, singletonMap(key, null)));

        assertTrue(grid().cache(FORBIDDEN_CACHE_NAME).containsKey(key));

        assertThrowsWithCause(() -> cacheRemove(FORBIDDEN_CACHE_NAME, key), SecurityException.class);
        assertThrowsWithCause(() -> cacheGetAndRemove(FORBIDDEN_CACHE_NAME, key), SecurityException.class);
    }

    /** */
    @Test
    public void testCacheDestroy() throws Exception {
        cachePerms.put(CACHE_NAME, new SecurityPermission[] {CACHE_CREATE, CACHE_DESTROY});
        cachePerms.put(FORBIDDEN_CACHE_NAME, new SecurityPermission[] {CACHE_CREATE});

        startGrid(getConfiguration());

        checkCacheCreate();

        cacheDestroy(CACHE_NAME);

        assertFalse(grid().cacheNames().contains(CACHE_NAME));

        if (sysPermSet.contains(CACHE_DESTROY)) {
            cacheDestroy(NEW_TEST_CACHE);

            assertFalse(grid().cacheNames().contains(NEW_TEST_CACHE));
        }
        else
            assertThrowsWithCause(() -> cacheDestroy(NEW_TEST_CACHE), SecurityException.class);

        if (sysPermSet.contains(CACHE_DESTROY)) {
            cacheDestroy(FORBIDDEN_CACHE_NAME);

            assertFalse(grid().cacheNames().contains(FORBIDDEN_CACHE_NAME));
        }
        else
            assertThrowsWithCause(() -> cacheDestroy(FORBIDDEN_CACHE_NAME), SecurityException.class);
    }

    /** */
    @Test
    public void testCacheDestroyWithSystemPermissionsCacheDestroy() throws Exception {
        sysPermSet.add(CACHE_DESTROY);

        testCacheDestroy();
    }

    /** */
    private GridRestResponse cacheCreate(String cacheName) throws IgniteCheckedException {
        return handle(new GridRestCacheRequest().cacheName(cacheName).command(GridRestCommand.GET_OR_CREATE_CACHE))
            .get();
    }

    /** */
    private GridRestResponse cacheDestroy(String cacheName) throws IgniteCheckedException {
        return handle(new GridRestCacheRequest().cacheName(cacheName).command(GridRestCommand.DESTROY_CACHE))
            .get();
    }

    /** */
    private IgniteInternalFuture<GridRestResponse> handle(GridRestRequest req) {
        return hnd.handleAsync(req);
    }

    /** */
    private Object cacheRestKey(String cacheName, GridRestCommand cmd, String key) throws IgniteCheckedException {
        return handle(new GridRestCacheRequest().cacheName(cacheName).key(key).command(cmd))
            .get().getResponse();
    }

    /** */
    private Object cacheRestKeyValue(String cacheName, GridRestCommand cmd, String key, String val)
        throws IgniteCheckedException {

        return handle(new GridRestCacheRequest().cacheName(cacheName).key(key).value(val).command(cmd))
            .get().getResponse();
    }

    /** */
    private Object cacheRestMap(String cacheName, GridRestCommand cmd, Map map) throws IgniteCheckedException {
        return handle(new GridRestCacheRequest().cacheName(cacheName).values(map).command(cmd))
            .get().getResponse();
    }

    /** */
    private boolean cachePut(String cacheName, String key, String val) throws IgniteCheckedException {
        return (boolean)cacheRestKeyValue(cacheName, GridRestCommand.CACHE_PUT, key, val);
    }

    /** */
    private boolean cachePutIfAbsent(String cacheName, String key, String val) throws IgniteCheckedException {
        return (boolean)cacheRestKeyValue(cacheName, GridRestCommand.CACHE_PUT_IF_ABSENT, key, val);
    }

    /** */
    private boolean cacheReplace(String cacheName, String key, String val) throws IgniteCheckedException {
        return (boolean)cacheRestKeyValue(cacheName, GridRestCommand.CACHE_REPLACE, key, val);
    }

    /** */
    private boolean cachePutAll(String cacheName, Map map) throws IgniteCheckedException {
        return (boolean)cacheRestMap(cacheName, GridRestCommand.CACHE_PUT_ALL, map);
    }

    /** */
    private boolean cacheContainsKey(String cacheName, String key) throws IgniteCheckedException {
        return (boolean)cacheRestKey(cacheName, GridRestCommand.CACHE_CONTAINS_KEY, key);
    }

    /** */
    private Object cacheGet(String cacheName, String key) throws IgniteCheckedException {
        return cacheRestKey(cacheName, GridRestCommand.CACHE_GET, key);
    }

    /** */
    private Map cacheGetAll(String cacheName, Map map) throws IgniteCheckedException {
        return (Map)cacheRestMap(cacheName, GridRestCommand.CACHE_GET_ALL, map);
    }

    /** */
    private Object cacheGetAndPut(String cacheName, String key, String newVal) throws IgniteCheckedException {
        return cacheRestKeyValue(cacheName, GridRestCommand.CACHE_GET_AND_PUT, key, newVal);
    }

    /** */
    private boolean cacheRemove(String cacheName, String key) throws IgniteCheckedException {
        return (boolean)cacheRestKey(cacheName, GridRestCommand.CACHE_REMOVE, key);
    }

    /** */
    private Object cacheGetAndRemove(String cacheName, String key) throws IgniteCheckedException {
        return cacheRestKey(cacheName, GridRestCommand.CACHE_GET_AND_REMOVE, key);
    }

    /** */
    private boolean cacheRemoveAll(String cacheName, Map map) throws IgniteCheckedException {
        return (boolean)cacheRestMap(cacheName, GridRestCommand.CACHE_REMOVE_ALL, map);
    }

    /** */
    private boolean cachePermsContains(String cacheName, SecurityPermission permission) {
        return Arrays.asList(cachePerms.get(cacheName)).contains(permission);
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        cachePerms.clear();
        sysPermSet.clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }


    /** {@inheritDoc} */
    @Override protected IgniteEx startGrid(IgniteConfiguration cfg) throws Exception {
        IgniteEx ex = super.startGrid(cfg);

        hnd = new GridCacheCommandHandler(ex.context());

        return ex;
    }
}
