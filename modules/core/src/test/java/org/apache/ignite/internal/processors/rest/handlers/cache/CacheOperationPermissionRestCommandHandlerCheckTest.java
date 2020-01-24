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

package org.apache.ignite.internal.processors.rest.handlers.cache;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandler;
import org.apache.ignite.internal.processors.rest.request.GridRestCacheRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.plugin.security.SecurityException;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static java.util.Collections.singletonMap;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_DESTROY;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_PUT;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_REMOVE;
import static org.apache.ignite.plugin.security.SecurityPermission.JOIN_AS_SERVER;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Test CRUD, create and destroy cache permissions with rest commands handler.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CacheOperationPermissionRestCommandHandlerCheckTest extends GridCommonAbstractTest {
    /** Empty permission. */
    private static final SecurityPermission[] EMPTY_PERM = new SecurityPermission[0];

    /** Cache name for tests. */
    private static final String CACHE_NAME = "TEST_CACHE";

    /** Create cache name. */
    private static final String CREATE_CACHE_NAME = "CREATE_TEST_CACHE";

    /** Forbidden cache. */
    private static final String FORBIDDEN_CACHE_NAME = "FORBIDDEN_TEST_CACHE";

    /** New cache. */
    private static final String NEW_TEST_CACHE = "NEW_TEST_CACHE";

    private String key = "key";
    private String val = "value";

    /** Cache perms. */
    private Map<String, SecurityPermission[]> cachePerms = new HashMap<>();
    /** Security permission set. */
    private Set<SecurityPermission> securityPermissionSet = new HashSet<>();
    /** Default allow all. */
    private boolean defaultAllowAll;

    /** Handler. */
    private GridRestCommandHandler hnd;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(new TcpDiscoveryVmIpFinder(true));
        disco.setJoinTimeout(5000);

        IgniteConfiguration cfg = super.getConfiguration();

        cfg.setDiscoverySpi(disco);

        SecurityPermissionSetBuilder builder = SecurityPermissionSetBuilder.create();

        builder.defaultAllowAll(defaultAllowAll);

        cachePerms.forEach((builder::appendCachePermissions));
        securityPermissionSet.forEach(builder::appendSystemPermissions);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        )
            .setAuthenticationEnabled(true)
            .setPluginProviders(
                new TestSecurityPluginProvider("login", "password", builder.build(), true));

        return cfg;
    }

    @Test
    public void testCacheCreate() throws Exception {
        cachePerms.putIfAbsent(CACHE_NAME, new SecurityPermission[] {CACHE_CREATE});
        cachePerms.put(FORBIDDEN_CACHE_NAME, EMPTY_PERM);

        startGrid(getConfiguration()).cluster().active(true);

        assertTrue(grid().cacheNames().isEmpty());

        cacheCreate(CACHE_NAME);

        assertTrue(grid().cacheNames().contains(CACHE_NAME));

        if (securityPermissionSet.contains(CACHE_CREATE)) {
            cacheCreate(FORBIDDEN_CACHE_NAME);
            assertTrue(grid().cacheNames().contains(FORBIDDEN_CACHE_NAME));
        }
        else {
            assertThrowsWithCause(() -> cacheCreate(FORBIDDEN_CACHE_NAME), IgniteCheckedException.class);

            assertFalse(grid().cacheNames().contains(FORBIDDEN_CACHE_NAME));
        }

        if (defaultAllowAll || securityPermissionSet.contains(CACHE_CREATE)) {
            cacheCreate(NEW_TEST_CACHE);
            assertTrue(grid().cacheNames().contains(NEW_TEST_CACHE));
        }
        else {
            assertThrowsWithCause(() -> cacheCreate(NEW_TEST_CACHE), IgniteCheckedException.class);
            assertFalse(grid().cacheNames().contains(NEW_TEST_CACHE));
        }
    }

    @Test
    public void atestCacheCreateDflAllTrue() throws Exception {
        try {
            defaultAllowAll = true;

            testCacheCreate();
        }
        finally {
            defaultAllowAll = false;
        }
    }

    @Test
    public void atestCacheCreateSysPerm() throws Exception {
        securityPermissionSet.add(CACHE_CREATE);

        testCacheCreate();
    }

    @Test
    public void testCachePut() throws Exception {
        cachePerms.putIfAbsent(CACHE_NAME, new SecurityPermission[] {CACHE_CREATE, CACHE_PUT});

        testCacheCreate();

        assertTrue(cacheRestKeyValue(CACHE_NAME, GridRestCommand.CACHE_PUT_IF_ABSENT));

        assertFalse(cacheRestKeyValue(CACHE_NAME, GridRestCommand.CACHE_PUT_IF_ABSENT));

        assertTrue(cachePut(CACHE_NAME));
        assertTrue(cachePutAll(CACHE_NAME));
        assertTrue(cacheRestKeyValue(CACHE_NAME, GridRestCommand.CACHE_REPLACE));

        if (defaultAllowAll) {
            assertTrue(cacheRestKeyValue(NEW_TEST_CACHE, GridRestCommand.CACHE_PUT_IF_ABSENT));

            assertFalse(cacheRestKeyValue(NEW_TEST_CACHE, GridRestCommand.CACHE_PUT_IF_ABSENT));

            assertTrue(cachePut(NEW_TEST_CACHE));
            assertTrue(cachePutAll(NEW_TEST_CACHE));
            assertTrue(cacheRestKeyValue(NEW_TEST_CACHE, GridRestCommand.CACHE_REPLACE));
        }
        else {
            assertThrowsWithCause(() -> cacheRestKeyValue(NEW_TEST_CACHE, GridRestCommand.CACHE_PUT_IF_ABSENT),
                IgniteCheckedException.class);
            assertThrowsWithCause(() -> cachePut(NEW_TEST_CACHE),
                IgniteCheckedException.class);
            assertThrowsWithCause(() -> cachePutAll(NEW_TEST_CACHE), IgniteCheckedException.class);
        }

        assertThrowsWithCause(() -> cachePut(FORBIDDEN_CACHE_NAME), IgniteCheckedException.class);
    }

    @Test
    public void testCacheRead() throws Exception {
        cachePerms.putIfAbsent(CACHE_NAME, new SecurityPermission[] {CACHE_CREATE, CACHE_PUT, CACHE_READ});

        securityPermissionSet.add(CACHE_CREATE);
        testCachePut();

        assertTrue((boolean)cacheRestKey(CACHE_NAME, GridRestCommand.CACHE_CONTAINS_KEY).getResponse());
        assertEquals(cacheGet(CACHE_NAME), val);
        assertTrue(cacheGetAll(CACHE_NAME).containsValue(val));

        if (defaultAllowAll) {
            assertTrue((boolean)cacheRestKey(NEW_TEST_CACHE, GridRestCommand.CACHE_CONTAINS_KEY).getResponse());
            assertEquals(cacheGet(NEW_TEST_CACHE), val);
        }
        else {
            assertThrowsWithCause(() -> cacheRestKey(NEW_TEST_CACHE, GridRestCommand.CACHE_CONTAINS_KEY), IgniteCheckedException.class);
            assertThrowsWithCause(() -> cacheGet(NEW_TEST_CACHE), IgniteCheckedException.class);
        }

        if (securityPermissionSet.contains(CACHE_CREATE))
            assertThrowsWithCause(() -> cacheGet(FORBIDDEN_CACHE_NAME), SecurityException.class);
    }

    @Test
    public void testCacheRemove() throws Exception {
        cachePerms.putIfAbsent(CACHE_NAME, new SecurityPermission[] {CACHE_CREATE, CACHE_PUT, CACHE_READ, CACHE_REMOVE});

        defaultAllowAll = true;

        testCachePut();

        assertTrue(grid().cache(CACHE_NAME).containsKey(key));

        cacheRestKey(CACHE_NAME, GridRestCommand.CACHE_REMOVE);

        assertFalse(grid().cache(CACHE_NAME).containsKey(key));

        if (defaultAllowAll) {
            assertTrue(grid().cache(NEW_TEST_CACHE).containsKey(key));

            cacheRestKey(NEW_TEST_CACHE, GridRestCommand.CACHE_REMOVE);

            assertFalse(grid().cache(NEW_TEST_CACHE).containsKey(key));
        }
    }

    @Test
    public void testCacheRemoveAll() throws Exception {
        cachePerms.putIfAbsent(CACHE_NAME, new SecurityPermission[] {CACHE_CREATE, CACHE_PUT, CACHE_READ, CACHE_REMOVE});

        testCachePut();

        assertTrue(grid().cache(CACHE_NAME).containsKey(key));

        if (defaultAllowAll) {
            cacheRestKeyValue(CACHE_NAME, GridRestCommand.CACHE_REMOVE_ALL);

            assertFalse(grid().cache(CACHE_NAME).containsKey(key));
        }
        else
            // Need permission TASK_EXECUTE for
            // "org.apache.ignite.internal.processors.cache.distributed.GridDistributedCacheAdapter$RemoveAllTask".
            assertThrowsWithCause(() -> cacheRestKeyValue(CACHE_NAME, GridRestCommand.CACHE_REMOVE_ALL),
                SecurityException.class);

        if (defaultAllowAll) {
            assertTrue(grid().cache(NEW_TEST_CACHE).containsKey(key));

            cacheRestKeyValue(NEW_TEST_CACHE, GridRestCommand.CACHE_REMOVE_ALL);

            assertFalse(grid().cache(NEW_TEST_CACHE).containsKey(key));
        }
    }

    @Test
    public void testCacheDestroy() throws Exception {
        cachePerms.putIfAbsent(CACHE_NAME, new SecurityPermission[] {CACHE_CREATE, CACHE_DESTROY});

        testCacheCreate();

        cacheDestroy(CACHE_NAME);

        assertFalse(grid().cacheNames().contains(CACHE_NAME));

        if (grid().cacheNames().contains(NEW_TEST_CACHE) &&
            (defaultAllowAll || securityPermissionSet.contains(CACHE_DESTROY))) {
            cacheDestroy(NEW_TEST_CACHE);

            assertFalse(grid().cacheNames().contains(NEW_TEST_CACHE));
        }
        else
            assertThrowsWithCause(() -> cacheDestroy(NEW_TEST_CACHE), SecurityException.class);

        if (grid().cacheNames().contains(FORBIDDEN_CACHE_NAME) && securityPermissionSet.contains(CACHE_DESTROY)) {
            cacheDestroy(FORBIDDEN_CACHE_NAME);

            assertFalse(grid().cacheNames().contains(FORBIDDEN_CACHE_NAME));
        }
        else
            assertThrowsWithCause(() -> cacheDestroy(FORBIDDEN_CACHE_NAME), SecurityException.class);
    }

    /**
     * @param cacheName Cache name.
     */
    private GridRestResponse cacheCreate(String cacheName) throws IgniteCheckedException {
        return handle(new GridRestCacheRequest().cacheName(cacheName).command(GridRestCommand.GET_OR_CREATE_CACHE))
            .get();
    }

    /**
     * @param cacheName Cache name.
     */
    private GridRestResponse cacheDestroy(String cacheName) throws IgniteCheckedException {
        return handle(new GridRestCacheRequest().cacheName(cacheName).command(GridRestCommand.DESTROY_CACHE))
            .get();
    }

    private void restartGrid() throws Exception {
        stopAllGrids();

        startGrid(getConfiguration()).cluster().active(true);
    }

    /**
     *
     */
    private IgniteInternalFuture<GridRestResponse> handle(GridRestRequest req) {
        return hnd.handleAsync(req);
    }

    /**
     *
     */
    private boolean cacheRestKeyValue(String cachename, GridRestCommand cmd) throws IgniteCheckedException {
        return (boolean)handle(new GridRestCacheRequest().cacheName(cachename).key(key).value(val).command(cmd))
            .get().getResponse();
    }

    /**
     *
     */
    private boolean cachePut(String cachename) throws IgniteCheckedException {
        return cacheRestKeyValue(cachename, GridRestCommand.CACHE_PUT);
    }

    /**
     *
     */
    private boolean cachePutAll(String cachename) throws IgniteCheckedException {
        return (boolean)handle(new GridRestCacheRequest().cacheName(cachename).values(singletonMap(key, val))
            .command(GridRestCommand.CACHE_PUT_ALL)).get().getResponse();
    }

    /**
     *
     */
    private Object cacheGet(String cachename) throws IgniteCheckedException {
        return handle(new GridRestCacheRequest().cacheName(cachename).key(key).command(GridRestCommand.CACHE_GET))
            .get().getResponse();
    }

    /**
     *
     */
    private Map cacheGetAll(String cachename) throws IgniteCheckedException {
        return (Map)handle(new GridRestCacheRequest().cacheName(cachename).values(singletonMap(key, null))
            .command(GridRestCommand.CACHE_GET_ALL)).get().getResponse();
    }

    /**
     *
     */
    private GridRestResponse cacheRestKey(String cacheName, GridRestCommand cmd) throws IgniteCheckedException {
        return handle(new GridRestCacheRequest().cacheName(cacheName).key(key).command(cmd)).get();
    }

    /**
     *
     */
    private List<Function<String, IgniteInternalFuture<GridRestResponse>>> operations() {
        return Arrays.asList(
//            n -> handle(new GridRestCacheRequest().cacheName(n).key("key").value("value")
//                .command(GridRestCommand.CACHE_PUT)),
//            n -> handle(new GridRestCacheRequest().cacheName(n).values(singletonMap("key", "value"))
//                .command(GridRestCommand.CACHE_PUT_ALL)),
//            n -> handle(new GridRestCacheRequest().cacheName(n).key("key")
//                .command(GridRestCommand.CACHE_GET)),
            n -> handle(new GridRestCacheRequest().cacheName(n).values(singletonMap("key", null))
                .command(GridRestCommand.CACHE_GET_ALL)),
            n -> handle(new GridRestCacheRequest().cacheName(n).key("key")
                .command(GridRestCommand.CACHE_CONTAINS_KEY)),
            n -> handle(new GridRestCacheRequest().cacheName(n).key("key")
                .command(GridRestCommand.CACHE_REMOVE)),
            n -> handle(new GridRestCacheRequest().cacheName(n).values(singletonMap("key", null))
                .command(GridRestCommand.CACHE_REMOVE_ALL)),
            n -> handle(new GridRestCacheRequest().cacheName(n).key("key").value("value")
                .command(GridRestCommand.CACHE_REPLACE)),
            n -> handle(new GridRestCacheRequest().cacheName(n).key("key").value("value")
                .command(GridRestCommand.CACHE_PUT_IF_ABSENT)),
            n -> handle(new GridRestCacheRequest().cacheName(n).key("key").value("value")
                .command(GridRestCommand.CACHE_GET_AND_PUT)),
            n -> handle(new GridRestCacheRequest().cacheName(n).key("key")
                .command(GridRestCommand.CACHE_GET_AND_REMOVE)),
            n -> handle(new GridRestCacheRequest().cacheName(n).key("key").value("value")
                .command(GridRestCommand.CACHE_GET_AND_REPLACE))
        );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        securityPermissionSet.clear();
        securityPermissionSet.add(JOIN_AS_SERVER);
        securityPermissionSet.add(CACHE_CREATE);
        securityPermissionSet.add(CACHE_DESTROY);
        defaultAllowAll = false;

//        startGrid(getConfiguration()).cluster().active(true);
//        hnd = new GridCacheCommandHandler(grid().context());
        cachePerms.clear();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
    }

    @Override protected IgniteEx startGrid(IgniteConfiguration cfg) throws Exception {
        IgniteEx ex = super.startGrid(cfg);
        hnd = new GridCacheCommandHandler(grid().context());
        return ex;
    }
}
