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

package org.apache.ignite.internal.processors.security.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import com.google.common.collect.ImmutableSet;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientAuthenticationException;
import org.apache.ignite.client.ClientAuthorizationException;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.ThinClientConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.eviction.paged.TestObject;
import org.apache.ignite.internal.processors.platform.cache.expiry.PlatformExpiryPolicyFactory;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.AbstractTestSecurityPluginProvider;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.util.lang.RunnableX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static java.util.Collections.singletonMap;
import static org.apache.ignite.configuration.DataPageEvictionMode.RANDOM_LRU;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_EXPIRED;
import static org.apache.ignite.internal.cluster.DistributedConfigurationUtils.CONN_DISABLED_BY_ADMIN_ERR_MSG;
import static org.apache.ignite.internal.processors.configuration.distributed.DistributedConfigurationProcessor.toMetaStorageKey;
import static org.apache.ignite.internal.processors.odbc.ClientListenerNioListener.MANAGEMENT_CLIENT_ATTR;
import static org.apache.ignite.internal.util.lang.GridFunc.t;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_OPS;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_DESTROY;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_PUT;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_REMOVE;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALL_PERMISSIONS;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Security tests for thin client.
 */
@RunWith(JUnit4.class)
public class ThinClientPermissionCheckTest extends AbstractSecurityTest {
    /** Client. */
    private static final String CLIENT = "client";

    /** Client with CACHE_READ permission only. */
    private static final String CLIENT_READ = "client_read";

    /** Client with CACHE_PUT permissions only. */
    private static final String CLIENT_PUT = "client_put";

    /** Client with CACHE_REMOVE permission only. */
    private static final String CLIENT_REMOVE = "client_remove";

    /** Client that has system permissions. */
    private static final String CLIENT_SYS_PERM = "client_sys_perm";

    /** Client that has admin permissions. */
    private static final String ADMIN = "admin";

    /** Cache. */
    protected static final String CACHE = "TEST_CACHE";

    /** Forbidden cache. */
    protected static final String FORBIDDEN_CACHE = "FORBIDDEN_TEST_CACHE";

    /** Cache to test system oper permissions. */
    private static final String DYNAMIC_CACHE = "DYNAMIC_TEST_CACHE";

    /** Cache for testing object expiration with security enabled. */
    protected static final String EXPIRATION_TEST_CACHE = "EXPIRATION_TEST_CACHE";

    /** Off-heap cache for testing object eviction with security enabled. */
    protected static final String EVICTION_TEST_CACHE = "EVICTION_TEST_CACHE";

    /** Name of the data region for object eviction testing. */
    protected static final String EVICTION_TEST_DATA_REGION_NAME = "EVICTION_TEST_DATA_REGION_NAME";

    /** Size of the data region for object eviction testing. */
    protected static final int EVICTION_TEST_DATA_REGION_SIZE = 20 * (1 << 20);

    /** */
    private static final String THIN_CONN_ENABLED_PROP = "newThinConnectionsEnabled";

    /** */
    private Map<String, String> userAttrs;

    /**
     * @param clientData Array of client security data.
     */
    private IgniteConfiguration getConfiguration(TestSecurityData... clientData) throws Exception {
        return getConfiguration(G.allGrids().size(), clientData);
    }

    /**
     * @param idx Index.
     * @param clientData Array of client security data.
     */
    protected IgniteConfiguration getConfiguration(int idx, TestSecurityData... clientData) throws Exception {
        String instanceName = getTestIgniteInstanceName(idx);

        return getConfiguration(
                instanceName,
                securityPluginProvider(instanceName, clientData))
            .setClientConnectorConfiguration(new ClientConnectorConfiguration()
                .setThinClientConfiguration(new ThinClientConfiguration()
                    .setServerToClientExceptionStackTraceSending(true)))
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDataRegionConfigurations(new DataRegionConfiguration()
                    .setName(EVICTION_TEST_DATA_REGION_NAME)
                    .setPageEvictionMode(RANDOM_LRU)
                    .setMaxSize(EVICTION_TEST_DATA_REGION_SIZE)
                    .setInitialSize(EVICTION_TEST_DATA_REGION_SIZE / 2)))
            .setCacheConfiguration(cacheConfigurations())
            .setIncludeEventTypes(EVT_CACHE_OBJECT_EXPIRED);
    }

    /** Gets cache configurations */
    protected CacheConfiguration[] cacheConfigurations() {
        return new CacheConfiguration[] {
            new CacheConfiguration().setName(CACHE),
            new CacheConfiguration().setName(FORBIDDEN_CACHE),
            new CacheConfiguration<>(EXPIRATION_TEST_CACHE)
                .setExpiryPolicyFactory(new PlatformExpiryPolicyFactory(10, 10, 10)),
            new CacheConfiguration<>(EVICTION_TEST_CACHE)
                .setDataRegionName(EVICTION_TEST_DATA_REGION_NAME)
        };
    }

    /**
     * @param instanceName Ignite instance name.
     * @param clientData Client data.
     */
    protected AbstractTestSecurityPluginProvider securityPluginProvider(String instanceName,
        TestSecurityData... clientData) {
        return new TestSecurityPluginProvider("srv_" + instanceName, null, ALL_PERMISSIONS, false, clientData);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        IgniteEx ignite = startGrid(
            getConfiguration(
                new TestSecurityData(CLIENT,
                    SecurityPermissionSetBuilder.create().defaultAllowAll(false)
                        .appendCachePermissions(CACHE, CACHE_READ, CACHE_PUT, CACHE_REMOVE)
                        .appendCachePermissions(FORBIDDEN_CACHE, EMPTY_PERMS)
                        .build()
                ),
                new TestSecurityData(CLIENT_READ,
                    SecurityPermissionSetBuilder.create().defaultAllowAll(false)
                        .appendCachePermissions(CACHE, CACHE_READ)
                        .build()
                ),
                new TestSecurityData(CLIENT_PUT,
                    SecurityPermissionSetBuilder.create().defaultAllowAll(false)
                        .appendCachePermissions(CACHE, CACHE_PUT)
                        .appendCachePermissions(EXPIRATION_TEST_CACHE, CACHE_PUT)
                        .appendCachePermissions(EVICTION_TEST_CACHE, CACHE_PUT)
                        .build()
                ),
                new TestSecurityData(CLIENT_REMOVE,
                    SecurityPermissionSetBuilder.create().defaultAllowAll(false)
                        .appendCachePermissions(CACHE, CACHE_REMOVE)
                        .build()
                ),
                new TestSecurityData(CLIENT_SYS_PERM,
                    SecurityPermissionSetBuilder.create().defaultAllowAll(false)
                        .appendSystemPermissions(CACHE_CREATE, CACHE_DESTROY)
                        .build()
                ),
                new TestSecurityData(ADMIN,
                    SecurityPermissionSetBuilder.create().defaultAllowAll(false)
                        .appendSystemPermissions(ADMIN_OPS)
                        .build()
                )
            )
        );

        ignite.cluster().state(ClusterState.ACTIVE);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        grid(0).cache(CACHE).clear();
    }

    /** */
    @Test
    public void testCacheSinglePermOperations() {
        for (IgniteBiTuple<Consumer<IgniteClient>, String> t : operations(CACHE))
            runOperation(CLIENT, t);

        for (IgniteBiTuple<Consumer<IgniteClient>, String> t : operations(FORBIDDEN_CACHE))
            assertThrowsWithCause(() -> runOperation(CLIENT, t), ClientAuthorizationException.class);
    }

    /** */
    @Test
    public void testCheckCacheReadOperations() {
        for (IgniteBiTuple<Consumer<IgniteClient>, String> t : operationsRead(CACHE)) {
            runOperation(CLIENT_READ, t);

            assertThrowsWithCause(() -> runOperation(CLIENT_PUT, t), ClientAuthorizationException.class);
            assertThrowsWithCause(() -> runOperation(CLIENT_REMOVE, t), ClientAuthorizationException.class);
        }
    }

    /** */
    @Test
    public void testCheckCachePutOperations() {
        for (IgniteBiTuple<Consumer<IgniteClient>, String> t : operationsPut(CACHE)) {
            runOperation(CLIENT_PUT, t);

            assertThrowsWithCause(() -> runOperation(CLIENT_READ, t), ClientAuthorizationException.class);
            assertThrowsWithCause(() -> runOperation(CLIENT_REMOVE, t), ClientAuthorizationException.class);
        }
    }

    /** */
    @Test
    public void testCheckCacheRemoveOperations() {
        for (IgniteBiTuple<Consumer<IgniteClient>, String> t : operationsRemove(CACHE)) {
            runOperation(CLIENT_REMOVE, t);

            assertThrowsWithCause(() -> runOperation(CLIENT_READ, t), ClientAuthorizationException.class);
            assertThrowsWithCause(() -> runOperation(CLIENT_PUT, t), ClientAuthorizationException.class);
        }
    }

    /** */
    @Test
    public void testCacheTaskPermOperations() {
        List<IgniteBiTuple<Consumer<IgniteClient>, String>> ops = Arrays.asList(
            t(c -> c.cache(CACHE).removeAll(), "removeAll"),
            t(c -> c.cache(CACHE).removeAll(Collections.singleton("key")), "removeAll"),
            t(c -> c.cache(CACHE).clear(), "clear"),
            t(c -> c.cache(CACHE).clear("key"), "clearKey"),
            t(c -> c.cache(CACHE).clearAll(ImmutableSet.of("key")), "clearKeys")
        );

        for (IgniteBiTuple<Consumer<IgniteClient>, String> op : ops) {
            grid(0).cache(CACHE).put("key", "val");

            runOperation(CLIENT_REMOVE, op);

            assertNull(grid(0).cache(CACHE).get("key"));
        }

        grid(0).cache(CACHE).put("key", "val");

        for (IgniteBiTuple<Consumer<IgniteClient>, String> op : ops)
            assertAuthorizationFailed(() -> runOperation(CLIENT_PUT, op));

        assertEquals("val", grid(0).cache(CACHE).get("key"));
    }

    /** */
    @Test
    public void testSysOperation() throws Exception {
        try (IgniteClient sysPrmClnt = startClient(CLIENT_SYS_PERM)) {
            sysPrmClnt.createCache(DYNAMIC_CACHE);

            assertTrue(sysPrmClnt.cacheNames().contains(DYNAMIC_CACHE));

            sysPrmClnt.destroyCache(DYNAMIC_CACHE);

            assertFalse(sysPrmClnt.cacheNames().contains(DYNAMIC_CACHE));
        }

        List<IgniteBiTuple<Consumer<IgniteClient>, String>> ops = Arrays.asList(
            t(c -> c.createCache(DYNAMIC_CACHE), "createCache"),
            t(c -> c.destroyCache(CACHE), "destroyCache")
        );

        for (IgniteBiTuple<Consumer<IgniteClient>, String> op : ops)
            assertThrowsWithCause(() -> runOperation(CLIENT, op), ClientAuthorizationException.class);
    }

    /** */
    @Test
    public void testAllowedOperationAfterSecurityViolation() throws Exception {
        try (IgniteClient client = startClient(CLIENT_READ)) {
            assertThrowsWithCause(() -> client.cache(CACHE).put("key", "value"), ClientAuthorizationException.class);
            assertNull(client.cache(CACHE).get("key"));
        }
    }

    /**
     * Tests that the expiration of a cache entry does not require any special permissions from the user who adds the
     * entry.
     */
    @Test
    public void testCacheEntryExpiration() throws Exception {
        CountDownLatch cacheObjExpiredLatch = new CountDownLatch(G.allGrids().size());

        for (Ignite ignite : G.allGrids()) {
            ignite.events().localListen(evt -> {
                if ((evt.type() == EVT_CACHE_OBJECT_EXPIRED && EXPIRATION_TEST_CACHE.equals(((CacheEvent)evt).cacheName())))
                    cacheObjExpiredLatch.countDown();

                return true;
            }, EVT_CACHE_OBJECT_EXPIRED);
        }

        try (IgniteClient client = startClient(CLIENT_PUT)) {
            ClientCache<Object, Object> cache = client.cache(EXPIRATION_TEST_CACHE);

            cache.put("key", "val");

            cacheObjExpiredLatch.await(getTestTimeout(), TimeUnit.MILLISECONDS);

            for (Ignite ignite : G.allGrids())
                assertNull(ignite.cache(EXPIRATION_TEST_CACHE).get("key"));
        }
    }

    /**
     * Tests that the eviction of a cache entry does not require any special permissions from the user who adds the
     * entry.
     */
    @Test
    public void testCacheEntryEviction() throws Exception {
        try (IgniteClient client = startClient(CLIENT_PUT)) {
            ClientCache<Object, Object> cache = client.cache(EVICTION_TEST_CACHE);

            int entrySize = 4 * (1 << 10);

            int entriesCnt = 2 * EVICTION_TEST_DATA_REGION_SIZE / entrySize;

            for (int i = 0; i < entriesCnt; i++)
                cache.put(i, new TestObject(entrySize / 4));

            for (Ignite ignite : G.allGrids()) {
                Set<Integer> insertedKeys = IntStream.range(0, entriesCnt).boxed().collect(Collectors.toSet());

                assertTrue(ignite.cache(EVICTION_TEST_CACHE).getAll(insertedKeys).size() < entriesCnt);
            }
        }
    }

    /** */
    @Test
    public void testConnectAsManagementClient() throws Exception {
        Runnable cliCanConnect = () -> {
            try (IgniteClient cli = startClient(CLIENT)) {
                assertNotNull("Cach query from CLIENT", cli.cacheNames());
            }
        };

        Runnable adminCanConnect = () -> {
            try (IgniteClient cli = startClient(ADMIN)) {
                assertNotNull("Cach query from CLIENT", cli.cacheNames());
            }
        };

        Runnable withUserAttrsCheck = () -> {
            userAttrs = F.asMap(MANAGEMENT_CLIENT_ATTR, "true");

            try {
                // Trying to connect as CLIENT with "management client" flag must fail, because of security.
                // CLIENT has no ADMIN_OPS permission.
                assertThrows(log, () -> startClient(CLIENT), ClientAuthenticationException.class, "ADMIN_OPS permission required");

                adminCanConnect.run();
            }
            finally {
                userAttrs = null;
            }
        };

        Runnable checkDflt = () -> {
            cliCanConnect.run();
            adminCanConnect.run();

            withUserAttrsCheck.run();
        };

        checkDflt.run();

        assertTrue(grid(0).context().distributedMetastorage().read(toMetaStorageKey(THIN_CONN_ENABLED_PROP)));

        // Disable all new thin client connections except ADMIN_OPS control.sh
        grid(0).context().distributedMetastorage().write(toMetaStorageKey(THIN_CONN_ENABLED_PROP), false);

        try {
            assertThrows(log, () -> startClient(CLIENT), ClientAuthenticationException.class, CONN_DISABLED_BY_ADMIN_ERR_MSG);
            // Trying to connect without specifying "management client" flag must fail.
            assertThrows(log, () -> startClient(ADMIN), ClientAuthenticationException.class, CONN_DISABLED_BY_ADMIN_ERR_MSG);

            withUserAttrsCheck.run();
        }
        finally {
            grid(0).context().distributedMetastorage().write(toMetaStorageKey(THIN_CONN_ENABLED_PROP), true);
        }

        checkDflt.run();
    }

    /**
     * Gets all operations.
     *
     * @param cacheName Cache name.
     */
    private Collection<IgniteBiTuple<Consumer<IgniteClient>, String>> operations(final String cacheName) {
        List<IgniteBiTuple<Consumer<IgniteClient>, String>> res = new ArrayList<>();

        res.addAll(operationsRead(cacheName));
        res.addAll(operationsPut(cacheName));
        res.addAll(operationsRemove(cacheName));

        return res;
    }

    /**
     * Gets operations taht require CACHE_READ permission.
     *
     * @param cacheName Cache name.
     */
    private Collection<IgniteBiTuple<Consumer<IgniteClient>, String>> operationsRead(final String cacheName) {
        return Arrays.asList(
            t(c -> c.cache(cacheName).get("key"), "get)"),
            t(c -> c.cache(cacheName).getAll(Collections.singleton("key")), "getAll"),
            t(c -> c.cache(cacheName).containsKey("key"), "containsKey"),
            t(c -> c.cache(cacheName).containsKeys(ImmutableSet.of("key")), "containsKeys")
        );
    }

    /**
     * Gets operations taht require CACHE_PUT permission.
     *
     * @param cacheName Cache name.
     */
    private Collection<IgniteBiTuple<Consumer<IgniteClient>, String>> operationsPut(final String cacheName) {
        return Arrays.asList(
            t(c -> c.cache(cacheName).put("key", "value"), "put"),
            t(c -> c.cache(cacheName).putAll(singletonMap("key", "value")), "putAll"),
            t(c -> c.cache(cacheName).replace("key", "value"), "replace"),
            t(c -> c.cache(cacheName).putIfAbsent("key", "value"), "putIfAbsent"),
            t(c -> c.cache(cacheName).getAndPutIfAbsent("key", "value"), "getAndPutIfAbsent"),
            t(c -> c.cache(cacheName).getAndPut("key", "value"), "getAndPut"),
            t(c -> c.cache(cacheName).getAndReplace("key", "value"), "getAndReplace")
        );
    }

    /**
     * Gets operations taht require CACHE_REMOVE permission.
     *
     * @param cacheName Cache name.
     */
    private Collection<IgniteBiTuple<Consumer<IgniteClient>, String>> operationsRemove(final String cacheName) {
        return Arrays.asList(
            t(c -> c.cache(cacheName).remove("key"), "remove"),
            t(c -> c.cache(cacheName).getAndRemove("key"), "getAndRemove")
        );
    }

    /** */
    private void runOperation(String clientName, IgniteBiTuple<Consumer<IgniteClient>, String> op) {
        try (IgniteClient client = startClient(clientName)) {
            op.get1().accept(client);
        }
        catch (Exception e) {
            throw new IgniteException(op.get2(), e);
        }
    }

    /**
     * @param userName User name.
     * @return Thin client for specified user.
     */
    private IgniteClient startClient(String userName) {
        return Ignition.startClient(
            new ClientConfiguration().setAddresses(Config.SERVER)
                .setUserName(userName)
                .setUserPassword("")
                .setUserAttributes(userAttributres())
        );
    }

    /**
     * @return User attributes.
     */
    protected Map<String, String> userAttributres() {
        return userAttrs;
    }

    /** */
    public static void assertAuthorizationFailed(RunnableX r) {
        try {
            r.run();
        }
        catch (Exception e) {
            if (X.hasCause(e, ClientAuthorizationException.class))
                return;

            // In cases when security exception is thrown during task execution, its stacktrace is propagated back as
            // a simple string. It is the case for removeAll/clear cache operations.
            ClientException cliEx = X.cause(e, ClientException.class);

            if (cliEx != null && cliEx.getMessage().contains("Authorization failed [perm=CACHE_REMOVE, name=TEST_CACHE"))
                return;
        }

        fail();
    }
}
