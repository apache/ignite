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
import java.util.function.Consumer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientAuthorizationException;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.AbstractTestSecurityPluginProvider;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static java.util.Collections.singletonMap;
import static org.apache.ignite.internal.util.lang.GridFunc.t;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_DESTROY;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_PUT;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_REMOVE;
import static org.apache.ignite.plugin.security.SecurityPermission.TASK_EXECUTE;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;
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

    /** Client that has system permissions. */
    private static final String CLIENT_CACHE_TASK_OPER = "client_task_oper";

    /** Cache. */
    protected static final String CACHE = "TEST_CACHE";

    /** Forbidden cache. */
    protected static final String FORBIDDEN_CACHE = "FORBIDDEN_TEST_CACHE";

    /** Cache to test system oper permissions. */
    private static final String DYNAMIC_CACHE = "DYNAMIC_TEST_CACHE";

    /** Remove all task name. */
    public static final String REMOVE_ALL_TASK =
        "org.apache.ignite.internal.processors.cache.distributed.GridDistributedCacheAdapter$RemoveAllTask";

    /** Clear task name. */
    public static final String CLEAR_TASK =
        "org.apache.ignite.internal.processors.cache.GridCacheAdapter$ClearTask";

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
    private IgniteConfiguration getConfiguration(int idx, TestSecurityData... clientData) throws Exception {
        String instanceName = getTestIgniteInstanceName(idx);

        return getConfiguration(
            instanceName,
            securityPluginProvider(instanceName, clientData)
        ).setCacheConfiguration(cacheConfigurations());
    }

    /** Gets cache configurations */
    protected CacheConfiguration[] cacheConfigurations() {
        return new CacheConfiguration[] {
            new CacheConfiguration().setName(CACHE),
            new CacheConfiguration().setName(FORBIDDEN_CACHE)
        };
    }

    /**
     * @param instanceName Ignite instance name.
     * @param clientData Client data.
     */
    protected AbstractTestSecurityPluginProvider securityPluginProvider(String instanceName,
        TestSecurityData... clientData) {
        return new TestSecurityPluginProvider("srv_" + instanceName, null, ALLOW_ALL, false, clientData);
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
                new TestSecurityData(CLIENT_CACHE_TASK_OPER,
                    SecurityPermissionSetBuilder.create().defaultAllowAll(false)
                        .appendCachePermissions(CACHE, CACHE_REMOVE)
                        .appendTaskPermissions(REMOVE_ALL_TASK, TASK_EXECUTE)
                        .appendTaskPermissions(CLEAR_TASK, TASK_EXECUTE)
                        .build()
                )
            )
        );

        ignite.cluster().state(ClusterState.ACTIVE);
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

    /**
     * That test shows the wrong case when a client has permission for a remove operation
     * but a removeAll operation is forbidden for it. To have permission for the removeAll (clear) operation
     * a client need to have the permission to execute {@link #REMOVE_ALL_TASK} ({@link #CLEAR_TASK}) task.
     *
     * @throws Exception If error occurs.
     */
    @Test
    public void testCacheTaskPermOperations() {
        List<IgniteBiTuple<Consumer<IgniteClient>, String>> ops = Arrays.asList(
            t(c -> c.cache(CACHE).removeAll(), "removeAll"),
            t(c -> c.cache(CACHE).clear(), "clear")
        );

        for (IgniteBiTuple<Consumer<IgniteClient>, String> op : ops) {
            runOperation(CLIENT_CACHE_TASK_OPER, op);

            assertThrowsWithCause(() -> runOperation(CLIENT, op), ClientAuthorizationException.class);
        }
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
            t(c -> c.cache(cacheName).containsKey("key"), "containsKey")
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
        return null;
    }
}
