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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientAuthorizationException;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.impl.TestCertificateSecurityPluginProvider;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.apache.ignite.testframework.GridTestUtils;
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
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Security tests for thin client.
 */
@RunWith(JUnit4.class)
public class ThinClientSslPermissionCheckTest extends AbstractSecurityTest {
    /** Client. */
    private static final String CLIENT = "node01";

    /** Client that has system permissions. */
    private static final String CLIENT_SYS_PERM = "node02";

    /** Client that has system permissions. */
    private static final String CLIENT_CACHE_TASK_OPER = "node03";

    /** Cache. */
    private static final String CACHE = "TEST_CACHE";

    /** Forbidden cache. */
    private static final String FORBIDDEN_CACHE = "FORBIDDEN_TEST_CACHE";

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
            new TestCertificateSecurityPluginProvider(clientData)
        ).setCacheConfiguration(
            new CacheConfiguration().setName(CACHE),
            new CacheConfiguration().setName(FORBIDDEN_CACHE)
        ).setClientConnectorConfiguration(
            new ClientConnectorConfiguration().setSslEnabled(true).setSslClientAuth(true)
            .setUseIgniteSslContextFactory(false)
            .setSslContextFactory(GridTestUtils.sslTrustedFactory("node01", "trustboth"))
        );
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

        ignite.cluster().active(true);
    }

    /** */
    @Test
    public void testCacheSinglePermOperations() throws Exception {
        for (IgniteBiTuple<Consumer<IgniteClient>, String> t : operations(CACHE))
            runOperation(CLIENT, t);

        for (IgniteBiTuple<Consumer<IgniteClient>, String> t : operations(FORBIDDEN_CACHE))
            assertThrowsWithCause(() -> runOperation(CLIENT, t), ClientAuthorizationException.class);
    }

    /**
     * That test shows the wrong case when a client has permission for a remove operation
     * but a removeAll operation is forbidden for it. To have permission for the removeAll (clear) operation
     * a client need to have the permission to execute {@link #REMOVE_ALL_TASK} ({@link #CLEAR_TASK}) task.
     *
     * @throws Exception If error occurs.
     */
    @Test
    public void testCacheTaskPermOperations() throws Exception {
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
     * @param cacheName Cache name.
     */
    private Collection<IgniteBiTuple<Consumer<IgniteClient>, String>> operations(final String cacheName) {
        return Arrays.asList(
            t(c -> c.cache(cacheName).put("key", "value"), "put"),
            t(c -> c.cache(cacheName).putAll(singletonMap("key", "value")), "putAll"),
            t(c -> c.cache(cacheName).get("key"), "get)"),
            t(c -> c.cache(cacheName).getAll(Collections.singleton("key")), "getAll"),
            t(c -> c.cache(cacheName).containsKey("key"), "containsKey"),
            t(c -> c.cache(cacheName).remove("key"), "remove"),
            t(c -> c.cache(cacheName).replace("key", "value"), "replace"),
            t(c -> c.cache(cacheName).putIfAbsent("key", "value"), "putIfAbsent"),
            t(c -> c.cache(cacheName).getAndPut("key", "value"), "getAndPut"),
            t(c -> c.cache(cacheName).getAndRemove("key"), "getAndRemove"),
            t(c -> c.cache(cacheName).getAndReplace("key", "value"), "getAndReplace")
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
     */
    private IgniteClient startClient(String userName) {
        return Ignition.startClient(
            new ClientConfiguration().setAddresses(Config.SERVER).setSslMode(SslMode.REQUIRED)
            .setSslClientCertificateKeyStorePath(GridTestUtils.keyStorePath(userName))
            .setSslClientCertificateKeyStorePassword(GridTestUtils.keyStorePassword())
            .setSslTrustCertificateKeyStorePath(GridTestUtils.keyStorePath("trustone"))
            .setSslTrustCertificateKeyStorePassword(GridTestUtils.keyStorePassword())
        );
    }
}
