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

package org.apache.ignite.internal.processor.security.client;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Consumer;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientAuthorizationException;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processor.security.AbstractSecurityTest;
import org.apache.ignite.internal.processor.security.TestSecurityData;
import org.apache.ignite.internal.processor.security.TestSecurityPluginConfiguration;
import org.apache.ignite.internal.util.typedef.G;
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
import static org.apache.ignite.plugin.security.SecurityPermission.TASK_EXECUTE;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Security tests for thin client.
 */
@RunWith(JUnit4.class)
public class ThinClientPermissionCheckTest extends AbstractSecurityTest {
    /** Client. */
    private static final String CLIENT = "client";

    /** Client that has system permissions. */
    private static final String CLIENT_SYS_PERM = "client_sys_perm";

    /** Client that has system permissions. */
    private static final String CLIENT_CACHE_TASK_OPER = "client_task_oper";

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
    protected IgniteConfiguration getConfiguration(TestSecurityData... clientData) throws Exception {
        return getConfiguration(G.allGrids().size(), clientData);
    }

    /**
     * @param idx Index.
     * @param clientData Array of client security data.
     */
    protected IgniteConfiguration getConfiguration(int idx,
        TestSecurityData... clientData) throws Exception {

        String instanceName = getTestIgniteInstanceName(idx);

        return getConfiguration(instanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration().setPersistenceEnabled(true)
                    )
            )
            .setAuthenticationEnabled(true)
            .setPluginConfigurations(
                new TestSecurityPluginConfiguration()
                    .setSecurityProcessorClass(TEST_SECURITY_PROCESSOR)
                    .setLogin("srv_" + instanceName)
                    .setPermissions(allowAllPermissionSet())
                    .thinClientSecData(clientData)
            )
            .setCacheConfiguration(
                new CacheConfiguration().setName(CACHE),
                new CacheConfiguration().setName(FORBIDDEN_CACHE)
            );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        IgniteEx ignite = startGrid(
            getConfiguration(
                new TestSecurityData(
                    CLIENT,
                    builder()
                        .appendCachePermissions(CACHE, CACHE_READ, CACHE_PUT, CACHE_REMOVE)
                        .appendCachePermissions(FORBIDDEN_CACHE, EMPTY_PERMS)
                        .build()
                ),
                new TestSecurityData(
                    CLIENT_SYS_PERM,
                    builder()
                        .appendSystemPermissions(CACHE_CREATE, CACHE_DESTROY)
                        .build()
                ),
                new TestSecurityData(
                    CLIENT_CACHE_TASK_OPER,
                    builder()
                        .appendCachePermissions(CACHE, CACHE_REMOVE)
                        .appendTaskPermissions(REMOVE_ALL_TASK, TASK_EXECUTE)
                        .appendTaskPermissions(CLEAR_TASK, TASK_EXECUTE)
                        .build()
                )
            )
        );

        ignite.cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected SecurityPermissionSetBuilder builder() {
        return super.builder().defaultAllowAll(false);
    }

    /**
     * @throws Exception If error occurs.
     */
    @Test
    public void testCacheSinglePermOperations() throws Exception {
        for (Consumer<IgniteClient> c : consumers(CACHE))
            executeOperation(CLIENT, c);

        for (Consumer<IgniteClient> c : consumers(FORBIDDEN_CACHE))
            executeForbiddenOperation(c);
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
        executeOperation(CLIENT_CACHE_TASK_OPER, c -> c.cache(CACHE).removeAll());
        executeOperation(CLIENT_CACHE_TASK_OPER, c -> c.cache(CACHE).clear());

        executeForbiddenOperation(c -> c.cache(CACHE).removeAll());
        executeForbiddenOperation(c -> c.cache(CACHE).clear());
    }

    /**
     * @throws Exception If error occurs.
     */
    @Test
    public void testSysOperation() throws Exception {
        try (IgniteClient sysPrmClnt = startClient(CLIENT_SYS_PERM)) {
            sysPrmClnt.createCache(DYNAMIC_CACHE);

            assertThat(sysPrmClnt.cacheNames().contains(DYNAMIC_CACHE), is(true));

            sysPrmClnt.destroyCache(DYNAMIC_CACHE);

            assertThat(sysPrmClnt.cacheNames().contains(DYNAMIC_CACHE), is(false));
        }

        executeForbiddenOperation(c -> c.createCache(DYNAMIC_CACHE));
        executeForbiddenOperation(c -> c.destroyCache(CACHE));
    }

    /**
     * @param cacheName Cache name.
     */
    private Collection<Consumer<IgniteClient>> consumers(final String cacheName) {
        return Arrays.asList(
            c -> c.cache(cacheName).put("key", "value"),
            c -> c.cache(cacheName).putAll(singletonMap("key", "value")),
            c -> c.cache(cacheName).get("key"),
            c -> c.cache(cacheName).getAll(Collections.singleton("key")),
            c -> c.cache(cacheName).containsKey("key"),
            c -> c.cache(cacheName).remove("key"),
            c -> c.cache(cacheName).replace("key", "value"),
            c -> c.cache(cacheName).putIfAbsent("key", "value"),
            c -> c.cache(cacheName).getAndPut("key", "value"),
            c -> c.cache(cacheName).getAndRemove("key"),
            c -> c.cache(cacheName).getAndReplace("key", "value")
        );
    }

    /**
     * @param cons Consumer.
     */
    private void executeOperation(String clientName, Consumer<IgniteClient> cons) throws Exception {
        try (IgniteClient client = startClient(clientName)) {
            cons.accept(client);
        }
    }

    /**
     * @param cons Consumer.
     */
    private void executeForbiddenOperation(Consumer<IgniteClient> cons) {
        try (IgniteClient client = startClient(CLIENT)) {
            cons.accept(client);

            fail();

        }
        catch (Exception e) {
            assertThrowable(e);
        }
    }

    /**
     * @param throwable Throwable.
     */
    private void assertThrowable(Throwable throwable) {
        assertThat(throwable instanceof ClientAuthorizationException, is(true));
    }

    /**
     * @param userName User name.
     */
    private IgniteClient startClient(String userName) {
        return Ignition.startClient(
            new ClientConfiguration().setAddresses(Config.SERVER)
                .setUserName(userName)
                .setUserPassword("")
        );
    }
}
