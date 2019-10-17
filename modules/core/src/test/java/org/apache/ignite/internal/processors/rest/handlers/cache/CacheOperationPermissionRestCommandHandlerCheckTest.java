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
import java.util.List;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
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

import org.junit.Test;

import static java.util.Collections.singletonMap;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_PUT;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_REMOVE;
import static org.apache.ignite.plugin.security.SecurityPermission.JOIN_AS_SERVER;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Test CRUD, create and destroy cache permissions with rest commands handler.
 */
public class CacheOperationPermissionRestCommandHandlerCheckTest extends GridCommonAbstractTest {
    /** Empty permission. */
    public static final SecurityPermission[] EMPTY_PERM = new SecurityPermission[0];

    /** Cache name for tests. */
    protected static final String CACHE_NAME = "TEST_CACHE";

    /** Create cache name. */
    protected static final String CREATE_CACHE_NAME = "CREATE_TEST_CACHE";

    /** Forbidden cache. */
    protected static final String FORBIDDEN_CACHE_NAME = "FORBIDDEN_TEST_CACHE";

    /** New cache. */
    protected static final String NEW_TEST_CACHE = "NEW_TEST_CACHE";

    /** Handler. */
    private GridRestCommandHandler hnd;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(new TcpDiscoveryVmIpFinder(true));
        disco.setJoinTimeout(5000);

        IgniteConfiguration cfg = super.getConfiguration();

        cfg.setLocalHost("127.0.0.1");

        ConnectorConfiguration clnCfg = new ConnectorConfiguration();
        clnCfg.setHost("127.0.0.1");
        clnCfg.setPort(11212);

        cfg.setConnectorConfiguration(clnCfg);
        cfg.setDiscoverySpi(disco);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        )
            .setAuthenticationEnabled(true)
            .setPluginProviders(new TestSecurityPluginProvider("login", "password", SecurityPermissionSetBuilder.create()
                .appendCachePermissions(CACHE_NAME, CACHE_CREATE, CACHE_READ, CACHE_PUT, CACHE_REMOVE)
                .appendCachePermissions(CREATE_CACHE_NAME, CACHE_CREATE)
                .appendCachePermissions(FORBIDDEN_CACHE_NAME, EMPTY_PERM)
                .appendSystemPermissions(JOIN_AS_SERVER)
                .build()));

        return cfg;
    }

    /**
     * Tests the execution of the GridRestCommand for Cache commands.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheRestCommand() throws Exception {
        // This won't fail since defaultAllowAll is true.
        createCache(NEW_TEST_CACHE);
        createCache(CACHE_NAME);
        createCache(CREATE_CACHE_NAME);

        assertThrowsWithCause(() -> createCache(FORBIDDEN_CACHE_NAME), IgniteCheckedException.class);

        assertFalse(grid().cacheNames().contains(FORBIDDEN_CACHE_NAME));
        assertTrue(grid().cacheNames().containsAll(Arrays.asList(NEW_TEST_CACHE,CACHE_NAME,CREATE_CACHE_NAME)));

        for (Function<String, IgniteInternalFuture<GridRestResponse>> f : operations()) {
            f.apply(NEW_TEST_CACHE).get();
            f.apply(CACHE_NAME).get();

            assertThrowsWithCause(() -> f.apply(CREATE_CACHE_NAME).get(), IgniteCheckedException.class);
        }

        // This won't fail since defaultAllowAll is true and Task permissions is empty.
        handle(new GridRestCacheRequest().cacheName(NEW_TEST_CACHE).command(GridRestCommand.CACHE_CLEAR)).get().getResponse();
        handle(new GridRestCacheRequest().cacheName(CACHE_NAME).command(GridRestCommand.CACHE_CLEAR)).get().getResponse();
        handle(new GridRestCacheRequest().cacheName(CREATE_CACHE_NAME).command(GridRestCommand.CACHE_CLEAR)).get().getResponse();

        // This won't fail since defaultAllowAll is true.
        handle(new GridRestCacheRequest().cacheName(NEW_TEST_CACHE).command(GridRestCommand.DESTROY_CACHE)).get().getResponse();

        assertThrowsWithCause(()->handle(new GridRestCacheRequest().cacheName(CACHE_NAME).command(GridRestCommand.DESTROY_CACHE)).get().getResponse(), IgniteCheckedException.class);
        assertThrowsWithCause(()->handle(new GridRestCacheRequest().cacheName(CREATE_CACHE_NAME).command(GridRestCommand.DESTROY_CACHE)).get().getResponse(), IgniteCheckedException.class);

    }

    /**
     * @param cacheName Cache name.
     */
    protected GridRestResponse createCache(String cacheName)
        throws IgniteCheckedException {
        GridRestCacheRequest req = new GridRestCacheRequest().cacheName(cacheName);
        req.command(GridRestCommand.GET_OR_CREATE_CACHE);

        return handle(req).get();
    }

    /** */
    protected IgniteInternalFuture<GridRestResponse> handle(GridRestRequest req) {
        return hnd.handleAsync(req);
    }

    /** */
    protected List<Function<String, IgniteInternalFuture<GridRestResponse>>> operations() {
        return Arrays.asList(
            n -> handle(new GridRestCacheRequest().cacheName(n).key("key").value("value").command(GridRestCommand.CACHE_PUT)),
            n -> handle(new GridRestCacheRequest().cacheName(n).values(singletonMap("key", "value")).command(GridRestCommand.CACHE_PUT_ALL)),
            n -> handle(new GridRestCacheRequest().cacheName(n).key("key").command(GridRestCommand.CACHE_GET)),
            n -> handle(new GridRestCacheRequest().cacheName(n).values(singletonMap("key", null)).command(GridRestCommand.CACHE_GET_ALL)),
            n -> handle(new GridRestCacheRequest().cacheName(n).key("key").command(GridRestCommand.CACHE_CONTAINS_KEY)),
            n -> handle(new GridRestCacheRequest().cacheName(n).key("key").command(GridRestCommand.CACHE_REMOVE)),
            n -> handle(new GridRestCacheRequest().cacheName(n).values(singletonMap("key", null)).command(GridRestCommand.CACHE_REMOVE_ALL)),
            n -> handle(new GridRestCacheRequest().cacheName(n).key("key").value("value").command(GridRestCommand.CACHE_REPLACE)),
            n -> handle(new GridRestCacheRequest().cacheName(n).key("key").value("value").command(GridRestCommand.CACHE_PUT_IF_ABSENT)),
            n -> handle(new GridRestCacheRequest().cacheName(n).key("key").value("value").command(GridRestCommand.CACHE_GET_AND_PUT)),
            n -> handle(new GridRestCacheRequest().cacheName(n).key("key").command(GridRestCommand.CACHE_GET_AND_REMOVE)),
            n -> handle(new GridRestCacheRequest().cacheName(n).key("key").value("value").command(GridRestCommand.CACHE_GET_AND_REPLACE))
        );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid(getConfiguration()).cluster().active(true);
        hnd = new GridCacheCommandHandler(((IgniteKernal)grid()).context());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
    }
}
