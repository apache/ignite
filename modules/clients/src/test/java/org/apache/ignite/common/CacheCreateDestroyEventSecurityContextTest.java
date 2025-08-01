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

package org.apache.ignite.common;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.util.lang.RunnableX;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.events.EventType.EVTS_CACHE_LIFECYCLE;
import static org.apache.ignite.events.EventType.EVT_CACHE_STARTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_STOPPED;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CLUSTER_SET_STATE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.DESTROY_CACHE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.GET_OR_CREATE_CACHE;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALL_PERMISSIONS;

/** Tests that security information specified in cache create/destroy events belongs to the operation initiator. */
public class CacheCreateDestroyEventSecurityContextTest extends AbstractEventSecurityContextTest {
    /** Counter of the created cache. */
    private static final AtomicInteger CACHE_COUNTER = new AtomicInteger();
    
    /** */
    private String operationInitiatorLogin;

    /** {@inheritDoc} */
    @Override protected int[] eventTypes() {
        return EVTS_CACHE_LIFECYCLE.clone();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridAllowAll("crd");
        startGridAllowAll("srv");
        startClientAllowAll("cli");
    }

    /** Tests cache create/destroy event security context in case operation is initiated from the {@link IgniteClient}. */
    @Test
    public void testIgniteClient() throws Exception {
        operationInitiatorLogin = THIN_CLIENT_LOGIN;

        ClientConfiguration cfg = new ClientConfiguration()
            .setAddresses(Config.SERVER)
            .setUserName(operationInitiatorLogin)
            .setUserPassword("");

        ClientCacheConfiguration ccfg = clientCacheConfiguration();

        try (IgniteClient cli = Ignition.startClient(cfg)) {
            checkCacheEvents(() -> cli.createCache(ccfg), EVT_CACHE_STARTED);
            checkCacheEvents(() -> cli.destroyCache(ccfg.getName()), EVT_CACHE_STOPPED);

            checkCacheEvents(() -> cli.createCacheAsync(ccfg).get(), EVT_CACHE_STARTED);
            checkCacheEvents(() -> cli.destroyCacheAsync(ccfg.getName()).get(), EVT_CACHE_STOPPED);

            checkCacheEvents(() -> cli.getOrCreateCache(clientCacheConfiguration()), EVT_CACHE_STARTED);
            checkCacheEvents(() -> cli.getOrCreateCacheAsync(clientCacheConfiguration()).get(), EVT_CACHE_STARTED);

            checkCacheEvents(() -> cli.cluster().state(INACTIVE), EVT_CACHE_STOPPED);
            checkCacheEvents(() -> cli.cluster().state(ACTIVE), EVT_CACHE_STARTED);
        }
    }

    /** Tests cache create/destroy event security context in case operation is initiated from the REST client. */
    @Test
    public void testRestClient() throws Exception {
        String cacheName = "rest_client_cache";

        operationInitiatorLogin = REST_CLIENT_LOGIN;

        checkCacheEvents(() -> sendRestRequest(GET_OR_CREATE_CACHE, cacheName, null), EVT_CACHE_STARTED);
        checkCacheEvents(() -> sendRestRequest(DESTROY_CACHE, cacheName, null), EVT_CACHE_STOPPED);

        grid("crd").createCache(cacheConfiguration());

        checkCacheEvents(() -> sendRestRequest(CLUSTER_SET_STATE, null, INACTIVE), EVT_CACHE_STOPPED);
        checkCacheEvents(() -> sendRestRequest(CLUSTER_SET_STATE, null, ACTIVE), EVT_CACHE_STARTED);
    }

    /** Tests cache create/destroy event security context in case operation is initiated from the server node. */
    @Test
    public void testServerNode() throws Exception {
        testNode(false);
    }

    /** Tests cache create/destroy event security context in case operation is initiated from the client node. */
    @Test
    public void testClientNode() throws Exception {
        testNode(true);
    }

    /** */
    private void testNode(boolean isClient) throws Exception {
        operationInitiatorLogin = isClient ? "cli" : "crd";

        Ignite ignite = grid(operationInitiatorLogin);

        CacheConfiguration<?, ?> ccfg = cacheConfiguration();

        checkCacheEvents(() -> ignite.createCache(ccfg), EVT_CACHE_STARTED);
        checkCacheEvents(() -> ignite.destroyCache(ccfg.getName()), EVT_CACHE_STOPPED);

        checkCacheEvents(() -> ignite.createCaches(singletonList(ccfg)), EVT_CACHE_STARTED);
        checkCacheEvents(() -> ignite.destroyCaches(singletonList(ccfg.getName())), EVT_CACHE_STOPPED);

        checkCacheEvents(() -> ignite.getOrCreateCache(cacheConfiguration()), EVT_CACHE_STARTED);

        checkCacheEvents(() -> ignite.cluster().state(INACTIVE), EVT_CACHE_STOPPED);
        checkCacheEvents(() -> ignite.cluster().state(ACTIVE), EVT_CACHE_STARTED);

        operationInitiatorLogin = "joining_" + (isClient ? "client_" : "server_") + "node";

        checkCacheEvents(
            () -> startGrid(getConfiguration(
                operationInitiatorLogin,
                    new TestSecurityPluginProvider(operationInitiatorLogin, "", ALL_PERMISSIONS, false))
                .setClientMode(isClient)
                .setCacheConfiguration(cacheConfiguration()))
                .close(),
            EVT_CACHE_STARTED);
    }

    /** */
    private ClientCacheConfiguration clientCacheConfiguration() {
        return new ClientCacheConfiguration()
            .setName("test-client=cache-" + CACHE_COUNTER.getAndIncrement());
    }

    /** */
    private CacheConfiguration<?, ?> cacheConfiguration() {
        return new CacheConfiguration<>("test-cache-" + CACHE_COUNTER.getAndIncrement());
    }

    /** */
    private void sendRestRequest(GridRestCommand cmd, String cacheName, ClusterState state) throws Exception {
        List<String> params = new ArrayList<>();

        if (cacheName != null)
            params.add("cacheName=" + cacheName);

        if (state != null) {
            params.add("force=true");
            params.add("state=" + state.name());
        }

        sendRestRequest(cmd, params, operationInitiatorLogin);
    }

    /** */
    private void checkCacheEvents(RunnableX op, int expEvtType) throws Exception {
        checkEvents(op, singletonList(expEvtType), operationInitiatorLogin);
    }
}
