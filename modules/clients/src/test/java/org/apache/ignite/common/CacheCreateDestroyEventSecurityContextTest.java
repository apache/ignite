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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.EventAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.AbstractTestSecurityPluginProvider;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityCredentialsBasicProvider;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.testframework.GridTestUtils.RunnableX;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.common.ComputeTaskRemoteSecurityContextTest.DFLT_REST_PORT;
import static org.apache.ignite.events.EventType.EVTS_CACHE_LIFECYCLE;
import static org.apache.ignite.events.EventType.EVT_CACHE_STARTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_STOPPED;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CLUSTER_SET_STATE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.DESTROY_CACHE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.GET_OR_CREATE_CACHE;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** Tests that security information specified in cache create/destroy events belongs to the operation initiator. */
public class CacheCreateDestroyEventSecurityContextTest extends AbstractSecurityTest {
    /** Events paired with the nodes on which they were listened to. */
    private static final Map<ClusterNode, Collection<CacheEvent>> LISTENED_CACHE_EVTS = new HashMap<>();

    /** Counter of the created cache. */
    private static final AtomicInteger CACHE_COUNTER = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        LISTENED_CACHE_EVTS.put(startGridAllowAll("crd").localNode(), ConcurrentHashMap.newKeySet());
        LISTENED_CACHE_EVTS.put(startGridAllowAll("srv").localNode(), ConcurrentHashMap.newKeySet());

        startClientAllowAll("cli");
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(
        String instanceName,
        AbstractTestSecurityPluginProvider pluginProv
    ) throws Exception {
        return super.getConfiguration(instanceName, pluginProv)
            .setLocalHost("127.0.0.1")
            .setIncludeEventTypes(EVTS_CACHE_LIFECYCLE)
            .setConnectorConfiguration(new ConnectorConfiguration()
                .setJettyPath("modules/clients/src/test/resources/jetty/rest-jetty.xml"));
    }

    /** {@inheritDoc} */
    @Override protected IgniteEx startGrid(String login, SecurityPermissionSet prmSet, boolean isClient) throws Exception {
        IgniteEx ignite = startGrid(login, prmSet, null, isClient);

        if (!isClient) {
            ignite.events().localListen(evt -> {
                LISTENED_CACHE_EVTS.get(evt.node()).add((CacheEvent)evt);

                return true;
            }, EVTS_CACHE_LIFECYCLE);
        }

        return ignite;
    }

    /** Tests cache create/destroy event security context in case operation is initiated from the {@link IgniteClient}. */
    @Test
    public void testIgniteClient() throws Exception {
        String login = "thin_client";

        ClientConfiguration cfg = new ClientConfiguration()
            .setAddresses(Config.SERVER)
            .setUserName(login)
            .setUserPassword("");

        ClientCacheConfiguration ccfg = clientCacheConfiguration();

        try (IgniteClient cli = Ignition.startClient(cfg)) {
            checkCacheEvents(() -> cli.createCache(ccfg), EVT_CACHE_STARTED, login);
            checkCacheEvents(() -> cli.destroyCache(ccfg.getName()), EVT_CACHE_STOPPED, login);

            checkCacheEvents(() -> cli.createCacheAsync(ccfg).get(), EVT_CACHE_STARTED, login);
            checkCacheEvents(() -> cli.destroyCacheAsync(ccfg.getName()).get(), EVT_CACHE_STOPPED, login);

            checkCacheEvents(() -> cli.getOrCreateCache(clientCacheConfiguration()), EVT_CACHE_STARTED, login);
            checkCacheEvents(() -> cli.getOrCreateCacheAsync(clientCacheConfiguration()).get(), EVT_CACHE_STARTED, login);

            checkCacheEvents(() -> cli.cluster().state(INACTIVE), EVT_CACHE_STOPPED, login);
            checkCacheEvents(() -> cli.cluster().state(ACTIVE), EVT_CACHE_STARTED, login);
        }
    }

    /** Tests cache create/destroy event security context in case operation is initiated from the {@link GridClient}. */
    @Test
    public void testGridClient() throws Exception {
        String login = "grid_client";

        GridClientConfiguration cfg = new GridClientConfiguration()
            .setServers(singletonList("127.0.0.1:11211"))
            .setSecurityCredentialsProvider(new SecurityCredentialsBasicProvider(new SecurityCredentials(login, "")));

        grid("crd").createCache(cacheConfiguration());

        try (GridClient cli = GridClientFactory.start(cfg)) {
            checkCacheEvents(() -> cli.state().state(INACTIVE, true), EVT_CACHE_STOPPED, login);
            checkCacheEvents(() -> cli.state().state(ACTIVE, true), EVT_CACHE_STARTED, login);
        }
    }

    /** Tests cache create/destroy event security context in case operation is initiated from the REST client. */
    @Test
    public void testRestClient() throws Exception {
        String cacheName = "rest_client_cache";

        String login = "rest_client";

        checkCacheEvents(() -> sendRestRequest(login, GET_OR_CREATE_CACHE, cacheName, null), EVT_CACHE_STARTED, login);
        checkCacheEvents(() -> sendRestRequest(login, DESTROY_CACHE, cacheName, null), EVT_CACHE_STOPPED, login);

        grid("crd").createCache(cacheConfiguration());

        checkCacheEvents(() -> sendRestRequest(login, CLUSTER_SET_STATE, null, INACTIVE), EVT_CACHE_STOPPED, login);
        checkCacheEvents(() -> sendRestRequest(login, CLUSTER_SET_STATE, null, ACTIVE), EVT_CACHE_STARTED, login);
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
        String login = isClient ? "cli" : "crd";

        Ignite ignite = grid(login);

        CacheConfiguration<?, ?> ccfg = cacheConfiguration();

        checkCacheEvents(() -> ignite.createCache(ccfg), EVT_CACHE_STARTED, login);
        checkCacheEvents(() -> ignite.destroyCache(ccfg.getName()), EVT_CACHE_STOPPED, login);

        checkCacheEvents(() -> ignite.createCaches(singletonList(ccfg)), EVT_CACHE_STARTED, login);
        checkCacheEvents(() -> ignite.destroyCaches(singletonList(ccfg.getName())), EVT_CACHE_STOPPED, login);

        checkCacheEvents(() -> ignite.getOrCreateCache(cacheConfiguration()), EVT_CACHE_STARTED, login);

        String joiningNodeLogin = "joining_" + (isClient ? "client_" : "server_") + "node";

        checkCacheEvents(
            () -> startGrid(getConfiguration(
                    joiningNodeLogin,
                    new TestSecurityPluginProvider(joiningNodeLogin, "", ALLOW_ALL, false))
                .setClientMode(isClient)
                .setCacheConfiguration(cacheConfiguration()))
                .close(),
            EVT_CACHE_STARTED,
            joiningNodeLogin);

        checkCacheEvents(() -> ignite.cluster().state(INACTIVE), EVT_CACHE_STOPPED, login);
        checkCacheEvents(() -> ignite.cluster().state(ACTIVE), EVT_CACHE_STARTED, login);
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
    private void sendRestRequest(String login, GridRestCommand cmd, String cacheName, ClusterState state) throws Exception {
        String req = "http://127.0.0.1:" + DFLT_REST_PORT + "/ignite" +
            "?ignite.login=" + login +
            "&ignite.password=" +
            "&cmd=" + cmd.key();

        if (cacheName != null)
            req += "&cacheName=" + cacheName;

        if (state != null)
            req += "&force=true&state=" + state.name();

        ComputeTaskRemoteSecurityContextTest.sendRestRequest(req);
    }

    /** */
    private void checkCacheEvents(RunnableX op, int expEvtType, String expLogin) throws Exception {
        LISTENED_CACHE_EVTS.values().forEach(Collection::clear);

        op.run();

        for (Collection<CacheEvent> nodeCacheEvts : LISTENED_CACHE_EVTS.values()) {
            assertTrue(waitForCondition(() ->
                nodeCacheEvts.stream()
                    .map(EventAdapter::type)
                    .collect(Collectors.toList())
                    .contains(expEvtType),
                getTestTimeout()));

            assertTrue(nodeCacheEvts.stream().map(evt -> {
                try {
                    return grid("crd").context().security().authenticatedSubject(evt.subjectId()).login();
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }).allMatch(expLogin::equals));
        }
    }
}
