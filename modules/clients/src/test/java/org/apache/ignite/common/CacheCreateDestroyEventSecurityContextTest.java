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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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
import static org.apache.ignite.common.ComputeTaskRemoteSecurityContextTest.sendRestRequest;
import static org.apache.ignite.events.EventType.EVTS_CACHE_LIFECYCLE;
import static org.apache.ignite.events.EventType.EVT_CACHE_STARTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_STOPPED;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class CacheCreateDestroyEventSecurityContextTest extends AbstractSecurityTest {
    /** */
    private static final Map<UUID, Set<CacheEvent>> LISTENED_CACHE_EVTS = new HashMap<>();

    /** */
    private static final AtomicInteger CACHE_COUNTER = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        LISTENED_CACHE_EVTS.put(startGridAllowAll("crd").localNode().id(), ConcurrentHashMap.newKeySet());
        LISTENED_CACHE_EVTS.put(startGridAllowAll("srv").localNode().id(), ConcurrentHashMap.newKeySet());

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

        ignite.events().localListen(evt -> {
            LISTENED_CACHE_EVTS.get(evt.node().id()).add((CacheEvent)evt);

            return true;
        }, EVTS_CACHE_LIFECYCLE);

        return ignite;
    }

    /** */
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

    /** */
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

    /** */
    @Test
    public void testRestClient() throws Exception {
        String cacheName = "rest_client_cache";

        String createCacheLogin = "rest_client_create_cache";

        checkCacheEvents(() -> sendRestRequest("http://127.0.0.1:" + DFLT_REST_PORT + "/ignite" +
                "?ignite.login=" + createCacheLogin +
                "&ignite.password=" +
                "&cmd=" + GridRestCommand.GET_OR_CREATE_CACHE.key() +
                "&cacheName=" + cacheName),
            EVT_CACHE_STARTED,
            createCacheLogin);

        String destroyCacheLogin = "rest_client_destroy_cache";

        checkCacheEvents(() -> sendRestRequest("http://127.0.0.1:" + DFLT_REST_PORT + "/ignite" +
                "?ignite.login=" + destroyCacheLogin +
                "&ignite.password=" +
                "&cmd=" + GridRestCommand.DESTROY_CACHE.key() +
                "&cacheName=" + cacheName),
            EVT_CACHE_STOPPED,
            destroyCacheLogin);

        grid("crd").createCache(cacheConfiguration());

        String deactivateLogin = "rest_client_deactivate";

        checkCacheEvents(() -> sendRestRequest("http://127.0.0.1:" + DFLT_REST_PORT + "/ignite" +
                "?ignite.login=" + deactivateLogin +
                "&ignite.password=" +
                "&cmd=" + GridRestCommand.CLUSTER_SET_STATE.key() +
                "&force=true" +
                "&state=" + INACTIVE.name()),
            EVT_CACHE_STOPPED,
            deactivateLogin);

        String activateLogin = "rest_client_activate";

        checkCacheEvents(() -> sendRestRequest("http://127.0.0.1:" + DFLT_REST_PORT + "/ignite" +
                "?ignite.login=" + activateLogin +
                "&ignite.password=" +
                "&cmd=" + GridRestCommand.CLUSTER_SET_STATE.key() +
                "&force=true" +
                "&state=" + ACTIVE.name()),
            EVT_CACHE_STARTED,
            activateLogin);
    }

    /** */
    @Test
    public void testServerNode() throws Exception {
        testNode(false);
    }

    /** */
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
    private void checkCacheEvents(RunnableX op, int expEvtType, String expLogin) throws Exception {
        LISTENED_CACHE_EVTS.values().forEach(Set::clear);

        op.run();

        for (Set<CacheEvent> nodeCacheEvts : LISTENED_CACHE_EVTS.values()) {
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
