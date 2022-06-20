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

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.URL;
import java.net.URLConnection;
import java.security.Permissions;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.CacheQueryExecutedEvent;
import org.apache.ignite.events.CacheQueryReadEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.JobEvent;
import org.apache.ignite.events.TaskEvent;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyObjectMapper;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.util.lang.RunnableX;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.resources.IgniteInstanceResource;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonMap;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** Represents utility class for testing security information contained in various events. */
public abstract class AbstractEventSecurityContextTest extends AbstractSecurityTest {
    /** Events paired with the nodes on which they were listened to. */
    protected static final Map<ClusterNode, Collection<Event>> LISTENED_EVTS = new ConcurrentHashMap<>();

    /** Custom object mapper for HTTP REST API.  */
    private static final ObjectMapper OBJECT_MAPPER = new GridJettyObjectMapper();

    /** Port for REST client connection. */
    private static final String DFLT_REST_PORT = "11080";

    /** {@inheritDoc} */
    @Override protected IgniteEx startGrid(
        String login,
        SecurityPermissionSet prmSet,
        Permissions sandboxPerms,
        boolean isClient
    ) throws Exception {
        IgniteConfiguration cfg = getConfiguration(
            login,
            new TestSecurityPluginProvider(login, "", prmSet, sandboxPerms, globalAuth));

        cfg.setClientMode(isClient);
        cfg.setLocalHost("127.0.0.1");
        cfg.setConnectorConfiguration(new ConnectorConfiguration()
            .setJettyPath("modules/clients/src/test/resources/jetty/rest-jetty.xml"));

        if (!isClient || includeClientNodes()) {
            cfg.setIncludeEventTypes(eventTypes());
            cfg.setLocalEventListeners(singletonMap(new IgnitePredicate<Event>() {
                    /** */
                    @IgniteInstanceResource
                    IgniteEx ignite;

                    /** {@inheritDoc} */
                    @Override public boolean apply(Event evt) {
                        LISTENED_EVTS.computeIfAbsent(ignite.localNode(), k -> ConcurrentHashMap.newKeySet()).add(evt);

                        return true;
                    }
                }, eventTypes())
            );
        }

        return startGrid(cfg);
    }

    /** Event types involved in testing. */
    protected abstract int[] eventTypes();

    /** Whether client nodes are included in testing. */
    protected boolean includeClientNodes() {
        return false;
    }

    /** */
    protected static JsonNode sendRestRequest(GridRestCommand cmd, Collection<String> params, String login) throws IOException {
        String url = "http://127.0.0.1:" + DFLT_REST_PORT + "/ignite" +
            "?ignite.login=" + login +
            "&ignite.password=" +
            "&cmd=" + cmd.key() +
            params.stream().collect(Collectors.joining("&", "&", ""));

        URLConnection conn = new URL(url).openConnection();

        StringBuilder buf = new StringBuilder(256);

        try (LineNumberReader rdr = new LineNumberReader(new InputStreamReader(conn.getInputStream(), UTF_8))) {
            for (String line = rdr.readLine(); line != null; line = rdr.readLine())
                buf.append(line);
        }

        return OBJECT_MAPPER.readTree(buf.toString());
    }

    /**
     * Checks that all expected events listened on the specified node contain security subject id that belongs to user
     * with specified login.
     */
    protected void checkEvents(
        ClusterNode node,
        Collection<Integer> expEvtTypes,
        String expLogin
    ) throws Exception {
        assertTrue(waitForCondition(() -> {
            Collection<Event> evts = LISTENED_EVTS.get(node);

            if (evts == null)
                return false;

            return evts.stream()
                .map(Event::type)
                .collect(Collectors.toList())
                .containsAll(expEvtTypes);
        }, getTestTimeout()));

        assertTrue(LISTENED_EVTS.get(node).stream()
            .map(evt -> {
                if (evt instanceof CacheEvent)
                    return ((CacheEvent)evt).subjectId();
                else if (evt instanceof CacheQueryExecutedEvent)
                    return ((CacheQueryExecutedEvent<?, ?>)evt).subjectId();
                else if (evt instanceof CacheQueryReadEvent)
                    return ((CacheQueryReadEvent<?, ?>)evt).subjectId();
                else if (evt instanceof TaskEvent)
                    return ((TaskEvent)evt).subjectId();
                else if (evt instanceof JobEvent)
                    return ((JobEvent)evt).taskSubjectId();
                else
                    throw new IgniteException();
            })
            .map(subjId -> {
                try {
                    return ((IgniteEx)grid(node)).context().security().authenticatedSubject(subjId).login();
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }).allMatch(expLogin::equals));
    }

    /** Executes specified operation and checks events on all nodes involved in testing. */
    protected void checkEvents(
        RunnableX op,
        Collection<Integer> expEvtTypes,
        String expLogin
    ) throws Exception {
        LISTENED_EVTS.values().forEach(Collection::clear);

        op.run();

        for (ClusterNode node : testNodes())
            checkEvents(node, expEvtTypes, expLogin);
    }

    /** @return Nodes involved in testing. */
    protected Collection<ClusterNode> testNodes() {
        return G.allGrids().stream()
            .map(ignite -> ignite.cluster().localNode())
            .filter(node -> !node.isClient() || includeClientNodes())
            .collect(Collectors.toList());
    }
}
