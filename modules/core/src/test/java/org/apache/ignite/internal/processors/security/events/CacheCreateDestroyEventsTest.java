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

package org.apache.ignite.internal.processors.security.events;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.events.EventType.EVT_CACHE_STARTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_STOPPED;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/**
 * Test that an event's local listener and an event's remote filter get correct subjectId
 * when a server (client) node create or destroy a cache.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
@RunWith(Parameterized.class)
public class CacheCreateDestroyEventsTest extends AbstractSecurityCacheEventTest {
    /** */
    @Parameterized.Parameter()
    public int cacheCnt;

    /** */
    @Parameterized.Parameter(1)
    public String login;

    /** */
    @Parameterized.Parameter(2)
    public int evtType;

    /** */
    @Parameterized.Parameter(3)
    public int opNum;

    /** Parameters. */
    @Parameterized.Parameters(name = "cacheCnt={0},evtNode={1},evtType={2},opNum={3}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(
            new Object[] {1, SRV, EVT_CACHE_STARTED, 0},
            new Object[] {1, CLNT, EVT_CACHE_STARTED, 0},
            new Object[] {1, SRV, EVT_CACHE_STARTED, 1},
            new Object[] {1, CLNT, EVT_CACHE_STARTED, 1},
            new Object[] {1, SRV, EVT_CACHE_STOPPED, 2},
            new Object[] {1, CLNT, EVT_CACHE_STOPPED, 2},
            new Object[] {2, SRV, EVT_CACHE_STARTED, 3},
            new Object[] {2, CLNT, EVT_CACHE_STARTED, 3},
            new Object[] {2, SRV, EVT_CACHE_STOPPED, 4},
            new Object[] {2, CLNT, EVT_CACHE_STOPPED, 4},
            new Object[] {1, "thin", EVT_CACHE_STARTED, 5},
            new Object[] {1, "thin", EVT_CACHE_STARTED, 6},
            new Object[] {1, "thin", EVT_CACHE_STOPPED, 7},
            new Object[] {2, "new_client_node", EVT_CACHE_STARTED, 8},
            new Object[] {2, "new_server_node", EVT_CACHE_STARTED, 8}
        );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridAllowAll(SRV);
        startClientAllowAll(CLNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopGrid("new_client_node");
        stopGrid("new_server_node");
    }

    private List<Consumer<Collection<CacheConfiguration>>> operations() {
        return Arrays.asList(
            ccfgs -> grid(login).getOrCreateCache(ccfgs.iterator().next()),
            ccfgs -> grid(login).createCache(ccfgs.iterator().next()),
            ccfgs -> grid(login).destroyCache(ccfgs.iterator().next().getName()),
            ccfgs -> grid(login).createCaches(ccfgs),
            ccfgs -> grid(login).destroyCaches(ccfgs.stream().map(CacheConfiguration::getName).collect(Collectors.toSet())),
            ccfgs -> startClient().createCache(ccfgs.iterator().next().getName()),
            ccfgs -> startClient().getOrCreateCache(ccfgs.iterator().next().getName()),
            ccfgs -> startClient().destroyCache(ccfgs.iterator().next().getName()),
            ccfgs -> {
                try {
                    startGrid(getConfiguration(login,
                        new TestSecurityPluginProvider(login, "", ALLOW_ALL, false))
                        .setClientMode(login.contains("client"))
                        .setCacheConfiguration(ccfgs.toArray(new CacheConfiguration[0]))
                    );
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        );
    }

    /** */
    @Test
    public void testDynamicCreateDestroyCache() throws Exception {
        int expTimes = cacheCnt*2 +
            ((!login.equals(SRV) && !"thin".equals(login) && evtType == EVT_CACHE_STARTED) ? cacheCnt : 0);

        testCacheEvents(expTimes, login, evtType, cacheConfigurations(cacheCnt, evtType == EVT_CACHE_STOPPED),
            operations().get(opNum));
    }

    /**
     * @return Thin client for specified user.
     */
    private IgniteClient startClient() {
        return Ignition.startClient(
            new ClientConfiguration().setAddresses(Config.SERVER)
                .setUserName(login)
                .setUserPassword("")
        );
    }
}
