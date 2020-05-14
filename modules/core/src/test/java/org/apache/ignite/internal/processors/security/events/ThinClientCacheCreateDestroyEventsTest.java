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

import java.util.Collection;
import java.util.function.Consumer;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_CACHE_STARTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_STOPPED;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_DESTROY;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/**
 * Test that a local listener and a remote filter get correct subjectId when
 * a thin client create or destroy a cache.
 */
@SuppressWarnings("rawtypes")
public class ThinClientCacheCreateDestroyEventsTest extends AbstractSecurityCacheEventTest {
    /** Client. */
    private static final String CLIENT = "client";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(getConfiguration("srv", new TestSecurityPluginProvider("srv", null, ALLOW_ALL, false,
            new TestSecurityData(CLIENT,
                SecurityPermissionSetBuilder.create().defaultAllowAll(false)
                    .appendSystemPermissions(CACHE_CREATE, CACHE_DESTROY)
                    .build()))
        )).cluster().state(ClusterState.ACTIVE);
    }

    /** */
    @Test
    public void testCreateCacheEvent() throws Exception {
        testCacheEvents(2, CLIENT, EVT_CACHE_STARTED, cacheConfigurations(1),
            operation(true));
    }

    /** */
    @Test
    public void testGetOrCreateCacheEvent() throws Exception {
        testCacheEvents(2, CLIENT, EVT_CACHE_STARTED, cacheConfigurations(1),
            operation(true, true));
    }

    /** */
    @Test
    public void testDestroyCacheEvent() throws Exception {
        testCacheEvents(2, CLIENT, EVT_CACHE_STOPPED, cacheConfigurations(1),
            operation(false));
    }

    /** */
    private Consumer<Collection<CacheConfiguration>> operation(boolean isCreate) {
        return operation(isCreate, false);
    }

    /** */
    private Consumer<Collection<CacheConfiguration>> operation(boolean isCreate, boolean isGetOrCreate) {
        return ccfgs -> {
            try (IgniteClient clnt = startClient()) {
                if (isCreate) {
                    ClientCacheConfiguration cfg = ccfgs.stream().findFirst()
                        .map(c -> new ClientCacheConfiguration().setName(c.getName()))
                        .orElseThrow(IllegalStateException::new);

                    if (isGetOrCreate)
                        clnt.getOrCreateCache(cfg);
                    else
                        clnt.createCache(cfg);
                }
                else {
                    clnt.destroyCache(
                        ccfgs.stream().findFirst()
                            .map(CacheConfiguration::getName)
                            .orElseThrow(IllegalStateException::new)
                    );
                }
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    /**
     * @return Thin client for specified user.
     */
    private IgniteClient startClient() {
        return Ignition.startClient(
            new ClientConfiguration().setAddresses(Config.SERVER)
                .setUserName(CLIENT)
                .setUserPassword("")
        );
    }
}
