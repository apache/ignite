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

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_CACHE_STARTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_STOPPED;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/**
 * Test that a local listener and a remote filter get correct subjectId when
 * a server (client) node create or destroy a cache.
 */
@SuppressWarnings({"rawtypes", "unchecked", "ZeroLengthArrayAllocation"})
public class CacheCreateDestroyEventsTest extends AbstractSecurityCacheEventTest {
    /** Client. */
    private static final String CLNT = "client";

    /** Server. */
    private static final String SRV = "server";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridAllowAll(SRV);

        startClientAllowAll(CLNT);
    }

    /** */
    @Test
    public void testCreateCacheSrvEvt() throws Exception {
        testDynamicCreateDestroyCache(2, SRV, EVT_CACHE_STARTED);
    }

    /** */
    @Test
    public void testCreateCacheClntEvt() throws Exception {
        testDynamicCreateDestroyCache(3, CLNT, EVT_CACHE_STARTED);
    }

    /** */
    @Test
    public void testGetOrCreateCacheSrvEvt() throws Exception {
        testDynamicCreateDestroyCache(2, SRV, EVT_CACHE_STARTED, true);
    }

    /** */
    @Test
    public void testGetOrCreateCacheClntEvt() throws Exception {
        testDynamicCreateDestroyCache(3, CLNT, EVT_CACHE_STARTED, true);
    }

    /** */
    @Test
    public void testCreateBatchCachesSrvEvt() throws Exception {
        testDynamicCreateDestroyCacheBatch(4, SRV, EVT_CACHE_STARTED);
    }

    /** */
    @Test
    public void testCreateBatchCachesClntEvt() throws Exception {
        testDynamicCreateDestroyCacheBatch(6, CLNT, EVT_CACHE_STARTED);
    }

    /** */
    @Test
    public void testDestroyCacheSrvEvt() throws Exception {
        testDynamicCreateDestroyCache(2, SRV, EVT_CACHE_STOPPED);
    }

    /** */
    @Test
    public void testDestroyCacheClntEvt() throws Exception {
        testDynamicCreateDestroyCache(2, CLNT, EVT_CACHE_STOPPED);
    }

    /** */
    @Test
    public void testDestroyBatchCachesSrvEvt() throws Exception {
        testDynamicCreateDestroyCacheBatch(4, SRV, EVT_CACHE_STOPPED);
    }

    /** */
    @Test
    public void testDestroyBatchCachesClntEvt() throws Exception {
        testDynamicCreateDestroyCacheBatch(4, CLNT, EVT_CACHE_STOPPED);
    }

    /** */
    @Test
    public void testCreateCacheOnNodeJoinAsServer() throws Exception {
        testCreateCacheOnNodeJoin(false);
    }

    /** */
    @Test
    public void testCreateCacheOnNodeJoinAsClient() throws Exception {
        testCreateCacheOnNodeJoin(true);
    }

    /** */
    private void testCreateCacheOnNodeJoin(boolean isClient) throws Exception {
        final String login = "new_node_" + COUNTER.incrementAndGet();

        try {
            testCacheEvents(6, login, EVT_CACHE_STARTED, cacheConfigurations(2), ccfgs -> {
                try {
                    startGrid(
                        getConfiguration(login,
                            new TestSecurityPluginProvider(login, "", ALLOW_ALL, null, globalAuth))
                            .setClientMode(isClient)
                            .setCacheConfiguration(ccfgs.toArray(new CacheConfiguration[] {}))
                    );
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
        finally {
            stopGrid(login);
        }
    }

    /** */
    private void testDynamicCreateDestroyCacheBatch(int expTimes, String evtNode, int evtType) throws Exception {
        testCacheEvents(expTimes, evtNode, evtType, cacheConfigurations(2), ccfgs -> {
            if (evtType == EVT_CACHE_STARTED)
                grid(evtNode).createCaches(ccfgs);
            else
                grid(evtNode).destroyCaches(cacheNames(ccfgs));
        });
    }

    /** */
    private void testDynamicCreateDestroyCache(int expTimes, String evtNode, int evtType) throws Exception {
        testDynamicCreateDestroyCache(expTimes, evtNode, evtType, false);
    }

    /** */
    private void testDynamicCreateDestroyCache(int expTimes, String evtNode, int evtType, boolean isGetOrCreate) throws Exception {
        testCacheEvents(expTimes, evtNode, evtType, cacheConfigurations(1), ccfgs -> {
            CacheConfiguration cfg = ccfgs.stream().findFirst().orElseThrow(IllegalStateException::new);

            if (evtType == EVT_CACHE_STARTED) {
                if (isGetOrCreate)
                    grid(evtNode).getOrCreateCache(cfg);
                else
                    grid(evtNode).createCache(cfg);
            }
            else
                grid(evtNode).destroyCache(cfg.getName());
        });
    }
}
