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

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.junit.Test;

import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_DESTROY;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/**
 *
 */
public class ThinClientCacheEventTest extends AbstractSecurityTest {
    /** Client. */
    private static final String CLIENT = "client";

    /** Cache. */
    protected static final String CACHE = "TEST_CACHE";

    /** */
    IgniteBiPredicate<UUID, Event> locLsnr = new IgniteBiPredicate<UUID, Event>() {
        /** */
        @IgniteInstanceResource
        private IgniteEx local;

        /** */
        @Override public boolean apply(UUID uuid, Event evt) {
            CacheEvent cacheEvt = (CacheEvent)evt;

            try {
                SecuritySubject subj = local.context().security().authenticatedSubject(cacheEvt.subjectId());

                /*System.out.println(
                    "MY_DEBUG localListener loc=" + local.localNode().id() +
                        ", subjId=" + subj.id() +
                        ", login=" + subj.login() +
                        ", evtNode=" + cacheEvt.eventNode().id()
                );*/

                assertEquals(subj.login(), CLIENT);

                return true;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }
    };

    /** */
    IgnitePredicate<Event> rmtFilter = new IgnitePredicate<Event>() {
        /** */
        @IgniteInstanceResource
        private IgniteEx local;

        /** */
        @Override public boolean apply(Event evt) {
            CacheEvent cacheEvt = (CacheEvent)evt;

            try {
                SecuritySubject subj = local.context().security().authenticatedSubject(cacheEvt.subjectId());

                /*System.out.println(
                    "MY_DEBUG remoteListener local=" + local.localNode().id() +
                        ", subjId=" + subj.id() +
                        ", login=" + subj.login() +
                        ", evtNode=" + cacheEvt.eventNode().id()
                );*/

                assertEquals(subj.login(), CLIENT);

                return true;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }
    };

    /** */
    @Test
    public void testCachCreateEvent() throws Exception {
        UUID lsnrId = grid("srv")
            .events()
            .remoteListen(locLsnr, rmtFilter, EventType.EVT_CACHE_STARTED);

        try (IgniteClient clnt = startClient()) {
            clnt.createCache(CACHE);
        }
        finally {
            grid("srv")
                .events()
                .stopRemoteListen(lsnrId);
        }
    }

    /** */
    @Test
    public void testCacheDestroyEvent() throws Exception {
        grid("srv").getOrCreateCache(CACHE);

        UUID lsnrId = grid("srv")
            .events()
            .remoteListen(locLsnr, rmtFilter, EventType.EVT_CACHE_STOPPED);

        try (IgniteClient clnt = startClient()) {
            clnt.destroyCache(CACHE);
        }
        finally {
            grid("srv")
                .events()
                .stopRemoteListen(lsnrId);
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(getConfiguration(0,
            new TestSecurityData(CLIENT,
                SecurityPermissionSetBuilder.create().defaultAllowAll(false)
                    .appendCachePermissions(CACHE, CACHE_CREATE, CACHE_DESTROY)
                    .build())
        ));

        startGridAllowAll("srv").cluster().state(ClusterState.ACTIVE);
    }

    /**
     * @param idx Index.
     * @param clientData Array of client security data.
     */
    private IgniteConfiguration getConfiguration(int idx, TestSecurityData... clientData) throws Exception {
        String instanceName = "srv_" + idx;

        return getConfiguration(instanceName, new TestSecurityPluginProvider(instanceName, null,
            ALLOW_ALL, false, clientData));

    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setIncludeEventTypes(EventType.EVTS_CACHE_LIFECYCLE);
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
