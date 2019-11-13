/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent.processor;

import java.util.Collections;
import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.agent.ws.WebSocketManager;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cluster.IgniteClusterImpl;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cluster.ClusterProcessor;
import org.apache.ignite.testframework.GridTestNode;
import org.springframework.messaging.simp.stomp.StompSession;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Abstract service test.
 */
public abstract class AbstractServiceTest {
    /** Is session connected. */
    protected boolean isSesConnected = true;

    /** Session. */
    protected StompSession ses = mock(StompSession.class);

    /** Websocket manager. */
    protected WebSocketManager mgr = getMockWebSocketManager();

    /**
     * @return Mocked web socket manager.
     */
    protected WebSocketManager getMockWebSocketManager() {
        WebSocketManager mgr = mock(WebSocketManager.class);

        when(ses.isConnected()).thenAnswer(i -> isSesConnected);

        return mgr;
    }

    /**
     * @return Mocked grid kernal context.
     */
    protected GridKernalContext getMockContext() {
        GridKernalContext ctx = mock(GridKernalContext.class);

        when(ctx.log(any(Class.class))).thenReturn(mock(IgniteLogger.class));

        ClusterProcessor clusterProcessor = mock(ClusterProcessor.class);

        IgniteEx grid = mock(IgniteEx.class);

        IgniteMessaging messaging = mock(IgniteMessaging.class);

        IgniteClusterImpl cluster = mock(IgniteClusterImpl.class);

        when(cluster.id()).thenReturn(UUID.fromString("a-a-a-a-a"));
        when(cluster.tag()).thenReturn("Test tag");
        when(ctx.grid()).thenReturn(grid);
        when(ctx.cluster()).thenReturn(clusterProcessor);
        when(grid.cluster()).thenReturn(cluster);
        when(clusterProcessor.get()).thenReturn(cluster);
        when(grid.message()).thenReturn(messaging);
        when(grid.message(any(ClusterGroup.class))).thenReturn(messaging);

        GridDiscoveryManager disco = mock(GridDiscoveryManager.class);

        DiscoCache discoCache = mock(DiscoCache.class);

        when(discoCache.version()).thenReturn(new AffinityTopologyVersion(1L));
        when(discoCache.allNodes()).thenReturn(Collections.singletonList(new GridTestNode(UUID.fromString("b-b-b-b-b"))));
        when(disco.discoCache()).thenReturn(discoCache);
        when(ctx.discovery()).thenReturn(disco);

        return ctx;
    }
}
