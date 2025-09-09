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

package org.apache.ignite.internal.client.thin.events;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.events.ConnectionDescription;
import org.apache.ignite.client.events.RequestEvent;
import org.apache.ignite.client.events.RequestEventListener;
import org.apache.ignite.client.events.RequestFailEvent;
import org.apache.ignite.client.events.RequestStartEvent;
import org.apache.ignite.client.events.RequestSuccessEvent;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.client.thin.AbstractThinClientTest;
import org.apache.ignite.internal.client.thin.ClientOperation;
import org.junit.Test;

/**
 * Tests query event listeners of a thin client.
 */
public class IgniteClientRequestEventListenerTest extends AbstractThinClientTest {
    /** */
    Map<Class<? extends RequestEvent>, RequestEvent> evSet = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected ClientConfiguration getClientConfiguration() {
        return super.getClientConfiguration()
            .setEventListeners(new RequestEventListener() {
                @Override public void onRequestStart(RequestStartEvent event) {
                    if (!isImplicitOperation(event.operationCode()))
                        evSet.put(event.getClass(), event);
                }

                @Override public void onRequestSuccess(RequestSuccessEvent event) {
                    if (!isImplicitOperation(event.operationCode()))
                        evSet.put(event.getClass(), event);
                }

                @Override public void onRequestFail(RequestFailEvent event) {
                    if (!isImplicitOperation(event.operationCode()))
                        evSet.put(event.getClass(), event);
                }
            });
    }

    /** */
    private boolean isImplicitOperation(short code) {
        return code == ClientOperation.GET_BINARY_CONFIGURATION.code()
            || code == ClientOperation.CACHE_PARTITIONS.code()
            || code == ClientOperation.CLUSTER_GROUP_GET_NODE_ENDPOINTS.code();
    }

    /** */
    @Test
    public void testQuerySuccessEvents() {
        long startTime = System.nanoTime();
        try (IgniteClient cli = startClient(0)) {
            cli.cacheNames();

            assertEquals(2, evSet.size());

            RequestStartEvent startEvt = (RequestStartEvent)evSet.get(RequestStartEvent.class);

            assertTrue(startEvt.requestId() >= 0);

            ConnectionDescription connDesc = startEvt.connectionDescription();
            assertEquals(clientHost(grid(0).localNode()), connDesc.remoteAddress().getAddress().getHostAddress());
            assertEquals(clientPort(grid(0).localNode()), connDesc.remoteAddress().getPort());
            assertEquals(clientHost(grid(0).localNode()), connDesc.localAddress().getAddress().getHostAddress());
            assertEquals(grid(0).localNode().id(), connDesc.serverNodeId());
            assertEquals(ClientOperation.CACHE_GET_NAMES.code(), startEvt.operationCode());
            assertEquals(ClientOperation.CACHE_GET_NAMES.name(), startEvt.operationName());

            RequestSuccessEvent successEvt = (RequestSuccessEvent)evSet.get(RequestSuccessEvent.class);
            assertEquals(successEvt.requestId(), successEvt.requestId());

            connDesc = startEvt.connectionDescription();
            assertEquals(clientHost(grid(0).localNode()), connDesc.remoteAddress().getAddress().getHostAddress());
            assertEquals(clientPort(grid(0).localNode()), connDesc.remoteAddress().getPort());
            assertEquals(clientHost(grid(0).localNode()), connDesc.localAddress().getAddress().getHostAddress());
            assertEquals(grid(0).localNode().id(), connDesc.serverNodeId());
            assertEquals(ClientOperation.CACHE_GET_NAMES.code(), startEvt.operationCode());
            assertEquals(ClientOperation.CACHE_GET_NAMES.name(), startEvt.operationName());

            assertTrue(System.nanoTime() - startTime >= successEvt.elapsedTime(TimeUnit.NANOSECONDS));

        }
    }

    /** */
    @Test
    public void testQueryFailEvents() {
        long startTime = System.nanoTime();
        try (IgniteClient cli = startClient(0)) {
            cli.cache("non-existent").put(1, 1);

            fail();
        }
        catch (ClientException err) {
            assertEquals(2, evSet.size());

            RequestStartEvent startEvt = (RequestStartEvent)evSet.get(RequestStartEvent.class);

            assertTrue(startEvt.requestId() >= 0);

            ConnectionDescription connDesc = startEvt.connectionDescription();
            assertEquals(clientHost(grid(0).localNode()), connDesc.remoteAddress().getAddress().getHostAddress());
            assertEquals(clientPort(grid(0).localNode()), connDesc.remoteAddress().getPort());
            assertEquals(clientHost(grid(0).localNode()), connDesc.localAddress().getAddress().getHostAddress());
            assertEquals(grid(0).localNode().id(), connDesc.serverNodeId());
            assertEquals(ClientOperation.CACHE_PUT.code(), startEvt.operationCode());
            assertEquals(ClientOperation.CACHE_PUT.name(), startEvt.operationName());

            RequestFailEvent failEvt = (RequestFailEvent)evSet.get(RequestFailEvent.class);
            assertEquals(failEvt.requestId(), failEvt.requestId());

            connDesc = startEvt.connectionDescription();
            assertEquals(clientHost(grid(0).localNode()), connDesc.remoteAddress().getAddress().getHostAddress());
            assertEquals(clientPort(grid(0).localNode()), connDesc.remoteAddress().getPort());
            assertEquals(clientHost(grid(0).localNode()), connDesc.localAddress().getAddress().getHostAddress());
            assertEquals(grid(0).localNode().id(), connDesc.serverNodeId());
            assertEquals(ClientOperation.CACHE_PUT.code(), startEvt.operationCode());
            assertEquals(ClientOperation.CACHE_PUT.name(), startEvt.operationName());

            assertEquals(err, failEvt.throwable());

            assertTrue(System.nanoTime() - startTime >= failEvt.elapsedTime(TimeUnit.NANOSECONDS));
        }
    }
}
