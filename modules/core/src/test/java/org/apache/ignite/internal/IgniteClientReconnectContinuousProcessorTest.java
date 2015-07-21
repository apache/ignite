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

package org.apache.ignite.internal;

import org.apache.ignite.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;

import javax.cache.event.*;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.events.EventType.*;

/**
 *
 */
public class IgniteClientReconnectContinuousProcessorTest extends IgniteClientReconnectAbstractTest {
    /** */
    private static volatile CountDownLatch latch;

    /** {@inheritDoc} */
    @Override protected int serverCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected int clientCount() {
        return 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testEventListenerReconnect() throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        Ignite srv = clientRouter(client);

        TestTcpDiscoverySpi srvSpi = spi(srv);

        EventListener lsnr = new EventListener();

        UUID opId = client.events().remoteListen(lsnr, null, EventType.EVT_JOB_STARTED);

        lsnr.latch = new CountDownLatch(1);

        log.info("Created remote listener: " + opId);

        final CountDownLatch reconnectLatch = new CountDownLatch(1);

        client.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                if (evt.type() == EVT_CLIENT_NODE_RECONNECTED) {
                    info("Reconnected: " + evt);

                    reconnectLatch.countDown();
                }

                return true;
            }
        }, EVT_CLIENT_NODE_RECONNECTED);

        srvSpi.failNode(client.cluster().localNode().id(), null);

        waitReconnectEvent(reconnectLatch);

        client.compute().run(new DummyJob());

        assertTrue(lsnr.latch.await(5000, MILLISECONDS));

        lsnr.latch = new CountDownLatch(1);

        srv.compute().run(new DummyJob());

        assertTrue(lsnr.latch.await(5000, MILLISECONDS));

        lsnr.latch = new CountDownLatch(1);

        log.info("Stop listen, should not get events anymore.");

        client.events().stopRemoteListen(opId);

        assertFalse(lsnr.latch.await(3000, MILLISECONDS));
    }

    /**
     * @throws Exception If failed.
     */
    public void testMessageListenerReconnect() throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        Ignite srv = clientRouter(client);

        TestTcpDiscoverySpi srvSpi = spi(srv);

        final String topic = "testTopic";

        MessageListener locLsnr  = new MessageListener();

        UUID opId = client.message().remoteListen(topic, new RemoteMessageListener());

        client.message().localListen(topic, locLsnr);

        final CountDownLatch reconnectLatch = new CountDownLatch(1);

        client.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                if (evt.type() == EVT_CLIENT_NODE_RECONNECTED) {
                    info("Reconnected: " + evt);

                    reconnectLatch.countDown();
                }

                return true;
            }
        }, EVT_CLIENT_NODE_RECONNECTED);

        srvSpi.failNode(client.cluster().localNode().id(), null);

        waitReconnectEvent(reconnectLatch);

        locLsnr.latch = new CountDownLatch(1);
        latch = new CountDownLatch(2);

        client.message().send(topic, "msg1");

        assertTrue(locLsnr.latch.await(5000, MILLISECONDS));
        assertTrue(latch.await(5000, MILLISECONDS));

        locLsnr.latch = new CountDownLatch(1);
        latch = new CountDownLatch(2);

        srv.message().send(topic, "msg2");

        assertTrue(locLsnr.latch.await(5000, MILLISECONDS));
        assertTrue(latch.await(5000, MILLISECONDS));

        log.info("Stop listen, should not get remote messages anymore.");

        client.message().stopRemoteListen(opId);

        srv.message().send(topic, "msg3");

        locLsnr.latch = new CountDownLatch(1);
        latch = new CountDownLatch(1);

        assertTrue(locLsnr.latch.await(5000, MILLISECONDS));
        assertFalse(latch.await(3000, MILLISECONDS));
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheContinuousQueryReconnect() throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        IgniteCache<Object, Object> clientCache = client.getOrCreateCache(new CacheConfiguration<>());

        CacheEventListener lsnr = new CacheEventListener();

        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

        qry.setAutoUnsubscribe(true);

        qry.setLocalListener(lsnr);

        QueryCursor<?> cur = clientCache.query(qry);

        for (int i = 0; i < 5; i++) {
            log.info("Iteration: " + i);

            continuousQueryReconnect(client, clientCache, lsnr);
        }

        log.info("Close cursor, should not get cache events anymore.");

        cur.close();

        lsnr.latch = new CountDownLatch(1);

        clientCache.put(3, 3);

        assertFalse(lsnr.latch.await(3000, MILLISECONDS));
    }

    /**
     * @param client Client.
     * @param clientCache Client cache.
     * @param lsnr Continuous query listener.
     * @throws Exception If failed.
     */
    private void continuousQueryReconnect(Ignite client,
        IgniteCache<Object, Object> clientCache,
        CacheEventListener lsnr)
        throws Exception
    {
        Ignite srv = clientRouter(client);

        TestTcpDiscoverySpi srvSpi = spi(srv);

        final CountDownLatch reconnectLatch = new CountDownLatch(1);

        IgnitePredicate<Event> p = new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                if (evt.type() == EVT_CLIENT_NODE_RECONNECTED) {
                    info("Reconnected: " + evt);

                    reconnectLatch.countDown();
                }

                return true;
            }
        };

        client.events().localListen(p, EVT_CLIENT_NODE_RECONNECTED);

        srvSpi.failNode(client.cluster().localNode().id(), null);

        waitReconnectEvent(reconnectLatch);

        client.events().stopLocalListen(p);

        lsnr.latch = new CountDownLatch(1);

        clientCache.put(1, 1);

        assertTrue(lsnr.latch.await(5000, MILLISECONDS));

        lsnr.latch = new CountDownLatch(1);

        srv.cache(null).put(2, 2);

        assertTrue(lsnr.latch.await(5000, MILLISECONDS));
    }

    /**
     *
     */
    private static class EventListener implements P2<UUID, Event> {
        /** */
        private volatile CountDownLatch latch;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public boolean apply(UUID uuid, Event evt) {
            assertTrue(ignite.cluster().localNode().isClient());

            ignite.log().info("Received event: " + evt);

            if (latch != null)
                latch.countDown();

            return true;
        }
    }

    /**
     *
     */
    private static class MessageListener implements P2<UUID, Object> {
        /** */
        private volatile CountDownLatch latch;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public boolean apply(UUID uuid, Object msg) {
            assertTrue(ignite.cluster().localNode().isClient());

            ignite.log().info("Local listener received message: " + msg);

            if (latch != null)
                latch.countDown();

            return true;
        }
    }

    /**
     *
     */
    private static class RemoteMessageListener implements P2<UUID, Object> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public boolean apply(UUID uuid, Object msg) {
            ignite.log().info("Remote listener received message: " + msg);

            if (latch != null)
                latch.countDown();

            return true;
        }
    }

    /**
     *
     */
    private static class CacheEventListener implements CacheEntryUpdatedListener<Object, Object> {
        /** */
        private volatile CountDownLatch latch;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> evts) {
            int cnt = 0;

            for (CacheEntryEvent<?, ?> evt : evts) {
                ignite.log().info("Received cache event: " + evt);

                cnt++;
            }

            assertEquals(1, cnt);

            if (latch != null)
                latch.countDown();
        }
    }

    /**
     *
     */
    static class DummyJob implements IgniteRunnable {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public void run() {
            ignite.log().info("Job run.");
        }
    }
}