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

package org.apache.ignite.internal.processors.service;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Service proxy test with client reconnect.
 */
public class GridServiceProxyClientReconnectSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnect() throws Exception {
        startGrid("server");

        Ignite client = startClientGrid("client");

        client.services().deployClusterSingleton("my-service", new MyServiceImpl());

        MyService proxy = client.services().serviceProxy("my-service", MyService.class, false);

        assertEquals(42, proxy.hello());

        final CountDownLatch latch = new CountDownLatch(1);

        client.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event event) {
                latch.countDown();

                return true;
            }
        }, EventType.EVT_CLIENT_NODE_RECONNECTED);

        stopGrid("server");

        startGrid("server");

        assertTrue(latch.await(12, TimeUnit.SECONDS));

        client.services().deployClusterSingleton("my-service", new MyServiceImpl());

        assertEquals(42, proxy.hello());
    }


    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnectLongServiceInit() throws Exception {
        startGrid("server");

        Ignite client = startClientGrid("client");

        client.services().deployClusterSingleton("my-service", new MyLongInitServiceImpl());

        MyService proxy = client.services().serviceProxy("my-service", MyService.class, false);

        assertEquals(9001, proxy.hello());

        final CountDownLatch latch = new CountDownLatch(1);

        client.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event event) {
                latch.countDown();

                return true;
            }
        }, EventType.EVT_CLIENT_NODE_RECONNECTED);

        stopGrid("server");

        startGrid("server");

        assertTrue(latch.await(12, TimeUnit.SECONDS));

        client.services().deployClusterSingleton("my-service", new MyLongInitServiceImpl());

        assertEquals(9001, proxy.hello());
    }

    /**
     */
    private interface MyService extends Service {
        /**
         * @return Response.
         */
        public int hello();
    }

    /**
     */
    private static class MyServiceImpl implements MyService {
        /** {@inheritDoc} */
        @Override public int hello() {
            return 42;
        }

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            // No-op.
        }
    }

    /**
     */
    private static class MyLongInitServiceImpl implements MyService {
        /** {@inheritDoc} */
        @Override public int hello() {
            return 9001;
        }

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            Thread.sleep(5_000);
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            // No-op.
        }
    }
}
