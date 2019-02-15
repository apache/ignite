/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.service;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Service proxy test with client reconnect.
 */
@RunWith(JUnit4.class)
public class GridServiceProxyClientReconnectSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode(igniteInstanceName.contains("client"));

        return cfg;
    }

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

        Ignite client = startGrid("client");

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

        Ignite client = startGrid("client");

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
