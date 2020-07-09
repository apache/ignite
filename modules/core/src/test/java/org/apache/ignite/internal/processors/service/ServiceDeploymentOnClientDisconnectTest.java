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
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cluster.ClusterGroupAdapter;
import org.apache.ignite.internal.processors.service.inner.LongInitializedTestService;
import org.apache.ignite.internal.processors.service.inner.MyService;
import org.apache.ignite.internal.processors.service.inner.MyServiceFactory;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;

/**
 * Test service deployment while client node disconnecting.
 */
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class ServiceDeploymentOnClientDisconnectTest extends GridCommonAbstractTest {
    /** */
    private static final long CLIENT_FAILURE_DETECTION_TIMEOUT = 5_000L;

    /** */
    private static final long CLIENT_RECONNECT_WAIT_TIMEOUT = 10_000L;

    /** */
    @Before
    public void check() {
        Assume.assumeTrue(isEventDrivenServiceProcessorEnabled());
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientFailureDetectionTimeout(CLIENT_FAILURE_DETECTION_TIMEOUT);

        if (cfg.getDiscoverySpi() instanceof TcpDiscoverySpi)
            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setClientReconnectDisabled(false);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrid(0);

        startClientGrid(getConfiguration("client"));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception In case of an error.
     */
    @Test
    public void testServiceDeploymentExchangeProcessingOnReconnect() throws Exception {
        IgniteEx client = client();

        IgniteFuture fut = client.services()
            .deployNodeSingletonAsync("testService1", new LongInitializedTestService(10_000L));
        client.services().deployNodeSingletonAsync("testService2", new LongInitializedTestService(10_000L));

        server().close();

        IgniteFuture reconnectFut = null;

        try {
            fut.get();

            fail("Client disconnected exception was expected.");
        }
        catch (IgniteClientDisconnectedException e) {
            reconnectFut = e.reconnectFuture();
        }

        assertNotNull(reconnectFut);

        startGrid(0);

        reconnectFut.get(CLIENT_RECONNECT_WAIT_TIMEOUT);

        assertEquals(2, client.cluster().topologyVersion());

        assertEquals(0, client.services().serviceDescriptors().size());

        client.services().deployNodeSingleton("testService3", MyServiceFactory.create());

        final MyService proxy = client.services().serviceProxy("testService3", MyService.class, false, 2_000);

        assertNotNull(proxy);

        assertEquals(42, proxy.hello());
    }

    /** */
    @Test
    public void testInitiatorDeploymentFutureCompletionOnClientDisconnect() {
        IgniteFuture fut = client().services().deployNodeSingletonAsync("testService",
            new LongInitializedTestService(10_000L));

        server().close();

        GridTestUtils.assertThrowsWithCause((Runnable)fut::get, IgniteClientDisconnectedException.class);
    }

    /**
     * @throws Exception In case of an error.
     */
    @Test
    public void testThrowingExceptionOnDeployUsingPuplicApiWhileClientDisconnected() throws Exception {
        runTaskWhenDisconnected(() -> {
            GridTestUtils.assertThrowsWithCause(
                () -> client().services().deployNodeSingletonAsync("testService", MyServiceFactory.create()),
                IgniteClientDisconnectedException.class);
        });
    }

    /**
     * @throws Exception In case of an error.
     */
    @Test
    public void testThrowingExceptionOnUndeployUsingPuplicApiWhileClientDisconnected() throws Exception {
        runTaskWhenDisconnected(() -> {
            GridTestUtils.assertThrowsWithCause(() -> client().services().cancelAll(),
                IgniteClientDisconnectedException.class);
        });
    }

    /**
     * @throws Exception In case of an error.
     */
    @Test
    public void testThrowingExceptionOnDeployUsingInternalApiWhileClientDisconnected() throws Exception {
        runTaskWhenDisconnected(() -> {
            GridTestUtils.assertThrowsWithCause(() -> client().context().service().deployNodeSingleton(
                new ClusterGroupAdapter(), "testService", MyServiceFactory.create()).get(),
                IgniteClientDisconnectedCheckedException.class);
        });
    }

    /**
     * @throws Exception In case of an error.
     */
    @Test
    public void testThrowingExceptionOnUndeployUsingInternalApiWhileClientDisconnectedTest() throws Exception {
        runTaskWhenDisconnected(() -> {
            GridTestUtils.assertThrowsWithCause(() -> client().context().service().cancelAll().get(),
                IgniteClientDisconnectedCheckedException.class);
        });
    }

    /**
     * Apply given task on disconnected client node.
     *
     * @param task Task.
     * @throws InterruptedException If interrupted.
     */
    private void runTaskWhenDisconnected(final Runnable task) throws InterruptedException {
        Ignite client = client();

        CountDownLatch latch = new CountDownLatch(1);

        client.events().localListen((IgnitePredicate<Event>)evt -> {
            latch.countDown();

            return false;
        }, EVT_CLIENT_NODE_DISCONNECTED);

        server().close();

        assertTrue(latch.await(
            CLIENT_FAILURE_DETECTION_TIMEOUT * 2 + CLIENT_FAILURE_DETECTION_TIMEOUT / 10,
                TimeUnit.MILLISECONDS
        ));

        task.run();
    }

    /**
     * @return Clients node.
     */
    private IgniteEx client() {
        return grid("client");
    }

    /**
     * @return Server node.
     */
    private IgniteEx server() {
        return grid(0);
    }
}
