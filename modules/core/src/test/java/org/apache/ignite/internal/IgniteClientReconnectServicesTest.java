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
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.processors.service.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.services.*;
import org.apache.ignite.testframework.*;

import java.util.concurrent.*;

/**
 *
 */
public class IgniteClientReconnectServicesTest extends IgniteClientReconnectAbstractTest {
    /** {@inheritDoc} */
    @Override protected int serverCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected int clientCount() {
        return 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnect() throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        IgniteServices services = client.services();

        services.deployClusterSingleton("testReconnect", new TestServiceImpl());

        TestService srvc = services.serviceProxy("testReconnect", TestService.class, false);

        assertNotNull(srvc);

        long topVer = grid(0).cluster().topologyVersion();

        assertEquals((Object)topVer, srvc.test());

        Ignite srv = clientRouter(client);

        reconnectClientNode(client, srv, null);

        CountDownLatch latch = new CountDownLatch(1);

        DummyService.exeLatch("testReconnect2", latch);

        services.deployClusterSingleton("testReconnect2", new DummyService());

        assertTrue(latch.await(5000, TimeUnit.MILLISECONDS));

        assertEquals((Object)(topVer + 2), srvc.test());
    }

    /**
     * @throws Exception If failed.
     */
    public void testServiceRemove() throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        Ignite srv = clientRouter(client);

        IgniteServices clnServices = client.services();

        final IgniteServices srvServices = srv.services();

        srvServices.deployClusterSingleton("testServiceRemove", new TestServiceImpl());

        final TestService srvc = clnServices.serviceProxy("testServiceRemove", TestService.class, false);

        assertNotNull(srvc);

        assertNotNull(srvc.test());

        reconnectClientNode(client, srv, new Runnable() {
            @Override public void run() {
                srvServices.cancel("testServiceRemove");
            }
        });

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return srvc.test();
            }
        }, IgniteException.class, null);

        clnServices.deployClusterSingleton("testServiceRemove", new TestServiceImpl());

        TestService newSrvc = clnServices.serviceProxy("testServiceRemove", TestService.class, false);

        assertNotNull(newSrvc);
        assertNotNull(newSrvc.test());
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnectInDeploying() throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        final IgniteServices services = client.services();

        Ignite srv = clientRouter(client);

        BlockTpcCommunicationSpi commSpi = commSpi(srv);

        commSpi.blockMessage(GridNearTxPrepareResponse.class);

        final IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                try {
                    services.deployClusterSingleton("testReconnectInDeploying", new TestServiceImpl());
                }
                catch (IgniteClientDisconnectedException e) {
                    checkAndWait(e);

                    return true;
                }

                return false;
            }
        });

        // Check that client waiting operation.
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fut.get(200);
            }
        }, IgniteFutureTimeoutCheckedException.class, null);

        assertNotDone(fut);

        commSpi.unblockMessage();

        reconnectClientNode(client, srv, null);

        assertTrue((Boolean)fut.get(2, TimeUnit.SECONDS));
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnectInProgress() throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        final IgniteServices services = client.services();

        final Ignite srv = clientRouter(client);

        services.deployClusterSingleton("testReconnectInProgress", new TestServiceImpl());

        final TestService srvc = services.serviceProxy("testReconnectInProgress", TestService.class, false);

        assertNotNull(srvc);

        BlockTpcCommunicationSpi commSpi = commSpi(srv);

        commSpi.blockMessage(GridJobExecuteResponse.class);

        final IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                try {
                    srvc.test();
                }
                catch (IgniteClientDisconnectedException e) {
                    checkAndWait(e);

                    return true;
                }

                return false;
            }
        });

        // Check that client waiting operation.
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fut.get(200);
            }
        }, IgniteFutureTimeoutCheckedException.class, null);

        assertNotDone(fut);

        commSpi.unblockMessage();

        reconnectClientNode(client, srv, null);

        assertTrue((Boolean)fut.get(2, TimeUnit.SECONDS));
    }

    /**
     *
     */
    public static interface TestService {
        /**
         * @return Topology version.
         */
        public Long test();
    }

    /**
     *
     */
    public static class TestServiceImpl implements Service, TestService {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

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

        /** {@inheritDoc} */
        @Override public Long test() {
            assertFalse(ignite.cluster().localNode().isClient());

            return ignite.cluster().topologyVersion();
        }
    }
}
