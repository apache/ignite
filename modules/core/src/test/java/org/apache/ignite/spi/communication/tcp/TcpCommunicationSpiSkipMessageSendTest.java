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

package org.apache.ignite.spi.communication.tcp;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutHelper;
import org.apache.ignite.spi.collision.fifoqueue.FifoQueueCollisionSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 * Tests that the client will be segmented in time and won't hang due to canceling compute jobs.
 */
public class TcpCommunicationSpiSkipMessageSendTest extends GridCommonAbstractTest {
    /** */
    private static final CountDownLatch COMPUTE_JOB_STARTED = new CountDownLatch(1);

    /** */
    private static final long FAILURE_DETECTION_TIMEOUT = 10000;

    /** */
    private static final long JOIN_TIMEOUT = 10000;

    /** */
    private static final long START_JOB_TIMEOUT = 10000;

    /** */
    private static final long DISABLE_NETWORK_DELAY = 2000;

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 2 * 60 * 1000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientSegmented() throws Exception {
        Ignite server = null;
        Ignite client = null;

        try {
            server = Ignition.start(getConfig(false));

            final CountDownLatch clientDisconnected = new CountDownLatch(1);
            final CountDownLatch clientSegmented = new CountDownLatch(1);

            client = startClient(clientDisconnected, clientSegmented);

            final IgniteCompute compute = client.compute();

            runJobAsync(compute);

            if (!COMPUTE_JOB_STARTED.await(START_JOB_TIMEOUT, TimeUnit.MILLISECONDS))
                fail("Compute job wasn't started.");

            disableNetwork(client);

            if (!clientDisconnected.await(FAILURE_DETECTION_TIMEOUT * 3, TimeUnit.MILLISECONDS))
                fail("Client wasn't disconnected.");

            if (!clientSegmented.await(JOIN_TIMEOUT * 2, TimeUnit.MILLISECONDS))
                fail("Client wasn't segmented.");
        }
        finally {
            if (client != null)
                client.close();

            if (server != null)
                server.close();
        }
    }

    /**
     * Simulate network disabling.
     *
     * @param ignite Ignite instance.
     * @throws IgniteInterruptedCheckedException If thread sleep interrupted.
     * @throws InterruptedException If waiting for network disabled failed (interrupted).
     */
    private void disableNetwork(Ignite ignite) throws IgniteInterruptedCheckedException, InterruptedException {
        U.sleep(DISABLE_NETWORK_DELAY);

        CustomCommunicationSpi communicationSpi = (CustomCommunicationSpi)ignite.configuration().getCommunicationSpi();

        CustomDiscoverySpi discoverySpi = (CustomDiscoverySpi)ignite.configuration().getDiscoverySpi();

        discoverySpi.disableNetwork();

        communicationSpi.disableNetwork();

        if (!discoverySpi.awaitNetworkDisabled(FAILURE_DETECTION_TIMEOUT * 2))
            fail("Network wasn't disabled.");
    }

    /**
     * Start compute jobs in the separate thread.
     *
     * @param compute Ignite compute instance.
     */
    private void runJobAsync(final IgniteCompute compute) {
        new Thread(new Runnable() {
            @Override public void run() {
                try {
                    compute.call(new IgniteCallable<Integer>() {
                        @Override public Integer call() throws Exception {
                            COMPUTE_JOB_STARTED.countDown();

                            // Simulate long-running job.
                            new CountDownLatch(1).await();

                            return null;
                        }
                    });
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    /**
     * Create Communication Spi instance.
     *
     * @param client Is a client node.
     * @return Communication Spi.
     */
    private TcpCommunicationSpi getCommunicationSpi(boolean client) {
        TcpCommunicationSpi spi = new CustomCommunicationSpi(client);

        spi.setName("CustomCommunicationSpi");

        return spi;
    }

    /**
     * Create Discovery Spi instance.
     *
     * @return Discovery Spi.
     */
    private TcpDiscoverySpi getDiscoverySpi() {
        TcpDiscoverySpi spi = new CustomDiscoverySpi();

        spi.setName("CustomDiscoverySpi");

        spi.setIpFinder(LOCAL_IP_FINDER);

        return spi;
    }

    /**
     * Create Ignite configuration.
     *
     * @param clientMode Client mode.
     * @return Ignite configuration.
     */
    private IgniteConfiguration getConfig(boolean clientMode) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName(clientMode ? "client-node" : "server-node");

        cfg.setClientMode(clientMode);

        cfg.setCommunicationSpi(getCommunicationSpi(clientMode));

        if (!clientMode) {
            cfg.setDiscoverySpi(getDiscoverySpi());

            FifoQueueCollisionSpi collisionSpi = new FifoQueueCollisionSpi();

            collisionSpi.setParallelJobsNumber(1);

            cfg.setCollisionSpi(collisionSpi);
        }
        else {
            cfg.setFailureDetectionTimeout(FAILURE_DETECTION_TIMEOUT);

            cfg.setDiscoverySpi(getDiscoverySpi().setJoinTimeout(JOIN_TIMEOUT));
        }

        return cfg;
    }

    /**
     * Start client node.
     *
     * @param clientDisconnected Client is disconnected.
     * @param clientSegmented Client is segmented.
     * @return Ignite instance.
     */
    private Ignite startClient(final CountDownLatch clientDisconnected, final CountDownLatch clientSegmented) {
        Ignite ignite = Ignition.start(getConfig(true));

        IgnitePredicate<Event> locLsnr = new IgnitePredicate<Event>() {
            @Override public boolean apply(Event event) {
                log.info("Client node received event: " + event.name());

                if (event.type() == EventType.EVT_CLIENT_NODE_DISCONNECTED)
                    clientDisconnected.countDown();

                if (event.type() == EventType.EVT_NODE_SEGMENTED)
                    clientSegmented.countDown();

                return true;
            }
        };

        ignite.events().localListen(locLsnr,
            EventType.EVT_NODE_SEGMENTED,
            EventType.EVT_CLIENT_NODE_DISCONNECTED);

        return ignite;
    }

    /**
     * Communication Spi that emulates connection troubles.
     */
    class CustomCommunicationSpi extends TcpCommunicationSpi {
        /** Network is disabled. */
        private volatile boolean networkDisabled = false;

        /** Additional logging is enabled. */
        private final boolean logEnabled;

        /**
         * @param enableLogs Enable additional logging.
         */
        CustomCommunicationSpi(boolean enableLogs) {
            super();
            this.logEnabled = enableLogs;
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg,
            IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            String message = msg.toString();

            if (logEnabled)
                log.info("CustomCommunicationSpi.sendMessage: " + message);

            if (message.contains("TOPIC_JOB_CANCEL"))
                closeTcpConnections();

            super.sendMessage(node, msg, ackC);
        }

        /** {@inheritDoc} */
        @Override protected GridCommunicationClient createTcpClient(ClusterNode node, int connIdx) throws IgniteCheckedException {
            if (logEnabled)
                log.info(String.format("CustomCommunicationSpi.createTcpClient [networkDisabled=%s, node=%s]", networkDisabled, node));

            if (networkDisabled) {
                IgniteSpiOperationTimeoutHelper timeoutHelper = new IgniteSpiOperationTimeoutHelper(this, !node.isClient());

                long timeout = timeoutHelper.nextTimeoutChunk(getConnectTimeout());

                if (logEnabled)
                    log.info("CustomCommunicationSpi.createTcpClient [timeoutHelper.nextTimeoutChunk=" + timeout + "]");

                sleep(timeout);

                return null;
            }
            else
                return super.createTcpClient(node, connIdx);
        }

        /**
         * Simulate network disabling.
         */
        void disableNetwork() {
            networkDisabled = true;
        }

        /**
         * Close communication clients. It will lead that sendMessage method will be trying to create new ones.
         */
        private void closeTcpConnections() {
            final ConcurrentMap<UUID, GridCommunicationClient[]> clients = U.field(this, "clients");

            Set<UUID> ids = clients.keySet();

            if (ids.size() > 0) {
                log.info("Close TCP clients: " + ids);

                for (UUID nodeId : ids) {
                    GridCommunicationClient[] clients0 = clients.remove(nodeId);

                    if (clients0 != null) {
                        for (GridCommunicationClient client : clients0) {
                            if (client != null)
                                client.forceClose();
                        }
                    }
                }

                log.info("TCP clients are closed.");
            }
        }
    }

    /**
     * Discovery Spi that emulates connection troubles.
     */
    class CustomDiscoverySpi extends TcpDiscoverySpi {
        /** Network is disabled. */
        private volatile boolean networkDisabled = false;

        /** */
        private final CountDownLatch networkDisabledLatch = new CountDownLatch(1);

        /** */
        CustomDiscoverySpi() {
            super();

            setName("CustomDiscoverySpi");
        }

        /** {@inheritDoc} */
        @Override protected <T> T readMessage(Socket sock, @Nullable InputStream in,
            long timeout) throws IOException, IgniteCheckedException {
            if (networkDisabled) {
                sleep(timeout);

                return null;
            }
            else
                return super.readMessage(sock, in, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (networkDisabled) {
                sleep(timeout);

                networkDisabledLatch.countDown();

                throw new IgniteCheckedException("CustomDiscoverySpi: network is disabled.");
            }
            else
                super.writeToSocket(sock, msg, timeout);
        }

        /**
         * Simulate network disabling.
         */
        void disableNetwork() {
            networkDisabled = true;
        }

        /**
         * Wait until the network is disabled.
         */
        boolean awaitNetworkDisabled(long timeout) throws InterruptedException {
            return networkDisabledLatch.await(timeout, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Sleeps for given number of milliseconds.
     *
     * @param timeout Time to sleep (2000 ms by default).
     * @throws IgniteInterruptedCheckedException If current thread interrupted.
     */
    static void sleep(long timeout) throws IgniteInterruptedCheckedException {
        if (timeout > 0)
            U.sleep(timeout);
        else
            U.sleep(2000);
    }
}
