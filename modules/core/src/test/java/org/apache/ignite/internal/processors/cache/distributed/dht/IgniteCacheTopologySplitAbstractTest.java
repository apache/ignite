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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutException;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutHelper;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Abstract class for tests over split in two half topology.
 */
public abstract class IgniteCacheTopologySplitAbstractTest extends GridCommonAbstractTest {

    /** Topology version before segmentation */
    protected long splitTopVer;

    /** Segmentation state. */
    private volatile boolean segmented;

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setFailureDetectionTimeout(3_000L);

        cfg.setDiscoverySpi(new SplitTcpDiscoverySpi());

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /**
     * Trigger segmentation. Should be called on stable topology.
     *
     * @throws IgniteCheckedException On error.
     */
    protected void split() throws IgniteCheckedException {
        if (log.isInfoEnabled())
            log.info("Simulating split");

        splitTopVer = grid(0).cluster().topologyVersion();

        // Trigger segmentation.
        segmented = true;

        for (Ignite ignite : G.allGrids()) {
            TestRecordingCommunicationSpi comm = (TestRecordingCommunicationSpi)
                ignite.configuration().getCommunicationSpi();

            comm.blockMessages(new SegmentBlocker(ignite.cluster().localNode()));
        }
    }

    /**
     * Wait for segmentation results. Should be called after {@link #split()}
     *
     * @throws InterruptedException If interrupted while waiting.
     */
    protected void awaitSegmentation() throws InterruptedException {
        Collection<Ignite> seg0 = F.view(G.allGrids(), new IgnitePredicate<Ignite>() {
            @Override public boolean apply(Ignite ignite) {
                return segment(ignite.cluster().localNode()) == 0;
            }
        });

        Collection<Ignite> seg1 = F.view(G.allGrids(), new IgnitePredicate<Ignite>() {
            @Override public boolean apply(Ignite ignite) {
                return segment(ignite.cluster().localNode()) == 1;
            }
        });

        try {
            for (Ignite grid : seg0)
                ((IgniteKernal)grid).context().discovery().topologyFuture(splitTopVer + seg1.size()).get();

            for (Ignite grid : seg1)
                ((IgniteKernal)grid).context().discovery().topologyFuture(splitTopVer + seg0.size()).get();

            // awaitPartitionMapExchange won't work because coordinator is wrong for second segment.
            for (Ignite grid : G.allGrids())
                ((IgniteKernal)grid).context().cache().context().exchange().lastTopologyFuture().get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to wait for exchange", e);
        }

        if (log.isInfoEnabled())
            log.info("Finished waiting for split");
    }

    /**
     * Trigger segmentation and wait for results. Should be called on stable topology.
     *
     * @throws InterruptedException If interrupted while waiting.
     * @throws IgniteCheckedException On error.
     */
    protected void splitAndWait() throws InterruptedException, IgniteCheckedException {
        split();
        awaitSegmentation();
    }

    /**
     * Restore initial state
     */
    protected void unsplit() {
        unsplit(true);
    }

    /**
     * Restore initial state
     *
     * @param sendMessages Send blocked messages after restoring from a split
     */
    protected void unsplit(boolean sendMessages) {
        if (log.isInfoEnabled())
            log.info("Restoring from split [sendMessages=" + sendMessages + ']');

        segmented = false;

        for (Ignite ignite : G.allGrids()) {
            TestRecordingCommunicationSpi comm = (TestRecordingCommunicationSpi)
                ignite.configuration().getCommunicationSpi();

            comm.stopBlock(sendMessages);
        }
    }

    /**
     * @return Segmented status.
     */
    protected boolean segmented() {
        return segmented;
    }

    /**
     * Defines split matrix.
     *
     * @param locPort Local port.
     * @param rmtPort Rmt port.
     * @return {@code true} is link is blocked.
     */
    protected abstract boolean isBlocked(int locPort, int rmtPort);

    /**
     * Defines instance segment: 0 or 1.
     *
     * @param node Node.
     * @return Index of instance segment.
     */
    protected abstract int segment(ClusterNode node);

    /**
     * Discovery SPI which can simulate network split.
     */
    protected class SplitTcpDiscoverySpi extends TcpDiscoverySpi {

        /**
         * @param sockAddr Remote socket address.
         * @return Segmented status.
         */
        protected boolean segmented(InetSocketAddress sockAddr) {
            if (!segmented)
                return false;

            int rmtPort = sockAddr.getPort();

            boolean b = isBlocked(getLocalPort(), rmtPort);

            if (b && log.isDebugEnabled())
                log.debug("Block cross-segment communication [locPort=" + getLocalPort() + ", rmtPort=" + rmtPort + ']');

            return b;
        }

        /**
         * @param sockAddr Socket address.
         * @param timeout Socket timeout.
         * @throws SocketTimeoutException If segmented.
         */
        protected void checkSegmented(InetSocketAddress sockAddr, long timeout) throws SocketTimeoutException {
            if (segmented(sockAddr)) {
                if (timeout > 0) {
                    try {
                        Thread.sleep(timeout);
                    }
                    catch (InterruptedException e) {
                        // No-op.
                    }
                }

                throw new SocketTimeoutException("Fake socket timeout.");
            }
        }

        /** {@inheritDoc} */
        @Override protected Socket openSocket(Socket sock, InetSocketAddress remAddr,
            IgniteSpiOperationTimeoutHelper timeoutHelper) throws IOException, IgniteSpiOperationTimeoutException {
            checkSegmented(remAddr, timeoutHelper.nextTimeoutChunk(getSocketTimeout()));

            return super.openSocket(sock, remAddr, timeoutHelper);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(
            Socket sock,
            TcpDiscoveryAbstractMessage msg,
            byte[] data,
            long timeout
        ) throws IOException {
            checkSegmented((InetSocketAddress)sock.getRemoteSocketAddress(), timeout);

            super.writeToSocket(sock, msg, data, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock,
            OutputStream out,
            TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            checkSegmented((InetSocketAddress)sock.getRemoteSocketAddress(), timeout);

            super.writeToSocket(sock, out, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(
            Socket sock,
            TcpDiscoveryAbstractMessage msg,
            long timeout
        ) throws IOException, IgniteCheckedException {
            checkSegmented((InetSocketAddress)sock.getRemoteSocketAddress(), timeout);

            super.writeToSocket(sock, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(TcpDiscoveryAbstractMessage msg, Socket sock, int res,
            long timeout) throws IOException {
            checkSegmented((InetSocketAddress)sock.getRemoteSocketAddress(), timeout);

            super.writeToSocket(msg, sock, res, timeout);
        }
    }

    /**  */
    protected class SegmentBlocker implements IgniteBiPredicate<ClusterNode, Message> {

        /**  */
        private final ClusterNode locNode;

        /**
         * @param locNode Local node.
         */
        SegmentBlocker(ClusterNode locNode) {
            assert locNode != null;

            this.locNode = locNode;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node, Message message) {
            return segment(locNode) != segment(node);
        }
    }
}