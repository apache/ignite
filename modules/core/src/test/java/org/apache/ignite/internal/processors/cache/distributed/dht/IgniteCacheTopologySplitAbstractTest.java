package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
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
     * Trigger segmentation and wait for results. Should be called on stable topology.
     *
     * @throws InterruptedException If interrupted while waiting.
     * @throws IgniteCheckedException On error.
     */
    protected void splitAndWait() throws InterruptedException, IgniteCheckedException {
        if (log.isInfoEnabled())
            log.info(">>> Simulating split");

        long topVer = grid(0).cluster().topologyVersion();

        // Trigger segmentation.
        segmented = true;

        for (Ignite ignite : G.allGrids()) {
            TestRecordingCommunicationSpi comm = (TestRecordingCommunicationSpi)
                ignite.configuration().getCommunicationSpi();

            comm.blockMessages(new SegmentBlocker(ignite.cluster().localNode()));
        }

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

        for (Ignite grid : seg0)
            ((IgniteKernal)grid).context().discovery().topologyFuture(topVer + seg1.size()).get();

        for (Ignite grid : seg1)
            ((IgniteKernal)grid).context().discovery().topologyFuture(topVer + seg0.size()).get();

        // awaitPartitionMapExchange won't work because coordinator is wrong for second segment.
        for (Ignite grid : G.allGrids())
            ((IgniteKernal)grid).context().cache().context().exchange().lastTopologyFuture().get();

        if (log.isInfoEnabled())
            log.info(">>> Finished waiting for split");
    }

    /**
     * Restore initial state
     */
    protected void unsplit() {
        if (log.isInfoEnabled())
            log.info(">>> Restoring from split");

        segmented = false;

        for (Ignite ignite : G.allGrids()) {
            TestRecordingCommunicationSpi comm = (TestRecordingCommunicationSpi)
                ignite.configuration().getCommunicationSpi();

            comm.stopBlock();
        }
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
         */
        protected boolean segmented(InetSocketAddress sockAddr) {
            if (!segmented)
                return false;

            int rmtPort = sockAddr.getPort();

            boolean b = isBlocked(getLocalPort(), rmtPort);

            if (b && log.isDebugEnabled())
                log.debug("Blocking cross-segment communication [locPort=" + getLocalPort() + ", rmtPort=" + rmtPort + ']');

            return b;
        }

        /**
         * @param sockAddr Socket address.
         * @param timeout Socket timeout.
         */
        protected void checkSegmented(InetSocketAddress sockAddr, long timeout) throws SocketTimeoutException {
            if (segmented(sockAddr)) {
                if (timeout > 0)
                    try {
                        Thread.sleep(timeout);
                    }
                    catch (InterruptedException e) {
                        // No-op.
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

        /**  */
        @Override protected void writeToSocket(
            Socket sock,
            TcpDiscoveryAbstractMessage msg,
            byte[] data,
            long timeout
        ) throws IOException {
            checkSegmented((InetSocketAddress)sock.getRemoteSocketAddress(), timeout);

            super.writeToSocket(sock, msg, data, timeout);
        }

        /**  */
        @Override protected void writeToSocket(Socket sock,
            OutputStream out,
            TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            checkSegmented((InetSocketAddress)sock.getRemoteSocketAddress(), timeout);

            super.writeToSocket(sock, out, msg, timeout);
        }

        /**  */
        @Override protected void writeToSocket(
            Socket sock,
            TcpDiscoveryAbstractMessage msg,
            long timeout
        ) throws IOException, IgniteCheckedException {
            checkSegmented((InetSocketAddress)sock.getRemoteSocketAddress(), timeout);

            super.writeToSocket(sock, msg, timeout);
        }
    }

    /**  */
    protected class SegmentBlocker implements IgniteBiPredicate<ClusterNode, Message> {

        /**  */
        private final ClusterNode locNode;

        /**  */
        public SegmentBlocker(ClusterNode locNode) {
            assert locNode != null;

            this.locNode = locNode;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node, Message message) {
            return segment(locNode) != segment(node);
        }
    }
}