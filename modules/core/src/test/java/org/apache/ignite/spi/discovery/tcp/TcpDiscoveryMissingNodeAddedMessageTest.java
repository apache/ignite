package org.apache.ignite.spi.discovery.tcp;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test checks a node is able to join to the cluster after missing single {@link TcpDiscoveryNodeAddedMessage}.
 */
public class TcpDiscoveryMissingNodeAddedMessageTest extends GridCommonAbstractTest {
    /** */
    private static final int NODE_0_PORT = 47500;

    /** */
    private static final int NODE_1_PORT = 47501;

    /** */
    private static final int NODE_2_PORT = 47502;

    /** */
    private static final String NODE_0_NAME = "node00-" + NODE_0_PORT;

    /** */
    private static final String NODE_1_NAME = "node01-" + NODE_1_PORT;

    /** */
    private static final String NODE_2_NAME = "node02-" + NODE_2_PORT;

    /** */
    private TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private TcpDiscoverySpi specialSpi;

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi spi = (specialSpi != null) ? specialSpi : new TcpDiscoverySpi();

        spi.setLocalPort(Integer.parseInt(igniteInstanceName.split("-")[1]));

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        return cfg;
    }

    /** */
    @Test
    public void discoverTest() throws Exception {
        AtomicInteger dropped = new AtomicInteger();

        specialSpi = new DropTcpDiscoverySpi(dropped);

        IgniteEx ig0 = startGrid(NODE_0_NAME);

        specialSpi = null;

        dropped.set(1);

        startGrid(NODE_2_NAME);

        waitForRemoteNodes(ig0, 1);
    }

    /**
     * Emulates situation when network drops {@code dropCnt} of {@link TcpDiscoveryNodeAddedMessage} messages.
     */
    private static class DropTcpDiscoverySpi extends TcpDiscoverySpi {

        /** Number of messages to be dropped */
        private AtomicInteger dropCnt;

        /** */
        public DropTcpDiscoverySpi(AtomicInteger dropCnt) {
            this.dropCnt = dropCnt;
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg, byte[] data,
            long timeout) throws IOException {
            if (isDrop(msg, sock)) {
                log.error("TEST | dropping TcpDiscoveryNodeAddedMessage on " + spiCtx.localNode().order());

                return;
            }

            super.writeToSocket(sock, msg, data, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (isDrop(msg, sock)) {
                log.error("TEST | dropping TcpDiscoveryNodeAddedMessage on " + spiCtx.localNode().order());

                return;
            }

            super.writeToSocket(sock, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(ClusterNode node, Socket sock, OutputStream out,
            TcpDiscoveryAbstractMessage msg, long timeout) throws IOException, IgniteCheckedException {

            if (isDrop(msg, sock)) {
                log.error("TEST | dropping TcpDiscoveryNodeAddedMessage on " + spiCtx.localNode().order());

                return;
            }

            super.writeToSocket(node, sock, out, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (isDrop(msg, sock)) {
                log.error("TEST | dropping TcpDiscoveryNodeAddedMessage on " + spiCtx.localNode().order());

                return;
            }

            super.writeToSocket(sock, out, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(TcpDiscoveryAbstractMessage msg, Socket sock, int res,
            long timeout) throws IOException {
            if (isDrop(msg, sock)) {
                log.error("TEST | dropping TcpDiscoveryNodeAddedMessage on " + spiCtx.localNode().order());

                return;
            }

            super.writeToSocket(msg, sock, res, timeout);
        }

        /** */
        private boolean isDrop(TcpDiscoveryAbstractMessage msg, Socket sock) {
            if (msg instanceof TcpDiscoveryNodeAddedMessage && sock.getPort() == NODE_2_PORT)
                return dropCnt.getAndUpdate(v -> Math.max(0, v - 1)) > 0;

            return false;
        }
    }

}