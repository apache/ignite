package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgnitePredicate;
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
     * Trigger segmentation and wait for results.
     * Should be called on stable topology.
     *
     * @throws InterruptedException If interrupted while waiting.
     * @throws IgniteCheckedException On error.
     */
    protected void splitAndWait() throws InterruptedException, IgniteCheckedException {
        long topVer = grid(0).cluster().topologyVersion();

        // Trigger segmentation.
        segmented = true;

        Collection<Ignite> seg0 = F.view(G.allGrids(), new IgnitePredicate<Ignite>() {
            @Override public boolean apply(Ignite ignite) {
                return segment(ignite) == 0;
            }
        });

        Collection<Ignite> seg1 = F.view(G.allGrids(), new IgnitePredicate<Ignite>() {
            @Override public boolean apply(Ignite ignite) {
                return segment(ignite) == 1;
            }
        });

        for (Ignite grid : seg0)
            ((IgniteKernal)grid).context().discovery().topologyFuture(topVer + seg1.size()).get();

        for (Ignite grid : seg1)
            ((IgniteKernal)grid).context().discovery().topologyFuture(topVer + seg0.size()).get();

        // awaitPartitionMapExchange won't work because coordinator is wrong for second segment.
        for (Ignite grid : G.allGrids())
            ((IgniteKernal)grid).context().cache().context().exchange().lastTopologyFuture().get();
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
     * @param ignite Ignite.
     * @return Index of instance segment.
     */
    protected abstract int segment(Ignite ignite);

    /**
     * Discovery SPI which can simulate network split.
     */
    protected class SplitTcpDiscoverySpi extends TcpDiscoverySpi {
        /**
         * @param socket Socket.
         */
        protected boolean segmented(Socket socket) {
            if (!segmented)
                return false;

            int rmtPort = socket.getPort();

            return isBlocked(getLocalPort(), rmtPort);
        }

        /**  */
        @Override protected void writeToSocket(
            Socket sock,
            TcpDiscoveryAbstractMessage msg,
            byte[] data,
            long timeout
        ) throws IOException {
            if (segmented(sock)) {
                try {
                    Thread.sleep(timeout);
                } catch (InterruptedException e) {
                    // No-op.
                }

                throw new SocketTimeoutException("Fake socket timeout.");
            }
            else
                super.writeToSocket(sock, msg, data, timeout);
        }

        /**  */
        @Override protected void writeToSocket(Socket sock,
            OutputStream out,
            TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (segmented(sock)) {
                try {
                    Thread.sleep(timeout);
                } catch (InterruptedException e) {
                    // No-op.
                }

                throw new SocketTimeoutException("Fake socket timeout.");
            }
            else
                super.writeToSocket(sock, out, msg, timeout);
        }

        /**  */
        @Override protected void writeToSocket(
            Socket sock,
            TcpDiscoveryAbstractMessage msg,
            long timeout
        ) throws IOException, IgniteCheckedException {
            if (segmented(sock)) {
                try {
                    Thread.sleep(timeout);
                } catch (InterruptedException e) {
                    // No-op.
                }

                throw new SocketTimeoutException("Fake socket timeout.");
            }
            else
                super.writeToSocket(sock, msg, timeout);
        }

        /**  */
        @Override protected void writeToSocket(
            TcpDiscoveryAbstractMessage msg,
            Socket sock,
            int res,
            long timeout
        ) throws IOException {
            if (segmented(sock)) {
                try {
                    Thread.sleep(timeout);
                } catch (InterruptedException e) {
                    // No-op.
                }

                throw new SocketTimeoutException("Fake socket timeout.");
            }
            else
                super.writeToSocket(msg, sock, res, timeout);
        }
    }
}