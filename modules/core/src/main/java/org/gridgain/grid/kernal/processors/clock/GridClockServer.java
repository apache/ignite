/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.clock;

import org.apache.ignite.*;
import org.apache.ignite.thread.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.worker.*;

import java.io.*;
import java.net.*;

/**
 * Time server that enables time synchronization between nodes.
 */
public class GridClockServer {
    /** Kernal context. */
    private GridKernalContext ctx;

    /** Datagram socket for message exchange. */
    private DatagramSocket sock;

    /** Logger. */
    private IgniteLogger log;

    /** Read worker. */
    private GridWorker readWorker;

    /** Instance of time processor. */
    private GridClockSyncProcessor clockSync;

    /**
     * Starts server.
     *
     * @param ctx Kernal context.
     * @throws GridException If server could not be started.
     */
    public void start(GridKernalContext ctx) throws GridException {
        this.ctx = ctx;

        clockSync = ctx.clockSync();
        log = ctx.log(GridClockServer.class);

        try {
            int startPort = ctx.config().getTimeServerPortBase();
            int endPort = startPort + ctx.config().getTimeServerPortRange() - 1;

            InetAddress locHost = !F.isEmpty(ctx.config().getLocalHost()) ?
                InetAddress.getByName(ctx.config().getLocalHost()) :
                U.getLocalHost();

            for (int p = startPort; p <= endPort; p++) {
                try {
                    sock = new DatagramSocket(p, locHost);

                    if (log.isDebugEnabled())
                        log.debug("Successfully bound time server [host=" + locHost + ", port=" + p + ']');

                    break;
                }
                catch (SocketException e) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to bind time server socket [host=" + locHost + ", port=" + p +
                            ", err=" + e.getMessage() + ']');
                }
            }

            if (sock == null)
                throw new GridException("Failed to bind time server socket within specified port range [locHost=" +
                    locHost + ", startPort=" + startPort + ", endPort=" + endPort + ']');
        }
        catch (IOException e) {
            throw new GridException("Failed to start time server (failed to get local host address)", e);
        }
    }

    /**
     * After start callback.
     */
    public void afterStart() {
        readWorker = new ReadWorker();

        IgniteThread th = new IgniteThread(readWorker);

        th.setPriority(Thread.MAX_PRIORITY);

        th.start();
    }

    /**
     * Stops server.
     */
    public void stop() {
        // No-op.
    }

    /**
     * Before stop callback.
     */
    public void beforeStop() {
        if (readWorker != null)
            readWorker.cancel();

        U.closeQuiet(sock);

        if (readWorker != null)
            U.join(readWorker, log);
    }

    /**
     * Sends packet to remote node.
     *
     * @param msg Message to send.
     * @param addr Address.
     * @param port Port.
     * @throws GridException If send failed.
     */
    public void sendPacket(GridClockMessage msg, InetAddress addr, int port) throws GridException {
        try {
            DatagramPacket packet = new DatagramPacket(msg.toBytes(), GridClockMessage.PACKET_SIZE, addr, port);

            if (log.isDebugEnabled())
                log.debug("Sending time sync packet [msg=" + msg + ", addr=" + addr + ", port=" + port);

            sock.send(packet);
        }
        catch (IOException e) {
            if (!sock.isClosed())
                throw new GridException("Failed to send datagram message to remote node [addr=" + addr +
                    ", port=" + port + ", msg=" + msg + ']', e);
        }
    }

    /**
     * @return Address to which this server is bound.
     */
    public InetAddress host() {
        return sock.getLocalAddress();
    }

    /**
     * @return Port to which this server is bound.
     */
    public int port() {
        return sock.getLocalPort();
    }

    /**
     * Message read worker.
     */
    private class ReadWorker extends GridWorker {
        /**
         * Creates read worker.
         */
        protected ReadWorker() {
            super(ctx.gridName(), "grid-time-server-reader", log);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, GridInterruptedException {
            DatagramPacket packet = new DatagramPacket(new byte[GridClockMessage.PACKET_SIZE],
                GridClockMessage.PACKET_SIZE);

            while (!isCancelled()) {
                try {
                    // Read packet from buffer.
                    sock.receive(packet);

                    if (log.isDebugEnabled())
                        log.debug("Received clock sync message from remote node [host=" + packet.getAddress() +
                            ", port=" + packet.getPort() + ']');

                    GridClockMessage msg = GridClockMessage.fromBytes(packet.getData(), packet.getOffset(),
                        packet.getLength());

                    clockSync.onMessageReceived(msg, packet.getAddress(), packet.getPort());
                }
                catch (GridException e) {
                    U.warn(log, "Failed to assemble clock server message (will ignore the packet) [host=" +
                        packet.getAddress() + ", port=" + packet.getPort() + ", err=" + e.getMessage() + ']');
                }
                catch (IOException e) {
                    if (!isCancelled())
                        U.warn(log, "Failed to receive message on datagram socket: " + e);
                }
            }
        }
    }
}
