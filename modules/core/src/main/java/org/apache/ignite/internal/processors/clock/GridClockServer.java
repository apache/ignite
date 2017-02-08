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

package org.apache.ignite.internal.processors.clock;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;

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
     * @throws IgniteCheckedException If server could not be started.
     */
    public void start(GridKernalContext ctx) throws IgniteCheckedException {
        this.ctx = ctx;

        clockSync = ctx.clockSync();
        log = ctx.log(GridClockServer.class);

        try {
            int startPort = ctx.config().getTimeServerPortBase();
            int portRange = ctx.config().getTimeServerPortRange();
            int endPort = portRange == 0 ? startPort : startPort + portRange - 1;

            InetAddress locHost;

            if (F.isEmpty(ctx.config().getLocalHost())) {
                try {
                    locHost = U.getLocalHost();
                }
                catch (IOException ignored) {
                    locHost = InetAddress.getLoopbackAddress();

                    U.warn(log, "Failed to get local host address, will use loopback address: " + locHost);
                }
            }
            else
                locHost = InetAddress.getByName(ctx.config().getLocalHost());

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
                throw new IgniteCheckedException("Failed to bind time server socket within specified port range " +
                    "[locHost=" + locHost + ", startPort=" + startPort + ", endPort=" + endPort + ']');
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to start time server (failed to get local host address)", e);
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
     * @throws IgniteCheckedException If send failed.
     */
    public void sendPacket(GridClockMessage msg, InetAddress addr, int port) throws IgniteCheckedException {
        try {
            DatagramPacket packet = new DatagramPacket(msg.toBytes(), GridClockMessage.PACKET_SIZE, addr, port);

            if (log.isDebugEnabled())
                log.debug("Sending time sync packet [msg=" + msg + ", addr=" + addr + ", port=" + port);

            sock.send(packet);
        }
        catch (IOException e) {
            if (!sock.isClosed())
                throw new IgniteCheckedException("Failed to send datagram message to remote node [addr=" + addr +
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
            super(ctx.gridName(), "grid-time-server-reader", GridClockServer.this.log);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
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
                catch (IgniteCheckedException e) {
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
