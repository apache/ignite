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

package org.apache.ignite.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.jetbrains.annotations.NotNull;

/**
 * The Tcp Disocvery being able to simulate network failure after given timeout.
 */
public class FailureSimulatingTcpDiscoverySpi extends TcpDiscoverySpi {
    /**
     * FOR TEST ONLY!! If not {@code null}, enables network timeout simulation. First value switches traffic droppage:
     * negative for all incoming, positive for all outgoing, 0 for both.
     */
    protected volatile IgnitePair<Integer> simulateNetTimeout;

    /** {@inheritDoc} */
    @Override protected Socket createSocket0(boolean encrypted) throws IOException {
        if (encrypted)
            throw new IllegalArgumentException("Failure simulation on encrypted socket isn't supported");

        return new SocketWrap();
    }

    /**
     * <strong>FOR TEST ONLY!!!</strong>
     * <p>
     * Enabled network timeout simulation.
     *
     * @param direction If negative, enables timeout simulation for reading incomming traffic. If positive, enables
     *                  timeout simulation on traffic.
     * @param delay     Milliseconds of awaiting before raising {@code SocketTimeoutException}.
     * @see SocketWrap#simulateNetFailure(Socket, int)
     */
    public void enableNetworkTimeoutSimulation(int direction, int delay) {
        simulateNetTimeout = new IgnitePair<>(direction, delay);
    }

    /**
     * <strong>FOR TEST ONLY!!!</strong>
     * <p>
     * If enabled, simulates network timeout. Throws {@code SocketTimeoutException} after the delay.
     *
     * @param sock         The socket to simulate failure at.
     * @param forceTimeout Overrides the value preset in {@link #enableNetworkTimeoutSimulation(int, int)}.
     * @see #enableNetworkTimeoutSimulation(int, int)
     */
    private void simulateNetFailure(Socket sock, int forceTimeout) throws SocketTimeoutException {
        IgnitePair<Integer> simulateNetTimeout = this.simulateNetTimeout;

        if (simulateNetTimeout == null)
            return;

        boolean incomingSock = sock.getLocalPort() < locPort + locPortRange + 1;

        if (incomingSock && simulateNetTimeout.get1() > 0 || !incomingSock && simulateNetTimeout.get1() < 0)
            return;

        int timeout = forceTimeout > 0 ? forceTimeout : simulateNetTimeout.get2();

        try {
            Thread.sleep(timeout);
        }
        catch (InterruptedException ignored) {
            // No-op.
        }

        throw new SocketTimeoutException("Simulated failure after delay: " + timeout + "ms.");
    }

    /**
     * @see #simulateNetFailure(Socket, int)
     */
    private void simulateNetFailure(Socket sock) throws SocketTimeoutException {
        simulateNetFailure(sock, 0);
    }

    /**
     * Network failure simulator.
     */
    private class SocketWrap extends Socket {
        /** {@inheritDoc} */
        @Override public OutputStream getOutputStream() throws IOException {
            OutputStream src = super.getOutputStream();

            return new OutputStream() {
                /** {@inheritDoc} */
                @Override public void write(@NotNull byte[] b) throws IOException {
                    simulateNetFailure(SocketWrap.this);

                    src.write(b);
                }

                /** {@inheritDoc} */
                @Override public void write(@NotNull byte[] b, int off, int len) throws IOException {
                    simulateNetFailure(SocketWrap.this);

                    src.write(b, off, len);
                }

                /** {@inheritDoc} */
                @Override public void write(int b) throws IOException {
                    simulateNetFailure(SocketWrap.this);

                    src.write(b);
                }

                /** {@inheritDoc} */
                @Override public void flush() throws IOException {
                    simulateNetFailure(SocketWrap.this);

                    src.flush();
                }

                /** {@inheritDoc} */
                @Override public void close() throws IOException {
                    src.close();
                }
            };
        }

        @Override public InputStream getInputStream() throws IOException {
            InputStream src = super.getInputStream();

            return new InputStream() {
                @Override public int read(@NotNull byte[] b) throws IOException {
                    simulateNetFailure(SocketWrap.this);

                    return src.read(b);
                }

                @Override public int read(@NotNull byte[] b, int off, int len) throws IOException {
                    simulateNetFailure(SocketWrap.this);

                    return src.read(b, off, len);
                }

                @Override public long skip(long n) throws IOException {
                    simulateNetFailure(SocketWrap.this);

                    return src.skip(n);
                }

                @Override public int available() throws IOException {
                    return src.available();
                }

                @Override public void close() throws IOException {
                    src.close();
                }

                @Override public synchronized void mark(int readlimit) {
                    src.mark(readlimit);
                }

                @Override public synchronized void reset() throws IOException {
                    src.reset();
                }

                @Override public boolean markSupported() {
                    return src.markSupported();
                }

                @Override public int read() throws IOException {
                    simulateNetFailure(SocketWrap.this);

                    return src.read();
                }
            };
        }

        /** {@inheritDoc} */
        @Override public void connect(SocketAddress endpoint) throws IOException {
            simulateNetFailure(this);

            super.connect(endpoint);
        }

        /** {@inheritDoc} */
        @Override public void connect(SocketAddress endpoint, int timeout) throws IOException {
            simulateNetFailure(this);

            super.connect(endpoint, timeout);
        }
    }
}
