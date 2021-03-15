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
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class FailureSimulatingTcpDiscoverySpi extends TcpDiscoverySpi {
    /**
     * FOR TEST ONLY!!
     * If not {@code null}, enables network timeout simulation.
     * First value switches traffic droppage: negative for all incoming, positive for all outgoing, 0 for both.
     */
    protected volatile IgnitePair<Integer> simulateNetTimeout;

    /** {@inheritDoc} */
    @Override protected Socket createSocket0(boolean encrypted) throws IOException {
        if(encrypted)
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
     * @see SocketWrap#simulateNetFailure()
     */
    public void enableNetworkTimeoutSimulation(int direction, int delay) {
        simulateNetTimeout = new IgnitePair<>(direction, delay);
    }

    /**
     * <strong>FOR TEST ONLY!!!</strong>
     * <p>
     * If enabled, simulates network timeout. Throws {@code SocketTimeoutException} after a delay.
     *
     * @see #enableNetworkTimeoutSimulation(int, int)
     */
    private void simulateNetFailure(int forceTimeout) throws SocketTimeoutException {
        IgnitePair<Integer> simulateNetTimeout = this.simulateNetTimeout;

        if (simulateNetTimeout == null)
            return;

        boolean incoming = getLocalPort() < locPort + locPortRange + 1;

        if (incoming && simulateNetTimeout.get1() > 0 || !incoming && simulateNetTimeout.get1() < 0)
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

    /** */
    private void simulateNetFailure() throws SocketTimeoutException {
        simulateNetFailure(0);
    }

        /**
         * @return Wrap for the OutputStream being able to simulate network timeout.
         */
    private OutputStream wrapWithFailureSimulator(OutputStream src){
        return new OutputStream() {
            /** {@inheritDoc} */
            @Override public void write(@NotNull byte[] b) throws IOException {
                simulateNetFailure();

                src.write(b);
            }

            /** {@inheritDoc} */
            @Override public void write(@NotNull byte[] b, int off, int len) throws IOException {
                simulateNetFailure();

                src.write(b, off, len);
            }

            /** {@inheritDoc} */
            @Override public void write(int b) throws IOException {
                simulateNetFailure();

                src.write(b);
            }
        };
    }

    /**
     * Network failure simulator.
     */
    private class SocketWrap extends Socket {
        /** {@inheritDoc} */
        @Override public OutputStream getOutputStream() throws IOException {
            return wrapWithFailureSimulator(super.getOutputStream());
        }

        /** {@inheritDoc} */
        @Override public void connect(SocketAddress endpoint) throws IOException {
            simulateNetFailure();

            super.connect(endpoint);
        }

        /** {@inheritDoc} */
        @Override public void connect(SocketAddress endpoint, int timeout) throws IOException {
            simulateNetFailure(timeout);

            super.connect(endpoint, timeout);
        }
    }
}
