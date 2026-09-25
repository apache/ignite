/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.spi.discovery.tcp;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpiInternalListener;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientReconnectMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryJoinRequestMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryPingResponse;
import org.apache.ignite.testframework.GridTestUtils.DiscoveryHook;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.testframework.GridTestUtils.DiscoverySpiListenerWrapper.wrap;

/**
 *
 */
public class TestTcpDiscoverySpi extends TcpDiscoverySpi implements IgniteDiscoverySpiInternalListenerSupport {
    /** */
    public boolean ignorePingResponse;

    /** Interceptor of discovery messages. */
    private DiscoveryHook discoHook;

    /** */
    private IgniteDiscoverySpiInternalListener internalLsnr;

    /** Latch released on {@link #unfreeze()}, {@code null} if the discovery I/O is not frozen. */
    private volatile CountDownLatch freezeLatch;

    /**
     * Freezes the discovery I/O of this node: every socket read and write blocks until {@link #unfreeze()} is called.
     * The node keeps accepting TCP connections. Emulates a node whose threads hang, e.g. at a long GC pause.
     */
    public synchronized void freeze() {
        if (freezeLatch == null)
            freezeLatch = new CountDownLatch(1);
    }

    /** Releases the discovery I/O frozen by {@link #freeze()}. */
    public synchronized void unfreeze() {
        if (freezeLatch != null) {
            freezeLatch.countDown();

            freezeLatch = null;
        }
    }

    /**
     * Blocks while the discovery I/O is frozen.
     *
     * @throws InterruptedIOException If interrupted.
     */
    private void awaitUnfrozen() throws InterruptedIOException {
        CountDownLatch latch = freezeLatch;

        if (latch == null)
            return;

        try {
            latch.await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new InterruptedIOException("Interrupted while discovery I/O is frozen.");
        }
    }

    /** {@inheritDoc} */
    @Override protected void write(TcpDiscoveryIoSession ses, byte[] data, long timeout) throws IOException,
        IgniteCheckedException {
        awaitUnfrozen();

        super.write(ses, data, timeout);
    }

    /** {@inheritDoc} */
    @Override protected void writeReceipt(TcpDiscoveryIoSession ses, int res, long timeout) throws IOException,
        IgniteCheckedException {
        awaitUnfrozen();

        super.writeReceipt(ses, res, timeout);
    }

    /** {@inheritDoc} */
    @Override protected <T extends Message> T readMessage(TcpDiscoveryIoSession ses, long timeout) throws IOException,
        IgniteCheckedException {
        awaitUnfrozen();

        try {
            return super.readMessage(ses, timeout);
        }
        finally {
            // A reader may have been blocked on the socket before the freeze, so hold the result (a message or
            // a failure) until unfreeze.
            awaitUnfrozen();
        }
    }

    /** {@inheritDoc} */
    @Override protected int readReceipt(TcpDiscoveryIoSession ses, long timeout) throws IOException {
        awaitUnfrozen();

        try {
            return super.readReceipt(ses, timeout);
        }
        finally {
            awaitUnfrozen();
        }
    }

    /** {@inheritDoc} */
    @Override protected void writeMessage(TcpDiscoveryIoSession ses, TcpDiscoveryAbstractMessage msg, long timeout) throws IOException,
        IgniteCheckedException {
        awaitUnfrozen();

        if (msg instanceof TcpDiscoveryPingResponse && ignorePingResponse)
            return;

        if (internalLsnr != null) {
            if (msg instanceof TcpDiscoveryJoinRequestMessage)
                internalLsnr.beforeJoin(locNode, log);

            if (msg instanceof TcpDiscoveryClientReconnectMessage)
                internalLsnr.beforeReconnect(locNode, log);
        }

        super.writeMessage(ses, msg, timeout);
    }

    /** {@inheritDoc} */
    @Override public void simulateNodeFailure() {
        super.simulateNodeFailure();
    }

    /** {@inheritDoc} */
    @Override public void setListener(@Nullable DiscoverySpiListener lsnr) {
        super.setListener(lsnr == null || discoHook == null ? lsnr : wrap(lsnr, discoHook));
    }

    /** {@inheritDoc} */
    @Override public void sendCustomEvent(DiscoverySpiCustomMessage msg) throws IgniteException {
        IgniteDiscoverySpiInternalListener internalLsnr = this.internalLsnr;

        if (internalLsnr != null) {
            if (!internalLsnr.beforeSendCustomEvent(this, log, msg))
                return;
        }

        super.sendCustomEvent(msg);
    }

    /** */
    @Override public void setInternalListener(IgniteDiscoverySpiInternalListener lsnr) {
        internalLsnr = lsnr;
    }

    /**
     * Sets interceptor of discovery messages. Note that {@link DiscoveryHook} must be set before SPI start.
     * Otherwise, this method call will take no effect.
     *
     * @param discoHook Interceptor of discovery messages.
     */
    public void discoveryHook(DiscoveryHook discoHook) {
        assert !started();

        this.discoHook = discoHook;
    }

    /** */
    public static @Nullable TcpDiscoveryAbstractMessage decodeMessage(GridKernalContext ctx, byte[] data) {
        if (Arrays.equals(U.IGNITE_HEADER, data))
            return null;

        Socket dataSock = new Socket() {
            @Override public InputStream getInputStream() {
                return new ByteArrayInputStream(data);
            }

            @Override public OutputStream getOutputStream() {
                return new ByteArrayOutputStream();
            }
        };

        try (dataSock) {
            return new TcpDiscoveryIoSession(ctx, dataSock).readMessage();
        }
        catch (Exception e) {
            throw new IgniteException("Failed to decode a message", e);
        }
    }
}
