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

package org.apache.ignite.internal.client.thin.io.gridnioserver;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.net.ssl.SSLHandshakeException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.client.thin.ClientSslUtils;
import org.apache.ignite.internal.client.thin.io.ClientConnection;
import org.apache.ignite.internal.client.thin.io.ClientConnectionMultiplexer;
import org.apache.ignite.internal.client.thin.io.ClientConnectionStateHandler;
import org.apache.ignite.internal.client.thin.io.ClientMessageHandler;
import org.apache.ignite.internal.ssl.SslContextProvider;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.nio.GridNioCodecFilter;
import org.apache.ignite.internal.util.nio.GridNioFilter;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.ssl.GridNioSslFilter;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.logger.NullLogger;

/**
 * Client connection multiplexer based on {@link org.apache.ignite.internal.util.nio.GridNioServer}.
 */
public class GridNioClientConnectionMultiplexer implements ClientConnectionMultiplexer {
    /** Worker thread prefix. */
    private static final String THREAD_PREFIX = "thin-client-channel";

    /** */
    private static final int CLIENT_MODE_PORT = -1;

    /** */
    private final GridNioServer<ByteBuffer> srv;

    /** Owner of the SSL context, {@code null} if SSL is disabled. */
    private final SslContextProvider sslCtxProvider;

    /** Set when a TLS handshake was refused, so that the next connection is opened on rebuilt certificates. */
    private final AtomicBoolean reloadSsl = new AtomicBoolean();

    /** */
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    /** */
    private final int connTimeout;

    /**
     * Constructor.
     *
     * @param cfg Client config.
     */
    public GridNioClientConnectionMultiplexer(ClientConfiguration cfg) {
        IgniteLogger gridLog = new NullLogger();

        GridNioFilter[] filters;

        GridNioFilter codecFilter = new GridNioCodecFilter(new GridNioClientParser(), gridLog, false);

        if (cfg.getSslMode() != SslMode.DISABLED) {
            sslCtxProvider = new SslContextProvider(() -> ClientSslUtils.getSslContext(cfg));

            GridNioSslFilter sslFilter =
                new GridNioSslFilter(sslCtxProvider, true, ByteOrder.nativeOrder(), gridLog, null, null);

            sslFilter.directMode(false);
            filters = new GridNioFilter[] {codecFilter, sslFilter};
        }
        else {
            sslCtxProvider = null;
            filters = new GridNioFilter[] {codecFilter};
        }

        connTimeout = cfg.getHandshakeTimeout();

        try {
            srv = GridNioServer.<ByteBuffer>builder()
                    .port(CLIENT_MODE_PORT)
                    .listener(new GridNioClientListener())
                    .filters(filters)
                    .logger(gridLog)
                    .selectorCount(1) // Using more selectors does not seem to improve performance.
                    .byteOrder(ByteOrder.nativeOrder())
                    .directBuffer(true)
                    .directMode(false)
                    .igniteInstanceName("thinClient")
                    .serverName(THREAD_PREFIX)
                    .idleTimeout(Long.MAX_VALUE)
                    .socketReceiveBufferSize(cfg.getReceiveBufferSize())
                    .socketSendBufferSize(cfg.getSendBufferSize())
                    .tcpNoDelay(true)
                    .build();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void start() {
        rwLock.writeLock().lock();

        try {
            srv.start();
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized void stop() {
        rwLock.writeLock().lock();

        try {
            srv.stop();
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public ClientConnection open(InetSocketAddress addr,
                                           ClientMessageHandler msgHnd,
                                           ClientConnectionStateHandler stateHnd)
            throws ClientConnectionException {
        rwLock.readLock().lock();

        try {
            if (reloadSsl.compareAndSet(true, false))
                reloadSslContext();

            SocketChannel ch = null;
            try {
                ch = SocketChannel.open();
                ch.socket().connect(new InetSocketAddress(addr.getHostName(), addr.getPort()), connTimeout);
            }
            catch (Exception e) {
                if (ch != null) {
                    if (ch.socket() != null) {
                        try {
                            ch.socket().close();
                        }
                        catch (Exception ignored) {
                            // ignore close exception
                        }
                    }

                    try {
                        ch.close();
                    }
                    catch (Exception ignored) {
                        // ignore close exception
                    }
                }
                throw new ClientConnectionException(e.getMessage(), e);
            }

            Map<Integer, Object> meta = new HashMap<>();
            IgniteInternalFuture<?> sslHandshakeFut = null;

            if (sslCtxProvider != null) {
                sslHandshakeFut = new GridFutureAdapter<>();

                meta.put(GridNioSslFilter.HANDSHAKE_FUT_META_KEY, sslHandshakeFut);
            }

            IgniteInternalFuture<GridNioSession> sesFut = srv.createSession(ch, meta, false, null);

            if (sesFut.error() != null)
                sesFut.get();

            if (sslHandshakeFut != null) {
                try {
                    sslHandshakeFut.get();
                }
                catch (Exception e) {
                    // A refused handshake may mean the certificates on disk have been rotated since, so the next
                    // attempt is made with them. A connection that merely dropped must not count: that happens on
                    // every node restart, and rebuilding the context there would also pick up a key store the
                    // operator has staged but not yet put in use anywhere else.
                    if (X.hasCause(e, SSLHandshakeException.class))
                        reloadSsl.set(true);

                    throw e;
                }
            }

            GridNioSession ses = sesFut.get();

            return new GridNioClientConnection(ses, msgHnd, stateHnd);
        }
        catch (Exception e) {
            throw new ClientConnectionException(e.getMessage() + " [remoteAddress=" + addr + ']', e);
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Rebuilds the SSL context out of the files on disk, so that connections opened afterwards use the certificates
     * that are there now. The context in use is kept if the new one cannot be built.
     */
    private void reloadSslContext() {
        try {
            sslCtxProvider.reloadSslContext();
        }
        catch (Exception ignored) {
            // The connection attempt fails on its own, and the certificates in use stay untouched.
        }
    }
}
