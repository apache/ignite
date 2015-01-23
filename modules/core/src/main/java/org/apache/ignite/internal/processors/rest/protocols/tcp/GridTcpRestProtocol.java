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

package org.apache.ignite.internal.processors.rest.protocols.tcp;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.marshaller.jdk.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.client.marshaller.*;
import org.apache.ignite.client.marshaller.jdk.*;
import org.apache.ignite.client.marshaller.optimized.*;
import org.apache.ignite.client.ssl.*;
import org.apache.ignite.internal.processors.rest.*;
import org.apache.ignite.internal.processors.rest.client.message.*;
import org.apache.ignite.internal.processors.rest.protocols.*;
import org.apache.ignite.internal.util.direct.*;
import org.apache.ignite.internal.util.nio.*;
import org.apache.ignite.internal.util.nio.ssl.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import javax.net.ssl.*;
import java.io.*;
import java.net.*;
import java.nio.*;
import java.util.*;

import static org.apache.ignite.internal.util.nio.GridNioSessionMetaKey.*;

/**
 * TCP binary protocol implementation.
 */
public class GridTcpRestProtocol extends GridRestProtocolAdapter {
    /** Server. */
    private GridNioServer<GridClientMessage> srv;

    /** JDK marshaller. */
    private final IgniteMarshaller jdkMarshaller = new IgniteJdkMarshaller();

    /** NIO server listener. */
    private GridTcpRestNioListener lsnr;

    /** Message reader. */
    private final GridNioMessageReader msgReader = new GridNioMessageReader() {
        @Override public boolean read(@Nullable UUID nodeId, GridTcpCommunicationMessageAdapter msg, ByteBuffer buf) {
            assert msg != null;
            assert buf != null;

            msg.messageReader(this, nodeId);

            return msg.readFrom(buf);
        }

        @Nullable @Override public GridTcpMessageFactory messageFactory() {
            return null;
        }
    };

    /** Message writer. */
    private final GridNioMessageWriter msgWriter = new GridNioMessageWriter() {
        @Override public boolean write(@Nullable UUID nodeId, GridTcpCommunicationMessageAdapter msg, ByteBuffer buf) {
            assert msg != null;
            assert buf != null;

            msg.messageWriter(this, nodeId);

            return msg.writeTo(buf);
        }

        @Override public int writeFully(@Nullable UUID nodeId, GridTcpCommunicationMessageAdapter msg, OutputStream out,
            ByteBuffer buf) throws IOException {
            assert msg != null;
            assert out != null;
            assert buf != null;
            assert buf.hasArray();

            msg.messageWriter(this, nodeId);

            boolean finished = false;
            int cnt = 0;

            while (!finished) {
                finished = msg.writeTo(buf);

                out.write(buf.array(), 0, buf.position());

                cnt += buf.position();

                buf.clear();
            }

            return cnt;
        }
    };

    /** @param ctx Context. */
    public GridTcpRestProtocol(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @return JDK marshaller.
     */
    IgniteMarshaller jdkMarshaller() {
        return jdkMarshaller;
    }

    /**
     * Returns marshaller.
     *
     * @param ses Session.
     * @return Marshaller.
     */
    GridClientMarshaller marshaller(GridNioSession ses) {
        GridClientMarshaller marsh = ses.meta(MARSHALLER.ordinal());

        assert marsh != null;

        return marsh;
    }

    /**
     * @param ses Session.
     * @return Whether portable marshaller is used.
     */
    boolean portableMode(GridNioSession ses) {
        return ctx.portable().isPortable(marshaller(ses));
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return "TCP binary";
    }

    /** {@inheritDoc} */
    @SuppressWarnings("BusyWait")
    @Override public void start(final GridRestProtocolHandler hnd) throws IgniteCheckedException {
        assert hnd != null;

        ClientConnectionConfiguration cfg = ctx.config().getClientConnectionConfiguration();

        assert cfg != null;

        lsnr = new GridTcpRestNioListener(log, this, hnd, ctx);

        GridNioParser parser = new GridTcpRestDirectParser(this, msgReader);

        try {
            host = resolveRestTcpHost(ctx.config());

            SSLContext sslCtx = null;

            if (cfg.isRestTcpSslEnabled()) {
                GridSslContextFactory factory = cfg.getRestTcpSslContextFactory();

                if (factory == null)
                    // Thrown SSL exception instead of IgniteCheckedException for writing correct warning message into log.
                    throw new SSLException("SSL is enabled, but SSL context factory is not specified.");

                sslCtx = factory.createSslContext();
            }

            int lastPort = cfg.getRestTcpPort() + cfg.getRestPortRange() - 1;

            for (int port0 = cfg.getRestTcpPort(); port0 <= lastPort; port0++) {
                if (startTcpServer(host, port0, lsnr, parser, sslCtx, cfg)) {
                    port = port0;

                    if (log.isInfoEnabled())
                        log.info(startInfo());

                    return;
                }
            }

            U.warn(log, "Failed to start TCP binary REST server (possibly all ports in range are in use) " +
                "[firstPort=" + cfg.getRestTcpPort() + ", lastPort=" + lastPort + ", host=" + host + ']');
        }
        catch (SSLException e) {
            U.warn(log, "Failed to start " + name() + " protocol on port " + port + ": " + e.getMessage(),
                "Failed to start " + name() + " protocol on port " + port + ". Check if SSL context factory is " +
                    "properly configured.");
        }
        catch (IOException e) {
            U.warn(log, "Failed to start " + name() + " protocol on port " + port + ": " + e.getMessage(),
                "Failed to start " + name() + " protocol on port " + port + ". " +
                    "Check restTcpHost configuration property.");
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() {
        super.onKernalStart();

        Map<Byte, GridClientMarshaller> marshMap = new HashMap<>();

        marshMap.put(GridClientOptimizedMarshaller.ID, new GridClientOptimizedMarshaller());
        marshMap.put(GridClientJdkMarshaller.ID, new GridClientJdkMarshaller());
        marshMap.put((byte)0, ctx.portable().portableMarshaller());

        lsnr.marshallers(marshMap);
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        if (srv != null) {
            ctx.ports().deregisterPorts(getClass());

            srv.stop();
        }

        if (log.isInfoEnabled())
            log.info(stopInfo());
    }

    /**
     * Resolves host for REST TCP server using grid configuration.
     *
     * @param cfg Grid configuration.
     * @return REST host.
     * @throws IOException If failed to resolve REST host.
     */
    private InetAddress resolveRestTcpHost(IgniteConfiguration cfg) throws IOException {
        String host = cfg.getClientConnectionConfiguration().getRestTcpHost();

        if (host == null)
            host = cfg.getLocalHost();

        return U.resolveLocalHost(host);
    }

    /**
     * Tries to start server with given parameters.
     *
     * @param hostAddr Host on which server should be bound.
     * @param port Port on which server should be bound.
     * @param lsnr Server message listener.
     * @param parser Server message parser.
     * @param sslCtx SSL context in case if SSL is enabled.
     * @param cfg Configuration for other parameters.
     * @return {@code True} if server successfully started, {@code false} if port is used and
     *      server was unable to start.
     */
    private boolean startTcpServer(InetAddress hostAddr, int port, GridNioServerListener<GridClientMessage> lsnr,
        GridNioParser parser, @Nullable SSLContext sslCtx, ClientConnectionConfiguration cfg) {
        try {
            GridNioFilter codec = new GridNioCodecFilter(parser, log, true);

            GridNioFilter[] filters;

            if (sslCtx != null) {
                GridNioSslFilter sslFilter = new GridNioSslFilter(sslCtx, log);

                sslFilter.directMode(true);

                boolean auth = cfg.isRestTcpSslClientAuth();

                sslFilter.wantClientAuth(auth);

                sslFilter.needClientAuth(auth);

                filters = new GridNioFilter[] {
                    codec,
                    sslFilter
                };
            }
            else
                filters = new GridNioFilter[] { codec };

            srv = GridNioServer.<GridClientMessage>builder()
                .address(hostAddr)
                .port(port)
                .listener(lsnr)
                .logger(log)
                .selectorCount(cfg.getRestTcpSelectorCount())
                .gridName(ctx.gridName())
                .tcpNoDelay(cfg.isRestTcpNoDelay())
                .directBuffer(cfg.isRestTcpDirectBuffer())
                .byteOrder(ByteOrder.nativeOrder())
                .socketSendBufferSize(cfg.getRestTcpSendBufferSize())
                .socketReceiveBufferSize(cfg.getRestTcpReceiveBufferSize())
                .sendQueueLimit(cfg.getRestTcpSendQueueLimit())
                .filters(filters)
                .directMode(true)
                .messageWriter(msgWriter)
                .build();

            srv.idleTimeout(cfg.getRestIdleTimeout());

            srv.start();

            ctx.ports().registerPort(port, IgnitePortProtocol.TCP, getClass());

            return true;
        }
        catch (IgniteCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to start " + name() + " protocol on port " + port + ": " + e.getMessage());

            return false;
        }
    }

    /** {@inheritDoc} */
    @Override protected String getAddressPropertyName() {
        return GridNodeAttributes.ATTR_REST_TCP_ADDRS;
    }

    /** {@inheritDoc} */
    @Override protected String getHostNamePropertyName() {
        return GridNodeAttributes.ATTR_REST_TCP_HOST_NAMES;
    }

    /** {@inheritDoc} */
    @Override protected String getPortPropertyName() {
        return GridNodeAttributes.ATTR_REST_TCP_PORT;
    }
}
