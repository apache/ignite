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

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.client.marshaller.GridClientMarshaller;
import org.apache.ignite.internal.client.marshaller.jdk.GridClientJdkMarshaller;
import org.apache.ignite.internal.client.marshaller.optimized.GridClientOptimizedMarshaller;
import org.apache.ignite.internal.client.marshaller.optimized.GridClientZipOptimizedMarshaller;
import org.apache.ignite.internal.client.ssl.GridSslContextFactory;
import org.apache.ignite.internal.processors.rest.GridRestProtocolHandler;
import org.apache.ignite.internal.processors.rest.client.message.GridClientMessage;
import org.apache.ignite.internal.processors.rest.protocols.GridRestProtocolAdapter;
import org.apache.ignite.internal.util.nio.GridNioCodecFilter;
import org.apache.ignite.internal.util.nio.GridNioFilter;
import org.apache.ignite.internal.util.nio.GridNioParser;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.nio.GridNioServerListener;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.ssl.GridNioSslFilter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.spi.IgnitePortProtocol;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.nio.GridNioSessionMetaKey.MARSHALLER;

/**
 * TCP binary protocol implementation.
 */
public class GridTcpRestProtocol extends GridRestProtocolAdapter {
    /** Server. */
    private GridNioServer<GridClientMessage> srv;

    /** JDK marshaller. */
    private final Marshaller jdkMarshaller = new JdkMarshaller();

    /** NIO server listener. */
    private GridTcpRestNioListener lsnr;

    /** @param ctx Context. */
    public GridTcpRestProtocol(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @return JDK marshaller.
     */
    Marshaller jdkMarshaller() {
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

    /** {@inheritDoc} */
    @Override public String name() {
        return "TCP binary";
    }

    /** {@inheritDoc} */
    @SuppressWarnings("BusyWait")
    @Override public void start(final GridRestProtocolHandler hnd) throws IgniteCheckedException {
        assert hnd != null;

        ConnectorConfiguration cfg = ctx.config().getConnectorConfiguration();

        assert cfg != null;

        lsnr = new GridTcpRestNioListener(log, this, hnd, ctx);

        GridNioParser parser = new GridTcpRestParser(false);

        try {
            host = resolveRestTcpHost(ctx.config());

            SSLContext sslCtx = null;

            if (cfg.isSslEnabled()) {
                Factory<SSLContext> igniteFactory = ctx.config().getSslContextFactory();

                Factory<SSLContext> factory = cfg.getSslFactory();

                // This factory deprecated and will be removed.
                GridSslContextFactory depFactory = cfg.getSslContextFactory();

                if (factory == null && depFactory == null && igniteFactory == null)
                    // Thrown SSL exception instead of IgniteCheckedException for writing correct warning message into log.
                    throw new SSLException("SSL is enabled, but SSL context factory is not specified.");

                if (factory != null)
                    sslCtx = factory.create();
                else if (depFactory != null)
                    sslCtx = depFactory.createSslContext();
                else
                    sslCtx = igniteFactory.create();
            }
            int startPort = cfg.getPort();
            int portRange = cfg.getPortRange();
            int lastPort = portRange == 0 ? startPort : startPort + portRange - 1;

            for (int port0 = startPort; port0 <= lastPort; port0++) {
                if (startTcpServer(host, port0, lsnr, parser, sslCtx, cfg)) {
                    port = port0;

                    if (log.isInfoEnabled())
                        log.info(startInfo());

                    return;
                }
            }

            U.warn(log, "Failed to start TCP binary REST server (possibly all ports in range are in use) " +
                "[firstPort=" + cfg.getPort() + ", lastPort=" + lastPort + ", host=" + host + ']');
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

        ArrayList<PluginProvider> providers = new ArrayList<>(ctx.plugins().allProviders());

        GridClientOptimizedMarshaller optMarsh = new GridClientOptimizedMarshaller(providers);

        marshMap.put(GridClientOptimizedMarshaller.ID, optMarsh);
        marshMap.put(GridClientZipOptimizedMarshaller.ID, new GridClientZipOptimizedMarshaller(optMarsh, providers));
        marshMap.put(GridClientJdkMarshaller.ID, new GridClientJdkMarshaller());

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
        String host = cfg.getConnectorConfiguration().getHost();

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
        GridNioParser parser, @Nullable SSLContext sslCtx, ConnectorConfiguration cfg) {
        try {
            GridNioFilter codec = new GridNioCodecFilter(parser, log, false);

            GridNioFilter[] filters;

            if (sslCtx != null) {
                GridNioSslFilter sslFilter = new GridNioSslFilter(sslCtx,
                    cfg.isDirectBuffer(), ByteOrder.nativeOrder(), log);

                sslFilter.directMode(false);

                boolean auth = cfg.isSslClientAuth();

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
                .selectorCount(cfg.getSelectorCount())
                .gridName(ctx.gridName())
                .serverName("tcp-rest")
                .tcpNoDelay(cfg.isNoDelay())
                .directBuffer(cfg.isDirectBuffer())
                .byteOrder(ByteOrder.nativeOrder())
                .socketSendBufferSize(cfg.getSendBufferSize())
                .socketReceiveBufferSize(cfg.getReceiveBufferSize())
                .sendQueueLimit(cfg.getSendQueueLimit())
                .filters(filters)
                .directMode(false)
                .build();

            srv.idleTimeout(cfg.getIdleTimeout());

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
        return IgniteNodeAttributes.ATTR_REST_TCP_ADDRS;
    }

    /** {@inheritDoc} */
    @Override protected String getHostNamePropertyName() {
        return IgniteNodeAttributes.ATTR_REST_TCP_HOST_NAMES;
    }

    /** {@inheritDoc} */
    @Override protected String getPortPropertyName() {
        return IgniteNodeAttributes.ATTR_REST_TCP_PORT;
    }
}
