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

package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.OdbcConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.nio.*;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgnitePortProtocol;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteOrder;

/**
 * TCP server that handles communication with ODBC driver.
 */
public class OdbcTcpServer {
    /** Server. */
    private GridNioServer<OdbcRequest> srv;

    /** Logger. */
    private final IgniteLogger log;

    /** Context. */
    private final GridKernalContext ctx;

    /**
     * @param ctx Kernel context.
     */
    public OdbcTcpServer(GridKernalContext ctx) {
        assert ctx != null;
        assert ctx.config().getConnectorConfiguration() != null;

        this.ctx = ctx;

        log = ctx.log(getClass());
    }

    /**
     * Start ODBC TCP server.
     *
     * @param hnd ODBC protocol handler.
     * @throws IgniteCheckedException
     */
    public void start(final OdbcProtocolHandler hnd) throws IgniteCheckedException {
        OdbcConfiguration cfg = ctx.config().getOdbcConfiguration();

        assert cfg != null;

        GridNioServerListener<OdbcRequest> listener = new OdbcTcpNioListener(log, hnd);

        GridNioParser parser = new OdbcParser(ctx);

        try {
            InetAddress host = resolveOdbcTcpHost(ctx.config());

            int port = cfg.getPort();

            if (startTcpServer(host, port, listener, parser, cfg)) {
                log.debug("ODBC Server has started on TCP port " + port);

                return;
            }

            U.warn(log, "Failed to start ODBC server (possibly all ports in range are in use) " +
                    "[port=" + port + ", host=" + host + ']');
        }
        catch (IOException e) {
            U.warn(log, "Failed to start ODBC server: " + e.getMessage(),
                    "Failed to start ODBC server. Check odbcTcpHost configuration property.");
        }
    }

    /**
     * Stop ODBC TCP server.
     */
    public void stop() {
        if (srv != null) {
            ctx.ports().deregisterPorts(getClass());

            srv.stop();
        }
    }

    /**
     * Resolves host for server using grid configuration.
     *
     * @param cfg Grid configuration.
     * @return Host address.
     * @throws IOException If failed to resolve host.
     */
    private InetAddress resolveOdbcTcpHost(IgniteConfiguration cfg) throws IOException {
        String host = null;

        OdbcConfiguration odbcCfg = cfg.getOdbcConfiguration();

        if (odbcCfg != null)
            host = odbcCfg.getHost();

        if (host == null)
            host = cfg.getLocalHost();

        return U.resolveLocalHost(host);
    }

    /**
     * Tries to start server with given parameters.
     *
     * @param hostAddr Host on which server should be bound.
     * @param port Port on which server should be bound.
     * @param listener Server message listener.
     * @param parser Server message parser.
     * @param cfg Configuration for other parameters.
     * @return {@code True} if server successfully started, {@code false} if port is used and
     *      server was unable to start.
     */
    private boolean startTcpServer(InetAddress hostAddr, int port, GridNioServerListener<OdbcRequest> listener,
                                   GridNioParser parser, OdbcConfiguration cfg) {
        try {
            GridNioFilter codec = new GridNioCodecFilter(parser, log, false);

            GridNioFilter[] filters;

            filters = new GridNioFilter[] { codec };

            srv = GridNioServer.<OdbcRequest>builder()
                    .address(hostAddr)
                    .port(port)
                    .listener(listener)
                    .logger(log)
                    .selectorCount(cfg.getSelectorCount())
                    .gridName(ctx.gridName())
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
                log.debug("Failed to start ODBC server on port " + port + ": " + e.getMessage());

            return false;
        }
    }
}
