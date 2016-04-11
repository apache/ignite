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

package org.apache.ignite.internal.processors.redis;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteOrder;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.RedisConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.nio.GridNioCodecFilter;
import org.apache.ignite.internal.util.nio.GridNioFilter;
import org.apache.ignite.internal.util.nio.GridNioParser;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.nio.GridNioServerListener;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgnitePortProtocol;

/**
 * Redis protocol server.
 */
public class GridRedisServer {
    /** Context. */
    protected final GridKernalContext ctx;

    /** Server. */
    private GridNioServer<GridRedisMessage> srv;

    /** NIO server listener. */
    private GridRedisNioListener lsnr;

    /** Host used by this protocol. */
    protected InetAddress host;

    /** Port used by this protocol. */
    protected int port;

    /** Logger. */
    protected final IgniteLogger log;

    public GridRedisServer(GridKernalContext ctx) {
        assert ctx != null;

        this.ctx = ctx;

        log = ctx.log(getClass());
    }

    public void start(final GridRedisProtocolHandler hnd) throws IgniteCheckedException {
        assert hnd != null;

        RedisConfiguration cfg = ctx.config().getRedisConfiguration();

        assert cfg != null;

        lsnr = new GridRedisNioListener(hnd, ctx);

        try {
            host = resolveRedisHost(ctx.config());

            int lastPort = cfg.getPort() + cfg.getPortRange() - 1;

            for (int port0 = cfg.getPort(); port0 <= lastPort; port0++) {
                if (startServer(host, port0, lsnr, cfg)) {
                    port = port0;

                    if (log.isInfoEnabled())
                        log.info(startInfo());

                    return;
                }
            }

            U.warn(log, "Failed to start " + name() + " server (possibly all ports in range are in use) " +
                "[firstPort=" + cfg.getPort() + ", lastPort=" + lastPort + ", host=" + host + ']');
        }
        catch (IOException e) {
            U.warn(log, "Failed to start " + name() + " on port " + port + ": " + e.getMessage(),
                "Failed to start " + name() + " on port " + port + ". " +
                    "Check redisHost configuration property.");
        }

    }

    /**
     * Stops the server.
     */
    public void stop() {
        if (srv != null) {
            ctx.ports().deregisterPorts(getClass());

            srv.stop();
        }

        if (log.isInfoEnabled())
            log.info(stopInfo());
    }

    /**
     * Starts a server with given parameters.
     *
     * @param hostAddr Host on which server should be bound.
     * @param port Port on which server should be bound.
     * @param lsnr Server message listener.
     * @param cfg Configuration for other parameters.
     * @return {@code True} if server successfully started, {@code false} if port is used and
     * server was unable to start.
     */
    private boolean startServer(InetAddress hostAddr, int port, GridNioServerListener<GridRedisMessage> lsnr,
        RedisConfiguration cfg) {
        try {
            GridNioParser parser = new GridRedisProtocolParser(log);

            GridNioFilter codec = new GridNioCodecFilter(parser, log, false);

            GridNioFilter[] filters;

            filters = new GridNioFilter[] {codec};

            srv = GridNioServer.<GridRedisMessage>builder()
                .address(hostAddr)
                .port(port)
                .listener(lsnr)
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
                log.debug("Failed to start " + name() + " on port " + port + ": " + e.getMessage());

            return false;
        }
    }

    /**
     * Resolves host for Redis server using grid configuration.
     *
     * @param cfg Grid configuration.
     * @return Redis host.
     * @throws IOException If failed to resolve REST host.
     */
    private InetAddress resolveRedisHost(IgniteConfiguration cfg) throws IOException {
        String host = cfg.getRedisConfiguration().getHost();

        if (host == null)
            host = cfg.getLocalHost();

        return U.resolveLocalHost(host);
    }

    /**
     * @return Start information string.
     */
    protected String startInfo() {
        return "Command protocol successfully started [name=" + name() + ", host=" + host + ", port=" + port + ']';
    }

    /**
     * @return Stop information string.
     */
    protected String stopInfo() {
        return "Command protocol successfully stopped: " + name();
    }

    /**
     * @return Protocol name.
     */
    public String name() {
        return "Redis protocol";
    }
}
