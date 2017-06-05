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

import java.net.InetAddress;
import java.nio.ByteOrder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.OdbcConfiguration;
import org.apache.ignite.configuration.SqlConnectorConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.HostAndPortRange;
import org.apache.ignite.internal.util.nio.GridNioAsyncNotifyFilter;
import org.apache.ignite.internal.util.nio.GridNioCodecFilter;
import org.apache.ignite.internal.util.nio.GridNioFilter;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgnitePortProtocol;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

/**
 * SQL processor.
 */
public class SqlListenerProcessor extends GridProcessorAdapter {
    /** Default number of selectors. */
    private static final int DFLT_SELECTOR_CNT = Math.min(4, Runtime.getRuntime().availableProcessors());

    /** Default TCP direct buffer flag. */
    private static final boolean DFLT_TCP_DIRECT_BUF = false;

    /** Busy lock. */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** TCP Server. */
    private GridNioServer<byte[]> srv;

    /** Executor service. */
    private ExecutorService execSvc;

    /**
     * @param ctx Kernal context.
     */
    public SqlListenerProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start(boolean activeOnStart) throws IgniteCheckedException {
        IgniteConfiguration cfg = ctx.config();

        SqlConnectorConfiguration sqlCfg = prepareConfiguration(cfg);

        if (sqlCfg != null) {
            try {
                validateConfiguration(sqlCfg);

                // Resolve host.
                String host = sqlCfg.getHost();

                if (host == null)
                    host = cfg.getLocalHost();

                InetAddress hostAddr;

                try {
                    hostAddr = U.resolveLocalHost(host);
                }
                catch (Exception e) {
                    throw new IgniteCheckedException("Failed to resolve SQL connector host: " + host, e);
                }

                execSvc = new IgniteThreadPoolExecutor(
                    "sql-connector",
                    cfg.getIgniteInstanceName(),
                    sqlCfg.getThreadPoolSize(),
                    sqlCfg.getThreadPoolSize(),
                    0,
                    new LinkedBlockingQueue<Runnable>());

                Exception lastErr = null;

                int portTo = sqlCfg.getPort() + sqlCfg.getPortRange();

                if (portTo <= 0) // Handle int overflow.
                    portTo = Integer.MAX_VALUE;

                for (int port = sqlCfg.getPort(); port <= portTo && port <= 65535; port++) {
                    try {
                        GridNioFilter[] filters = new GridNioFilter[] {
                            new GridNioAsyncNotifyFilter(ctx.igniteInstanceName(), execSvc, log) {
                                @Override public void onSessionOpened(GridNioSession ses)
                                    throws IgniteCheckedException {
                                    proceedSessionOpened(ses);
                                }
                            },
                            new GridNioCodecFilter(new SqlListenerBufferedParser(), log, false)
                        };

                        int maxOpenCursors = sqlCfg.getMaxOpenCursorsPerConnection();

                        GridNioServer<byte[]> srv0 = GridNioServer.<byte[]>builder()
                            .address(hostAddr)
                            .port(port)
                            .listener(new SqlListenerNioListener(ctx, busyLock, maxOpenCursors))
                            .logger(log)
                            .selectorCount(DFLT_SELECTOR_CNT)
                            .igniteInstanceName(ctx.igniteInstanceName())
                            .serverName("sql-listener")
                            .tcpNoDelay(sqlCfg.isTcpNoDelay())
                            .directBuffer(DFLT_TCP_DIRECT_BUF)
                            .byteOrder(ByteOrder.nativeOrder())
                            .socketSendBufferSize(sqlCfg.getSocketSendBufferSize())
                            .socketReceiveBufferSize(sqlCfg.getSocketReceiveBufferSize())
                            .filters(filters)
                            .directMode(false)
                            .idleTimeout(Long.MAX_VALUE)
                            .build();

                        srv0.start();

                        srv = srv0;

                        ctx.ports().registerPort(port, IgnitePortProtocol.TCP, getClass());

                        log.info("SQL connector processor has started on TCP port " + port);

                        lastErr = null;

                        break;
                    }
                    catch (Exception e) {
                        lastErr = e;
                    }
                }

                assert (srv != null && lastErr == null) || (srv == null && lastErr != null);

                if (lastErr != null)
                    throw new IgniteCheckedException("Failed to bind to any [host:port] from the range [" +
                        "host=" + host + ", portFrom=" + sqlCfg.getPort() + ", portTo=" + portTo +
                        ", lastErr=" + lastErr + ']');
            }
            catch (Exception e) {
                throw new IgniteCheckedException("Failed to start SQL connector processor.", e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        if (srv != null) {
            busyLock.block();

            srv.stop();

            ctx.ports().deregisterPorts(getClass());

            if (execSvc != null) {
                U.shutdownNow(getClass(), execSvc, log);

                execSvc = null;
            }

            if (log.isDebugEnabled())
                log.debug("SQL connector processor stopped.");
        }
    }

    /**
     * Prepare connector configuration.
     *
     * @param cfg Ignote configuration.
     * @return Connector configuration.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("deprecation")
    @Nullable private SqlConnectorConfiguration prepareConfiguration(IgniteConfiguration cfg)
        throws IgniteCheckedException {
        SqlConnectorConfiguration res = cfg.getSqlConnectorConfiguration();

        OdbcConfiguration odbcCfg = cfg.getOdbcConfiguration();

        if (odbcCfg != null) {
            if (res == null) {
                // SQL connector is either default or null, so we replace it with ODBC stuff.
                HostAndPortRange hostAndPort = parseOdbcEndpoint(odbcCfg);

                res = new SqlConnectorConfiguration();

                res.setHost(hostAndPort.host());
                res.setPort(hostAndPort.portFrom());
                res.setPortRange(hostAndPort.portTo() - hostAndPort.portFrom());
                res.setThreadPoolSize(odbcCfg.getThreadPoolSize());
                res.setSocketSendBufferSize(odbcCfg.getSocketSendBufferSize());
                res.setSocketReceiveBufferSize(odbcCfg.getSocketReceiveBufferSize());
                res.setMaxOpenCursorsPerConnection(odbcCfg.getMaxOpenCursors());

                U.warn(log, "Automatically converted deprecated ODBC configuration to SQL connector configuration: " +
                    res);
            }
            else {
                // Non-default SQL connector is set, ignore ODBC.
                U.warn(log, "Deprecated ODBC configuration will be ignored because SQL connector configuration is " +
                    "set (either migrate to new SqlConnectorConfiguration or set " +
                    "IgniteConfiguration.sqlConnectorConfiguration to null explicitly).");
            }
        }

        return res;
    }

    /**
     * Validate SQL connector configuration.
     *
     * @param cfg Configuration.
     * @throws IgniteCheckedException If failed.
     */
    private void validateConfiguration(SqlConnectorConfiguration cfg) throws IgniteCheckedException {
        assertParameter(cfg.getPort() > 1024, "port > 1024");
        assertParameter(cfg.getPort() <= 65535, "port <= 65535");
        assertParameter(cfg.getPortRange() >= 0, "portRange > 0");
        assertParameter(cfg.getSocketSendBufferSize() >= 0, "socketSendBufferSize > 0");
        assertParameter(cfg.getSocketReceiveBufferSize() >= 0, "socketReceiveBufferSize > 0");
        assertParameter(cfg.getMaxOpenCursorsPerConnection() >= 0, "maxOpenCursorsPerConnection() >= 0");
        assertParameter(cfg.getThreadPoolSize() > 0, "threadPoolSize > 0");
    }

    /**
     * Parse ODBC endpoint.
     *
     * @param odbcCfg ODBC configuration.
     * @return ODBC host and port range.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("deprecation")
    private HostAndPortRange parseOdbcEndpoint(OdbcConfiguration odbcCfg) throws IgniteCheckedException {
        HostAndPortRange res;

        if (F.isEmpty(odbcCfg.getEndpointAddress())) {
            res = new HostAndPortRange(OdbcConfiguration.DFLT_TCP_HOST,
                OdbcConfiguration.DFLT_TCP_PORT_FROM,
                OdbcConfiguration.DFLT_TCP_PORT_TO
            );
        }
        else {
            res = HostAndPortRange.parse(odbcCfg.getEndpointAddress(),
                OdbcConfiguration.DFLT_TCP_PORT_FROM,
                OdbcConfiguration.DFLT_TCP_PORT_TO,
                "Failed to parse SQL connector endpoint address"
            );
        }

        return res;
    }
}
