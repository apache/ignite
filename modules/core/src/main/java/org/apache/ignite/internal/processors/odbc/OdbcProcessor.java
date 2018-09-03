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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.OdbcConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
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
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.spi.IgnitePortProtocol;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;

import java.net.InetAddress;
import java.nio.ByteOrder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * ODBC processor.
 */
public class OdbcProcessor extends GridProcessorAdapter {
    /** Default number of selectors. */
    private static final int DFLT_SELECTOR_CNT = Math.min(4, Runtime.getRuntime().availableProcessors());

    /** Default TCP_NODELAY flag. */
    private static final boolean DFLT_TCP_NODELAY = true;

    /** Default TCP direct buffer flag. */
    private static final boolean DFLT_TCP_DIRECT_BUF = false;

    /** Busy lock. */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** ODBC TCP Server. */
    private GridNioServer<byte[]> srv;

    /** ODBC executor service. */
    private ExecutorService odbcExecSvc;

    /**
     * @param ctx Kernal context.
     */
    public OdbcProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        IgniteConfiguration cfg = ctx.config();

        OdbcConfiguration odbcCfg = cfg.getOdbcConfiguration();

        if (odbcCfg != null) {
            try {
                Marshaller marsh = cfg.getMarshaller();

                if (marsh != null && !(marsh instanceof BinaryMarshaller))
                    throw new IgniteCheckedException("ODBC can only be used with BinaryMarshaller (please set it " +
                        "through IgniteConfiguration.setMarshaller())");

                HostAndPortRange hostPort;

                if (F.isEmpty(odbcCfg.getEndpointAddress())) {
                    hostPort = new HostAndPortRange(OdbcConfiguration.DFLT_TCP_HOST,
                        OdbcConfiguration.DFLT_TCP_PORT_FROM,
                        OdbcConfiguration.DFLT_TCP_PORT_TO
                    );
                }
                else {
                    hostPort = HostAndPortRange.parse(odbcCfg.getEndpointAddress(),
                        OdbcConfiguration.DFLT_TCP_PORT_FROM,
                        OdbcConfiguration.DFLT_TCP_PORT_TO,
                        "Failed to parse ODBC endpoint address"
                    );
                }

                assertParameter(odbcCfg.getThreadPoolSize() > 0, "threadPoolSize > 0");

                odbcExecSvc = new IgniteThreadPoolExecutor(
                    "odbc",
                    cfg.getGridName(),
                    odbcCfg.getThreadPoolSize(),
                    odbcCfg.getThreadPoolSize(),
                    0,
                    new LinkedBlockingQueue<Runnable>());

                InetAddress host;

                try {
                    host = InetAddress.getByName(hostPort.host());
                }
                catch (Exception e) {
                    throw new IgniteCheckedException("Failed to resolve ODBC host: " + hostPort.host(), e);
                }

                Exception lastErr = null;

                for (int port = hostPort.portFrom(); port <= hostPort.portTo(); port++) {
                    try {
                        GridNioFilter[] filters = new GridNioFilter[] {
                            new GridNioAsyncNotifyFilter(ctx.gridName(), odbcExecSvc, log) {
                                @Override public void onSessionOpened(GridNioSession ses) throws IgniteCheckedException {
                                    proceedSessionOpened(ses);
                                }
                            },
                            new GridNioCodecFilter(new OdbcBufferedParser(), log, false)
                        };

                        GridNioServer<byte[]> srv0 = GridNioServer.<byte[]>builder()
                            .address(host)
                            .port(port)
                            .listener(new OdbcNioListener(ctx, busyLock, odbcCfg.getMaxOpenCursors()))
                            .logger(log)
                            .selectorCount(DFLT_SELECTOR_CNT)
                            .gridName(ctx.gridName())
                            .serverName("odbc")
                            .tcpNoDelay(DFLT_TCP_NODELAY)
                            .directBuffer(DFLT_TCP_DIRECT_BUF)
                            .byteOrder(ByteOrder.nativeOrder())
                            .socketSendBufferSize(odbcCfg.getSocketSendBufferSize())
                            .socketReceiveBufferSize(odbcCfg.getSocketReceiveBufferSize())
                            .filters(filters)
                            .directMode(false)
                            .build();

                        srv0.start();

                        srv = srv0;

                        ctx.ports().registerPort(port, IgnitePortProtocol.TCP, getClass());

                        log.info("ODBC processor has started on TCP port " + port);

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
                        "address=" + hostPort + ", lastErr=" + lastErr + ']');
            }
            catch (Exception e) {
                throw new IgniteCheckedException("Failed to start ODBC processor.", e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        if (srv != null) {
            busyLock.block();

            srv.stop();

            ctx.ports().deregisterPorts(getClass());

            if (odbcExecSvc != null) {
                U.shutdownNow(getClass(), odbcExecSvc, log);

                odbcExecSvc = null;
            }

            if (log.isDebugEnabled())
                log.debug("ODBC processor stopped.");
        }
    }
}
