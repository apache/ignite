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
import org.apache.ignite.configuration.OdbcConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.nio.GridNioCodecFilter;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.spi.IgnitePortProtocol;

import java.net.InetAddress;
import java.nio.ByteOrder;

/**
 * ODBC processor.
 */
public class OdbcProcessor extends GridProcessorAdapter {
    /** Busy lock. */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** OBCD TCP Server. */
    private GridNioServer<byte[]> srv;

    /**
     * @param ctx Kernal context.
     */
    public OdbcProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        OdbcConfiguration odbcCfg = ctx.config().getOdbcConfiguration();

        if (odbcCfg != null) {
            try {
                Marshaller marsh = ctx.config().getMarshaller();

                if (marsh != null && !(marsh instanceof BinaryMarshaller))
                    throw new IgniteCheckedException("ODBC can only be used with BinaryMarshaller (please set it " +
                        "through IgniteConfiguration.setMarshaller())");

                String hostStr = odbcCfg.getHost();

                if (hostStr == null)
                    hostStr = ctx.config().getLocalHost();

                InetAddress host = U.resolveLocalHost(hostStr);

                int port = odbcCfg.getPort();

                srv = GridNioServer.<byte[]>builder()
                    .address(host)
                    .port(port)
                    .listener(new OdbcNioListener(ctx, busyLock))
                    .logger(log)
                    .selectorCount(odbcCfg.getSelectorCount())
                    .gridName(ctx.gridName())
                    .tcpNoDelay(odbcCfg.isNoDelay())
                    .directBuffer(odbcCfg.isDirectBuffer())
                    .byteOrder(ByteOrder.nativeOrder())
                    .socketSendBufferSize(odbcCfg.getSendBufferSize())
                    .socketReceiveBufferSize(odbcCfg.getReceiveBufferSize())
                    .sendQueueLimit(odbcCfg.getSendQueueLimit())
                    .filters(new GridNioCodecFilter(new OdbcBufferedParser(), log, false))
                    .directMode(false)
                    .idleTimeout(odbcCfg.getIdleTimeout())
                    .build();

                srv.start();

                ctx.ports().registerPort(port, IgnitePortProtocol.TCP, getClass());

                log.info("ODBC processor has started on TCP port " + port);
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

            if (log.isDebugEnabled())
                log.debug("ODBC processor stopped.");
        }
    }
}
