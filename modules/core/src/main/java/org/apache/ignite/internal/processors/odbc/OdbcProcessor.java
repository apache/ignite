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
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.spi.IgnitePortProtocol;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * ODBC processor.
 */
public class OdbcProcessor extends GridProcessorAdapter {
    /** Default TCP_NODELAY flag. */
    private static final boolean DFLT_TCP_NODELAY = true;

    /** Default TCP direct buffer flag. */
    private static final boolean DFLT_TCP_DIRECT_BUF = false;

    /** Default ODBC idle timeout. */
    private static final int DFLT_IDLE_TIMEOUT = 7000;

    /** Default socket send and receive buffer size. */
    private static final int DFLT_SOCK_BUF_SIZE = 32 * 1024;

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
            validateConfiguration(odbcCfg);

            Marshaller marsh = ctx.config().getMarshaller();

            if (marsh != null && !(marsh instanceof BinaryMarshaller))
                throw new IgniteCheckedException("ODBC can only be used with BinaryMarshaller (please set it " +
                        "through IgniteConfiguration.setMarshaller())");

            String addrStr = odbcCfg.getAddress();

            if (addrStr == null) {
                addrStr = ctx.config().getLocalHost();

                if (addrStr == null)
                    addrStr = ""; // Using default host and port range.
            }

            Collection<InetSocketAddress> addrs = address(addrStr);

            for (InetSocketAddress addr : addrs) {
                try {
                    srv = GridNioServer.<byte[]>builder()
                            .address(addr.getAddress())
                            .port(addr.getPort())
                            .listener(new OdbcNioListener(ctx, busyLock))
                            .logger(log)
                            .selectorCount(Math.min(4, Runtime.getRuntime().availableProcessors()))
                            .gridName(ctx.gridName())
                            .tcpNoDelay(DFLT_TCP_NODELAY)
                            .directBuffer(DFLT_TCP_DIRECT_BUF)
                            .byteOrder(ByteOrder.nativeOrder())
                            .socketSendBufferSize(DFLT_SOCK_BUF_SIZE)
                            .socketReceiveBufferSize(DFLT_SOCK_BUF_SIZE)
                            .filters(new GridNioCodecFilter(new OdbcBufferedParser(), log, false))
                            .directMode(false)
                            .idleTimeout(DFLT_IDLE_TIMEOUT)
                            .build();

                    srv.start();

                    ctx.ports().registerPort(addr.getPort(), IgnitePortProtocol.TCP, getClass());

                    log.info("ODBC processor has started on " + addr);

                    break;
                }
                catch (Exception e) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to start ODBC processor on " + addr + ": " + e);

                    srv = null;
                }
            }

            if (srv == null)
                throw new IgniteCheckedException("Failed to start ODBC processor using provided address: " + addrStr);
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

    /**
     * Validate ODBC configuration
     *
     * @param cfg Configuration to validate.
     * @throws IgniteCheckedException if configuration is not valid.
     */
    private static void validateConfiguration(OdbcConfiguration cfg) throws IgniteCheckedException {
        int maxOpenCursors = cfg.getMaxOpenCursors();

        if (maxOpenCursors < 1)
            throw new IgniteCheckedException("Parameter maxOpenCursors of the OdbcConfiguration " +
                    "must be greater than zero: [maxOpenCursors=" + maxOpenCursors + ']');
    }

    /**
     * Creates address from string.
     *
     * @param ipStr Address string.
     * @return Socket addresses (may contain 1 or more addresses if provided string
     *      includes port range).
     * @throws IgniteCheckedException If failed.
     */
    private static Collection<InetSocketAddress> address(String ipStr) throws IgniteCheckedException {
        assert ipStr != null;

        ipStr = ipStr.trim();

        String errMsg = "Failed to parse provided ODBC address: " + ipStr;

        int colonCnt = ipStr.length() - ipStr.replace(":", "").length();

        if (colonCnt > 1) {
            // IPv6 address (literal IPv6 addresses are enclosed in square brackets, for example
            // https://[2001:db8:85a3:8d3:1319:8a2e:370:7348]:443).
            if (ipStr.startsWith("[")) {
                ipStr = ipStr.substring(1);

                if (ipStr.contains("]:"))
                    return addresses(ipStr, "\\]\\:", errMsg);
                else if (ipStr.endsWith("]"))
                    ipStr = ipStr.substring(0, ipStr.length() - 1);
                else
                    throw new IgniteCheckedException(errMsg);
            }
        }
        else {
            // IPv4 address.
            if (ipStr.endsWith(":"))
                ipStr = ipStr.substring(0, ipStr.length() - 1);
            else if (ipStr.indexOf(':') >= 0)
                return addresses(ipStr, "\\:", errMsg);
        }

        // Provided address does not contain port (will use default range).
        return address(ipStr + ':' + OdbcConfiguration.DFLT_TCP_PORT_RANGE);
    }

    /**
     * Creates address from string with port information.
     *
     * @param ipStr Address string
     * @param regexDelim Port regex delimiter.
     * @param errMsg Error message.
     * @return Socket addresses (may contain 1 or more addresses if provided string
     *      includes port range).
     * @throws IgniteCheckedException If failed.
     */
    private static Collection<InetSocketAddress> addresses(String ipStr, String regexDelim, String errMsg)
            throws IgniteCheckedException {
        String[] tokens = ipStr.split(regexDelim);

        if (tokens.length == 2) {
            String addrStr = tokens[0];
            String portStr = tokens[1];

            if (portStr.contains("..")) {
                try {
                    int port1 = Integer.parseInt(portStr.substring(0, portStr.indexOf("..")));
                    int port2 = Integer.parseInt(portStr.substring(portStr.indexOf("..") + 2, portStr.length()));

                    if (port2 < port1 || port1 == port2 || port1 <= 0 || port2 <= 0)
                        throw new IgniteCheckedException(errMsg);

                    Collection<InetSocketAddress> res = new ArrayList<>(port2 - port1);

                    // Upper bound included.
                    for (int i = port1; i <= port2; i++)
                        res.add(new InetSocketAddress(addrStr, i));

                    return res;
                }
                catch (IllegalArgumentException e) {
                    throw new IgniteCheckedException(errMsg, e);
                }
            }
            else {
                try {
                    int port = Integer.parseInt(portStr);

                    return Collections.singleton(new InetSocketAddress(addrStr, port));
                }
                catch (IllegalArgumentException e) {
                    throw new IgniteCheckedException(errMsg, e);
                }
            }
        }
        else
            throw new IgniteCheckedException(errMsg);
    }
}
