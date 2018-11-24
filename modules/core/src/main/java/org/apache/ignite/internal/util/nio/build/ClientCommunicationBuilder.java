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

package org.apache.ignite.internal.util.nio.build;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.nio.GridNioRecoveryDescriptor;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridTcpNioCommunicationClient;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutHelper;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionKey;

import static org.apache.ignite.internal.util.nio.GridNioSessionMetaKey.SSL_META;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.CONN_IDX_META;

/**
 * 3-step node communication handshake:
 * 1) receive NodeIdMessage
 * 2) write header IGNITE_HEADER and HandshakeMessage
 * 3) receive RecoveryLastReceivedMessage
 */
public class ClientCommunicationBuilder implements CommunicationBuilder<GridCommunicationClient> {
    /** */
    private IgniteLogger log;

    /** */
    private CommunicationBuilder<SocketChannel> socketChannelBuilder;
    /** */
    private GridNioRecoveryDescriptor recoveryDesc;
    /** */
    private IgniteSpiOperationTimeoutHelper timeoutHelper;
    /** */
    private GridNioServer<Message> nioSrvr;
    /** */
    private ConnectionKey connKey;

    /**
     * @param log
     */
    public ClientCommunicationBuilder(IgniteLogger log) {
        this.log = log;
    }

    /** */
    @Override public GridCommunicationClient build(CommunicationBuilderContext ctx,
        InetSocketAddress addr) throws Exception {
        return build(ctx, addr, null);
    }

    /** */
    @Override public GridCommunicationClient build(
        CommunicationBuilderContext ctx,
        InetSocketAddress addr,
        CompletionHandler hndlr
    ) throws Exception {
        GridCommunicationClient client = null;

        if (!recoveryDesc.reserve()) {
            // Ensure the session is closed.
            GridNioSession ses = recoveryDesc.session();

            if (ses != null) {
                while (ses.closeTime() == 0)
                    ses.close();
            }

            return null;
        }

        Map<Integer, Object> meta = new HashMap<>();

        if (ctx.sslMeta() != null)
            meta.put(SSL_META.ordinal(), ctx.sslMeta());

        SocketChannel ch = socketChannelBuilder.build(ctx, addr, recoveryDesc);

        meta.put(CONN_IDX_META, connKey);
        meta.put(GridNioServer.RECOVERY_DESC_META_KEY, recoveryDesc);

        try {
            GridNioSession ses = nioSrvr.createSession(ch, meta, false, null).get();

            client = new GridTcpNioCommunicationClient(connKey.connectionIndex(), ses, log);

            return client;
        }
        finally {
            if (client == null)
                U.closeQuiet(ch);
        }
    }

    /** */
    public ClientCommunicationBuilder setConnKey(ConnectionKey connKey) {
        this.connKey = connKey;

        return this;
    }

    /** */
    public ClientCommunicationBuilder setNioSrvr(GridNioServer<Message> nioSrvr) {
        this.nioSrvr = nioSrvr;

        return this;
    }

    /** */
    public ClientCommunicationBuilder setSock(CommunicationBuilder<SocketChannel> sock) {
        this.socketChannelBuilder = sock;

        return this;
    }

    /** */
    public ClientCommunicationBuilder setRecoveryDesc(GridNioRecoveryDescriptor recoveryDesc) {
        this.recoveryDesc = recoveryDesc;

        return this;
    }

    /** */
    public ClientCommunicationBuilder setTimeoutHelper(IgniteSpiOperationTimeoutHelper timeoutHelper) {
        this.timeoutHelper = timeoutHelper;

        return this;
    }
}
