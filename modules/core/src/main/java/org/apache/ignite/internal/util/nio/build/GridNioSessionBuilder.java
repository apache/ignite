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
import org.apache.ignite.internal.util.nio.GridNioRecoveryDescriptor;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.util.nio.GridNioSessionMetaKey.SSL_META;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.CONN_IDX_META;

/**
 * 3-step node communication handshake:
 * 1) receive NodeIdMessage
 * 2) write header IGNITE_HEADER and HandshakeMessage
 * 3) receive RecoveryLastReceivedMessage
 */
public class GridNioSessionBuilder extends AbstractGridNioConnectionBuilder<GridNioSession> {
    /**
     * @param log
     */
    public GridNioSessionBuilder(IgniteLogger log) {
        super(log);
    }

    /** {@inheritDoc} */
    @Override public GridNioSession build(
        GridNioConnectionBuilderContext ctx,
        InetSocketAddress addr,
        GridNioHandshakeCompletionHandler hndlr
    ) throws Exception {
        assert recoveryDesc != null;

        GridNioSession session = null;

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

        SocketChannel ch = initSocketChannel(ctx, addr, hndlr);

        meta.put(CONN_IDX_META, connKey);
        meta.put(GridNioServer.RECOVERY_DESC_META_KEY, recoveryDesc);

        try {
            session = ctx.nioSrvr().createSession(ch, meta, false, null).get();

            return session;
        }
        finally {
            if (session == null)
                U.closeQuiet(ch);
        }
    }
}
