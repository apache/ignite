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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.nio.channel.GridNioSocketChannel;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * 3-step node communication handshake:
 * 1) receive NodeIdMessage
 * 2) write header IGNITE_HEADER and HandshakeMessage
 * 3) receive RecoveryLastReceivedMessage
 */
public class GridNioSocketChannelBuilder extends AbstractGridNioConnectionBuilder<GridNioSocketChannel> {
    /**
     * @param log
     */
    public GridNioSocketChannelBuilder(IgniteLogger log) {
        super(log);
    }

    /** {@inheritDoc} */
    @Override public GridNioSocketChannel build(
        GridNioConnectionBuilderContext ctx,
        InetSocketAddress addr,
        GridNioHandshakeCompletionHandler hndlr
    ) throws Exception {
        GridNioSocketChannel sockChnl = null;
        SocketChannel ch = null;

        try {
            ch = initSocketChannel(ctx, addr, hndlr);

            sockChnl = ctx.nioSrvr().createNioChannel(connKey, ch).get();

            return sockChnl;
        }
        finally {
            if (sockChnl == null)
                U.closeQuiet(ch);
        }
    }
}
