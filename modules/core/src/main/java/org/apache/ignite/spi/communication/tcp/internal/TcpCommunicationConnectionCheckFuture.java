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

package org.apache.ignite.spi.communication.tcp.internal;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;

/**
 *
 */
public class TcpCommunicationConnectionCheckFuture extends GridFutureAdapter<Boolean> {
    /** */
    private final UUID nodeId;

    /** */
    private final GridNioServer nioSrvr;

    /** */
    private Map<Integer, Object> sesMeta;

    /** */
    private SocketChannel ch;

    /**
     * @param nodeId Remote note ID.
     */
    public TcpCommunicationConnectionCheckFuture(GridNioServer nioSrvr, UUID nodeId) {
        this.nioSrvr = nioSrvr;
        this.nodeId = nodeId;
    }

    /**
     * @param addr
     * @throws IOException
     */
    public void init(InetSocketAddress addr) throws IOException {
        ch = SocketChannel.open();

        ch.configureBlocking(false);

        ch.socket().setTcpNoDelay(true);
        ch.socket().setKeepAlive(false);

        boolean connect = ch.connect(addr);

        if (!connect) {
            sesMeta = new GridLeanMap<>(2);

            sesMeta.put(TcpCommunicationSpi.CONN_IDX_META, TcpCommunicationSpi.CONN_CHECK_DUMMY_KEY);
            sesMeta.put(TcpCommunicationSpi.SES_FUT_META, this);

            nioSrvr.createSession(ch, sesMeta, true, new IgniteInClosure<IgniteInternalFuture<GridNioSession>>() {
                @Override public void apply(IgniteInternalFuture<GridNioSession> fut) {
                    if (fut.error() != null)
                        onDone(false);
                }
            });
        }
    }

    /**
     *
     */
    public void onTimeout() {
        if (super.onDone(false))
            nioSrvr.cancelConnect(ch, sesMeta);
    }

    /**
     * @param rmtNodeId
     */
    public void onConnected(UUID rmtNodeId) {
        onDone(nodeId.equals(rmtNodeId));
    }
}
