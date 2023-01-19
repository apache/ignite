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

package org.apache.ignite.internal.client.thin.io.gridnioserver;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.client.thin.io.ClientConnection;
import org.apache.ignite.internal.client.thin.io.ClientConnectionStateHandler;
import org.apache.ignite.internal.client.thin.io.ClientMessageHandler;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridNioSessionMetaKey;
import org.jetbrains.annotations.Nullable;

/**
 * Client connection.
 */
class GridNioClientConnection implements ClientConnection {
    /** */
    static final int SES_META_CONN = GridNioSessionMetaKey.nextUniqueKey();

    /** */
    private final GridNioSession ses;

    /** */
    private final ClientMessageHandler msgHnd;

    /** */
    private final ClientConnectionStateHandler stateHnd;

    /**
     * Ctor.
     *
     * @param ses Session.
     */
    public GridNioClientConnection(GridNioSession ses,
                                   ClientMessageHandler msgHnd,
                                   ClientConnectionStateHandler stateHnd) {
        assert ses != null;
        assert msgHnd != null;
        assert stateHnd != null;

        this.ses = ses;
        this.msgHnd = msgHnd;
        this.stateHnd = stateHnd;

        ses.addMeta(SES_META_CONN, this);
    }

    /** {@inheritDoc} */
    @Override public void send(ByteBuffer msg, @Nullable Runnable onDone) throws IgniteCheckedException {
        if (onDone != null)
            ses.send(msg).listen(f -> onDone.run());
        else
            ses.sendNoFuture(msg, null);
    }

    /** {@inheritDoc} */
    @Override public InetSocketAddress getAddress() {
        return ses.remoteAddress();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        ses.close();
    }

    /**
     * Handles incoming message.
     *
     * @param msg Message.
     */
    void onMessage(ByteBuffer msg) {
        assert msg != null;

        msgHnd.onMessage(msg);
    }

    /**
     * Handles disconnect.
     *
     * @param e Exception that caused the disconnect.
     */
    void onDisconnected(Exception e) {
        stateHnd.onDisconnected(e);
    }
}
