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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.client.thin.io.ClientConnection;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridNioSessionMetaKey;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

class GridNioServerClientConnection implements ClientConnection {
    /** */
    static final int SES_META_CONN = GridNioSessionMetaKey.nextUniqueKey();

    /** */
    private final GridNioSession ses;

    /** */
    private Consumer<ByteBuffer> messageHandler;

    public GridNioServerClientConnection(GridNioSession ses) {
        assert ses != null;

        this.ses = ses;
        ses.addMeta(SES_META_CONN, this);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> sendAsync(ByteBuffer msg) {
        CompletableFuture<Void> res = new CompletableFuture<>();

        try {
            ses.sendNoFuture(msg, e -> {
                if (e == null)
                    res.complete(null);
                else
                    res.completeExceptionally(e);
            });
        } catch (IgniteCheckedException e) {
            res.completeExceptionally(e);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public void setMessageHandler(Consumer<ByteBuffer> handler) {
        this.messageHandler = handler;
    }

    /** {@inheritDoc} */
    @Override public void close() {

    }

    /**
     * Handle incoming message.
     *
     * @param msg Message.
     */
    void onMessage(ByteBuffer msg) {
        Consumer<ByteBuffer> handler0 = messageHandler;

        if (handler0 != null) {
            handler0.accept(msg);
        }
    }
}
