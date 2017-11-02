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

package org.apache.ignite.internal.util.nio;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Connection operation
 */
final class ConnectionOperationFuture extends NioOperationFuture<GridNioSession> {
    /** */
    private final SocketChannel sockCh;
    /** */
    private final boolean accepted;
    /** */
    private final Map<Integer, ?> meta;

    /**
     * @param sockCh Socket channel.
     * @param accepted Was the channel accepted or not, true for incoming connection requests, false otherwise.
     * @param meta Mata information.
     * @param op Nio operation.
     */
    ConnectionOperationFuture(SocketChannel sockCh, boolean accepted, Map<Integer, ?> meta, NioOperation op) {
        super(null, op);
        this.sockCh = sockCh;
        this.accepted = accepted;
        this.meta = meta;
    }

    /**
     * @return Socket channel for register request.
     */
    SocketChannel socketChannel() {
        return sockCh;
    }

    /**
     * @return {@code True} if connection has been accepted.
     */
    boolean accepted() {
        return accepted;
    }

    /**
     * @return Meta.
     */
    public Map<Integer, ?> meta() {
        return meta;
    }

    /** {@inheritDoc} */
    @Override public <T> void invoke(GridNioServer<T> nio, GridNioWorker worker) {
        switch (op) {
            case CONNECT: {
                SocketChannel ch = socketChannel();

                try {
                    ch.register(worker.selector(), SelectionKey.OP_CONNECT, this);
                }
                catch (IOException e) {
                    onDone(new IgniteCheckedException("Failed to register channel on selector", e));
                }

                break;
            }

            case CANCEL_CONNECT: {
                SocketChannel ch = socketChannel();

                SelectionKey key = ch.keyFor(worker.selector());

                if (key != null)
                    key.cancel();

                U.closeQuiet(ch);

                onDone();

                break;
            }

            case REGISTER: {
                worker.register(this);

                break;
            }

            default:
                throw new UnsupportedOperationException();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ConnectionOperationFuture.class, this, super.toString());
    }
}
