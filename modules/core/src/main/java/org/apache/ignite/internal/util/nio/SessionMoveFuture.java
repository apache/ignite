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
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
final class SessionMoveFuture extends NioOperationFuture<Boolean> {
    /** */
    private final int toIdx;

    /** */
    private SocketChannel channel;

    /**
     * @param ses Session.
     * @param toIdx Target worker index.
     */
    SessionMoveFuture(
        GridSelectorNioSessionImpl ses,
        int toIdx
    ) {
        super(ses, NioOperation.MOVE);

        this.toIdx = toIdx;
    }

    /**
     * @return Target worker index.
     */
    int toIndex() {
        return toIdx;
    }

    @Override
    public <T> void invoke(GridNioServer<T> nio, GridNioWorker worker) throws IOException {
        if (worker.sessions().remove(ses)) {
            assert channel == null : this;

            SelectionKey key = ses.key();
            SocketChannel channel = (SocketChannel)key.channel();

            assert channel != null : key;
            assert key.selector() == worker.selector();

            this.channel = channel;

            key.cancel();

            ses.moving(true);
            ses.reset();

            GridNioWorker toWorker = nio.workers().get(toIdx);

            assert toWorker != null;

            ses.worker(toWorker);

            toWorker.offer(this);
        }
        else if (worker.idx() == toIdx) {
            assert channel != null : this;

            ses.key(channel.register(worker.selector(), SelectionKey.OP_READ | SelectionKey.OP_WRITE, ses));

            worker.sessions().add(ses);

            if (toIdx % 2 == 0)
                nio.incrementReaderMovedCount();
            else
                nio.incrementWriterMovedCount();

            ses.moving(false);

            onDone(true);
        }
        else
            onDone(false);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SessionMoveFuture.class, this, super.toString());
    }
}
