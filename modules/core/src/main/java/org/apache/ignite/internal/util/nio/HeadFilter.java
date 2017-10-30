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

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Filter forwarding messages from chain's head to this server.
 */
class HeadFilter<T> extends GridNioFilterAdapter {
    /** */
    private GridNioServer<T> nio;

    /** */
    private final boolean direct;

    /** */
    private final boolean isSsl;

    /**
     * Assigns filter name.
     */
    HeadFilter(GridNioServer<T> nio, boolean direct, boolean isSsl) {
        super("HeadFilter");
        this.nio = nio;
        this.direct = direct;
        this.isSsl = isSsl;
    }

    /** {@inheritDoc} */
    @Override public void onSessionOpened(GridNioSession ses) throws IgniteCheckedException {
        if (direct && isSsl)
            ses.addMeta(GridNioServer.BUF_SSL_SYSTEM_META_KEY, new ConcurrentLinkedQueue<>());

        proceedSessionOpened(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionClosed(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionClosed(ses);
    }

    /** {@inheritDoc} */
    @Override public void onExceptionCaught(GridNioSession ses,
        IgniteCheckedException ex) throws IgniteCheckedException {
        proceedExceptionCaught(ses, ex);
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<?> onSessionWrite(GridNioSession ses,
        Object msg,
        boolean fut,
        IgniteInClosure<IgniteException> ackC) throws IgniteCheckedException {
        if (direct) {
            boolean sslSys = isSsl && msg instanceof ByteBuffer;

            if (sslSys) {
                ConcurrentLinkedQueue<ByteBuffer> queue = ses.meta(GridNioServer.BUF_SSL_SYSTEM_META_KEY);

                assert queue != null;

                queue.offer((ByteBuffer)msg);

                GridSelectorNioSessionImpl ses0 = (GridSelectorNioSessionImpl)ses;

                if (!ses0.procWrite.get() && ses0.procWrite.compareAndSet(false, true))
                    ses0.worker().registerWrite(ses0);

                return null;
            }
            else
                return nio.send(ses, (Message)msg, fut, ackC);
        }
        else
            return nio.send(ses, (ByteBuffer)msg, fut, ackC);
    }

    /** {@inheritDoc} */
    @Override public void onMessageReceived(GridNioSession ses, Object msg) throws IgniteCheckedException {
        proceedMessageReceived(ses, msg);
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<Boolean> onSessionClose(GridNioSession ses) {
        return nio.close(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionIdleTimeout(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionIdleTimeout(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionWriteTimeout(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionWriteTimeout(ses);
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<?> onPauseReads(GridNioSession ses) throws IgniteCheckedException {
        return nio.pauseResumeReads(ses, NioOperation.PAUSE_READ);
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<?> onResumeReads(GridNioSession ses) throws IgniteCheckedException {
        return nio.pauseResumeReads(ses, NioOperation.RESUME_READ);
    }
}
