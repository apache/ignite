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

import java.util.concurrent.Executor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.util.worker.GridWorkerPool;

/**
 * Enables multithreaded notification of session opened, message received and session closed events.
 */
public class GridNioAsyncNotifyFilter extends GridNioFilterAdapter {
    /** Logger. */
    private IgniteLogger log;

    /** Worker pool. */
    private GridWorkerPool workerPool;

    /** Grid name. */
    private String gridName;

    /**
     * Assigns filter name to a filter.
     *
     * @param gridName Grid name.
     * @param exec Executor.
     * @param log Logger.
     */
    public GridNioAsyncNotifyFilter(String gridName, Executor exec, IgniteLogger log) {
        super(GridNioAsyncNotifyFilter.class.getSimpleName());

        this.gridName = gridName;
        this.log = log;

        workerPool = new GridWorkerPool(exec, log);
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        workerPool.join(false);
    }

    /** {@inheritDoc} */
    @Override public void onSessionOpened(final GridNioSession ses) throws IgniteCheckedException {
        workerPool.execute(new GridWorker(gridName, "session-opened-notify", log) {
            @Override protected void body() {
                try {
                    proceedSessionOpened(ses);
                }
                catch (IgniteCheckedException e) {
                    handleException(ses, e);
                }
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void onSessionClosed(final GridNioSession ses) throws IgniteCheckedException {
        workerPool.execute(new GridWorker(gridName, "session-closed-notify", log) {
            @Override protected void body() {
                try {
                    proceedSessionClosed(ses);
                }
                catch (IgniteCheckedException e) {
                    handleException(ses, e);
                }
            }
        });

    }

    /** {@inheritDoc} */
    @Override public void onMessageReceived(final GridNioSession ses, final Object msg) throws IgniteCheckedException {
        workerPool.execute(new GridWorker(gridName, "message-received-notify", log) {
            @Override protected void body() {
                try {
                    proceedMessageReceived(ses, msg);
                }
                catch (IgniteCheckedException e) {
                    handleException(ses, e);
                }
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void onExceptionCaught(GridNioSession ses, IgniteCheckedException ex) throws IgniteCheckedException {
        proceedExceptionCaught(ses, ex);
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<?> onSessionWrite(
        GridNioSession ses,
        Object msg,
        boolean fut
    ) throws IgniteCheckedException {
        return proceedSessionWrite(ses, msg, fut);
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<Boolean> onSessionClose(GridNioSession ses) throws IgniteCheckedException {
        return proceedSessionClose(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionIdleTimeout(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionIdleTimeout(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionWriteTimeout(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionWriteTimeout(ses);
    }

    /**
     * @param ses Session.
     * @param ex Exception.
     */
    private void handleException(GridNioSession ses, IgniteCheckedException ex) {
        try {
            proceedExceptionCaught(ses, ex);
        }
        catch (IgniteCheckedException e) {
            U.warn(log, "Failed to forward exception to the underlying filter (will ignore) [ses=" + ses + ", " +
                "originalEx=" + ex + ", ex=" + e + ']');
        }
    }
}
