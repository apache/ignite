/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.util.nio;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.lang.IgniteInClosure;

/**
 * Filter chain implementation for nio server filters.
 */
public class GridNioFilterChain<T> extends GridNioFilterAdapter {
    /** Grid logger. */
    private IgniteLogger log;

    /** Listener that will be used to notify on server events. */
    private GridNioServerListener<T> lsnr;

    /** Head of filter list. */
    private GridNioFilter head;

    /** Tail of filter list. */
    private GridNioFilter tail;

    /** Cached value for toString method. */
    private volatile String str;

    /**
     * Constructor.
     *
     * @param log Logger instance.
     * @param lsnr Listener for events passed through chain.
     * @param head First filter in chain, it expected to be connected to actual endpoint.
     * @param filters Filters applied between listener and head. Will be inserted in the same order,
     *      so chain will look like (lsnr) -> (filters[0]) -> ... -> (filters[n]) -> (head).
     */
    public GridNioFilterChain(IgniteLogger log, GridNioServerListener<T> lsnr, GridNioFilter head,
        GridNioFilter... filters) {
        super("FilterChain");

        this.log = log;
        this.lsnr = lsnr;
        this.head = head;

        GridNioFilter prev;

        tail = prev = new TailFilter();

        for (GridNioFilter filter : filters) {
            prev.nextFilter(filter);

            filter.previousFilter(prev);

            prev = filter;
        }

        prev.nextFilter(head);

        head.previousFilter(prev);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        if (str == null) {
            StringBuilder res = new StringBuilder("FilterChain[filters=[");

            GridNioFilter ref = tail.nextFilter();

            while (ref != head) {
                res.append(ref);

                ref = ref.nextFilter();

                if (ref != head)
                    res.append(", ");
            }

            res.append(']');

            // It is OK if this variable will be rewritten concurrently.
            str = res.toString();
        }

        return str;
    }

    /**
     * Starts all filters in order from application layer to the network layer.
     */
    @Override public void start() {
        GridNioFilter ref = tail.nextFilter();

        // Walk through the linked list and start all the filters.
        while (ref != head) {
            ref.start();

            ref = ref.nextFilter();
        }
    }

    /**
     * Stops all filters in order from network layer to the application layer.
     */
    @Override public void stop() {
        GridNioFilter ref = head.previousFilter();

        // Walk through the linked list and stop all the filters.
        while (ref != tail) {
            ref.stop();

            ref = ref.previousFilter();
        }
    }

    /**
     * Starts chain notification from head to tail.
     *
     * @param ses Session that was created.
     * @throws IgniteCheckedException If IgniteCheckedException occurred while handling event.
     */
    @Override public void onSessionOpened(GridNioSession ses) throws IgniteCheckedException {
        head.onSessionOpened(ses);
    }

    /**
     * Starts chain notification from head to tail.
     *
     * @param ses Session that was closed.
     * @throws IgniteCheckedException If IgniteCheckedException occurred while handling event.
     */
    @Override public void onSessionClosed(GridNioSession ses) throws IgniteCheckedException {
        head.onSessionClosed(ses);
    }

    /**
     * Starts chain notification from head to tail.
     *
     * @param ses Session in which GridNioException was caught.
     * @param e IgniteCheckedException instance.
     */
    @Override public void onExceptionCaught(GridNioSession ses, IgniteCheckedException e) {
        try {
            head.onExceptionCaught(ses, e);
        }
        catch (Exception ex) {
            LT.error(log, ex, "Failed to forward GridNioException to filter chain [ses=" + ses + ", e=" + e + ']');
        }
    }

    /**
     * Starts chain notification from head to tail.
     *
     * @param ses Session in which message was received.
     * @param msg Received message.
     * @throws IgniteCheckedException If IgniteCheckedException occurred while handling event.
     */
    @Override public void onMessageReceived(GridNioSession ses, Object msg) throws IgniteCheckedException {
        head.onMessageReceived(ses, msg);
    }

    /**
     * Starts chain notification from tail to head.
     *
     * @param ses Session to which message should be written.
     * @param msg Message to write.
     * @return Send future.
     * @throws IgniteCheckedException If IgniteCheckedException occurred while handling event.
     */
    @Override public GridNioFuture<?> onSessionWrite(
        GridNioSession ses,
        Object msg,
        boolean fut,
        IgniteInClosure<IgniteException> ackC
    ) throws IgniteCheckedException {
        return tail.onSessionWrite(ses, msg, fut, ackC);
    }

    /**
     * Starts chain notification from tail to head.
     *
     * @param ses Session to close.
     * @return Close future.
     * @throws IgniteCheckedException If IgniteCheckedException occurred while handling event.
     */
    @Override public GridNioFuture<Boolean> onSessionClose(GridNioSession ses) throws IgniteCheckedException {
        return tail.onSessionClose(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionIdleTimeout(GridNioSession ses) throws IgniteCheckedException {
        head.onSessionIdleTimeout(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionWriteTimeout(GridNioSession ses) throws IgniteCheckedException {
        head.onSessionWriteTimeout(ses);
    }

    /**
     * Starts chain notification from tail to head.
     *
     * @param ses Session to pause reads.
     * @return Future.
     * @throws IgniteCheckedException If IgniteCheckedException occurred while handling event.
     */
    @Override public GridNioFuture<?> onPauseReads(GridNioSession ses) throws IgniteCheckedException {
        return tail.onPauseReads(ses);
    }

    /**
     * Starts chain notification from tail to head.
     *
     * @param ses Session to resume reads.
     * @return Future.
     * @throws IgniteCheckedException If IgniteCheckedException occurred while handling event.
     */
    @Override public GridNioFuture<?> onResumeReads(GridNioSession ses) throws IgniteCheckedException {
        return tail.onResumeReads(ses);
    }

    /**
     * Tail filter that handles all incoming events.
     */
    private class TailFilter extends GridNioFilterAdapter {
        /**
         * Constructs tail filter.
         */
        private TailFilter() {
            super("TailFilter");
        }

        /** {@inheritDoc} */
        @Override public void onSessionOpened(GridNioSession ses) {
            lsnr.onConnected(ses);
        }

        /** {@inheritDoc} */
        @Override public void onSessionClosed(GridNioSession ses) {
            lsnr.onDisconnected(ses, null);
        }

        /** {@inheritDoc} */
        @Override public void onExceptionCaught(GridNioSession ses, IgniteCheckedException ex) {
            lsnr.onDisconnected(ses, ex);
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<?> onSessionWrite(GridNioSession ses,
            Object msg,
            boolean fut,
            IgniteInClosure<IgniteException> ackC) throws IgniteCheckedException {
            return proceedSessionWrite(ses, msg, fut, ackC);
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<Boolean> onSessionClose(GridNioSession ses) throws IgniteCheckedException {
            return proceedSessionClose(ses);
        }

        /** {@inheritDoc} */
        @Override public void onMessageReceived(GridNioSession ses, Object msg) {
            lsnr.onMessage(ses, (T)msg);
        }

        /** {@inheritDoc} */
        @Override public void onSessionIdleTimeout(GridNioSession ses) throws IgniteCheckedException {
            lsnr.onSessionIdleTimeout(ses);
        }

        /** {@inheritDoc} */
        @Override public void onSessionWriteTimeout(GridNioSession ses) throws IgniteCheckedException {
            lsnr.onSessionWriteTimeout(ses);
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<?> onPauseReads(GridNioSession ses) throws IgniteCheckedException {
            return proceedPauseReads(ses);
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<?> onResumeReads(GridNioSession ses) throws IgniteCheckedException {
            return proceedResumeReads(ses);
        }
    }
}
