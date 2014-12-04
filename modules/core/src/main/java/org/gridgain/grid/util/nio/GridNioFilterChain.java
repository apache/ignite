/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.nio;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;

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
    public String toString() {
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
     * @throws GridException If GridException occurred while handling event.
     */
    @Override public void onSessionOpened(GridNioSession ses) throws GridException {
        head.onSessionOpened(ses);
    }

    /**
     * Starts chain notification from head to tail.
     *
     * @param ses Session that was closed.
     * @throws GridException If GridException occurred while handling event.
     */
    @Override public void onSessionClosed(GridNioSession ses) throws GridException {
        head.onSessionClosed(ses);
    }

    /**
     * Starts chain notification from head to tail.
     *
     * @param ses Session in which GridNioException was caught.
     * @param e GridException instance.
     */
    @Override public void onExceptionCaught(GridNioSession ses, GridException e) {
        try {
            head.onExceptionCaught(ses, e);
        }
        catch (Exception ex) {
            LT.warn(log, ex, "Failed to forward GridNioException to filter chain [ses=" + ses + ", e=" + e + ']');
        }
    }

    /**
     * Starts chain notification from head to tail.
     *
     * @param ses Session in which message was received.
     * @param msg Received message.
     * @throws GridException If GridException occurred while handling event.
     */
    @Override public void onMessageReceived(GridNioSession ses, Object msg) throws GridException {
        head.onMessageReceived(ses, msg);
    }

    /**
     * Starts chain notification from tail to head.
     *
     * @param ses Session to which message should be written.
     * @param msg Message to write.
     * @return Send future.
     * @throws GridException If GridException occurred while handling event.
     */
    @Override public GridNioFuture<?> onSessionWrite(GridNioSession ses, Object msg) throws GridException {
        return tail.onSessionWrite(ses, msg);
    }

    /**
     * Starts chain notification from tail to head.
     *
     * @param ses Session to close.
     * @return Close future.
     * @throws GridException If GridException occurred while handling event.
     */
    @Override public GridNioFuture<Boolean> onSessionClose(GridNioSession ses) throws GridException {
        return tail.onSessionClose(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionIdleTimeout(GridNioSession ses) throws GridException {
        head.onSessionIdleTimeout(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionWriteTimeout(GridNioSession ses) throws GridException {
        head.onSessionWriteTimeout(ses);
    }

    /**
     * Starts chain notification from tail to head.
     *
     * @param ses Session to pause reads.
     * @return Future.
     * @throws GridException If GridException occurred while handling event.
     */
    @Override public GridNioFuture<?> onPauseReads(GridNioSession ses) throws GridException {
        return tail.onPauseReads(ses);
    }

    /**
     * Starts chain notification from tail to head.
     *
     * @param ses Session to resume reads.
     * @return Future.
     * @throws GridException If GridException occurred while handling event.
     */
    @Override public GridNioFuture<?> onResumeReads(GridNioSession ses) throws GridException {
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
        @Override public void onExceptionCaught(GridNioSession ses, GridException ex) {
            lsnr.onDisconnected(ses, ex);
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<?> onSessionWrite(GridNioSession ses, Object msg)
            throws GridException {
            return proceedSessionWrite(ses, msg);
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<Boolean> onSessionClose(GridNioSession ses) throws GridException {
            return proceedSessionClose(ses);
        }

        /** {@inheritDoc} */
        @Override public void onMessageReceived(GridNioSession ses, Object msg) {
            lsnr.onMessage(ses, (T)msg);
        }

        /** {@inheritDoc} */
        @Override public void onSessionIdleTimeout(GridNioSession ses) throws GridException {
            lsnr.onSessionIdleTimeout(ses);
        }

        /** {@inheritDoc} */
        @Override public void onSessionWriteTimeout(GridNioSession ses) throws GridException {
            lsnr.onSessionWriteTimeout(ses);
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<?> onPauseReads(GridNioSession ses) throws GridException {
            return proceedPauseReads(ses);
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<?> onResumeReads(GridNioSession ses) throws GridException {
            return proceedResumeReads(ses);
        }
    }
}
