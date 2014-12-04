/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.nio;

import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.worker.*;

import java.util.concurrent.*;

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
    @Override public void onSessionOpened(final GridNioSession ses) throws GridException {
        workerPool.execute(new GridWorker(gridName, "session-opened-notify", log) {
            @Override protected void body() {
                try {
                    proceedSessionOpened(ses);
                }
                catch (GridException e) {
                    handleException(ses, e);
                }
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void onSessionClosed(final GridNioSession ses) throws GridException {
        workerPool.execute(new GridWorker(gridName, "session-closed-notify", log) {
            @Override protected void body() {
                try {
                    proceedSessionClosed(ses);
                }
                catch (GridException e) {
                    handleException(ses, e);
                }
            }
        });

    }

    /** {@inheritDoc} */
    @Override public void onMessageReceived(final GridNioSession ses, final Object msg) throws GridException {
        workerPool.execute(new GridWorker(gridName, "message-received-notify", log) {
            @Override protected void body() {
                try {
                    proceedMessageReceived(ses, msg);
                }
                catch (GridException e) {
                    handleException(ses, e);
                }
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void onExceptionCaught(GridNioSession ses, GridException ex) throws GridException {
        proceedExceptionCaught(ses, ex);
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<?> onSessionWrite(GridNioSession ses, Object msg) throws GridException {
        return proceedSessionWrite(ses, msg);
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<Boolean> onSessionClose(GridNioSession ses) throws GridException {
        return proceedSessionClose(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionIdleTimeout(GridNioSession ses) throws GridException {
        proceedSessionIdleTimeout(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionWriteTimeout(GridNioSession ses) throws GridException {
        proceedSessionWriteTimeout(ses);
    }

    /**
     * @param ses Session.
     * @param ex Exception.
     */
    private void handleException(GridNioSession ses, GridException ex) {
        try {
            proceedExceptionCaught(ses, ex);
        }
        catch (GridException e) {
            U.warn(log, "Failed to forward exception to the underlying filter (will ignore) [ses=" + ses + ", " +
                "originalEx=" + ex + ", ex=" + e + ']');
        }
    }
}
