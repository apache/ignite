/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.nio;

import org.gridgain.grid.*;

/**
 * This interface defines the general element in transformation chain between the nio server and
 * application.
 */
public interface GridNioFilter {
    /**
     * Beginning of a filter lifecycle, invoked on server start. It is guaranteed that this method will be invoked
     * prior to any of the event-related methods.
     */
    public void start();

    /**
     * End of a filter lifecycle, invoked on server stop. It is guaranteed that this method will be invoked after all
     * events are processed, no more event-related methods will be invoked after this method called.
     */
    public void stop();

    /**
     * Gets next filter in filter chain.
     *
     * @return Next filter (in order from application to network layer).
     */
    public GridNioFilter nextFilter();

    /**
     * Gets previous filter in filter chain.
     *
     * @return Previous filter (in order from application to network layer).
     */
    public GridNioFilter previousFilter();

    /**
     * Sets next filter in filter chain. In filter chain next filter would be more close
     * to the network layer.
     *
     * @param filter Next filter in filter chain.
     */
    public void nextFilter(GridNioFilter filter);

    /**
     * Sets previous filter in filter chain. In filter chain previous filter would be more close
     * to application layer.
     *
     * @param filter Previous filter in filter chain.
     */
    public void previousFilter(GridNioFilter filter);

    /**
     * Forwards session opened event to the next logical filter in filter chain.
     *
     * @param ses Opened session.
     * @throws GridException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    public void proceedSessionOpened(GridNioSession ses) throws GridException;

    /**
     * Forwards session closed event to the next logical filter in filter chain.
     *
     * @param ses Closed session.
     * @throws GridException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    public void proceedSessionClosed(GridNioSession ses) throws GridException;

    /**
     * Forwards GridNioException event to the next logical filter in filter chain.
     *
     * @param ses Session instance.
     * @param e GridNioException instance.
     * @throws GridException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    public void proceedExceptionCaught(GridNioSession ses, GridException e) throws GridException;

    /**
     * Forwards received message to the next logical filter in filter chain.
     *
     * @param ses Session instance.
     * @param msg Received message.
     * @throws GridException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    public void proceedMessageReceived(GridNioSession ses, Object msg) throws GridException;

    /**
     * Forwards write request to the next logical filter in filter chain.
     *
     * @param ses Session instance.
     * @param msg Message to send.
     * @return Write future.
     * @throws GridException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    public GridNioFuture<?> proceedSessionWrite(GridNioSession ses, Object msg) throws GridException;

    /**
     * Forwards session close request to the next logical filter in filter chain.
     *
     * @param ses Session instance.
     * @return Close future.
     * @throws GridException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    public GridNioFuture<Boolean> proceedSessionClose(GridNioSession ses) throws GridException;

    /**
     * Invoked when a new session was created.
     *
     * @param ses Opened session.
     * @throws GridException If GridNioException occurred while handling event.
     */
    public void onSessionOpened(GridNioSession ses) throws GridException;

    /**
     * Invoked after session get closed.
     *
     * @param ses Closed session.
     * @throws GridException If GridNioException occurred while handling event.
     */
    public void onSessionClosed(GridNioSession ses) throws GridException;

    /**
     * Invoked when exception is caught in filter processing.
     *
     * @param ses Session that caused GridException.
     * @param ex GridNioException instance.
     * @throws GridException If GridException occurred while handling event.
     */
    public void onExceptionCaught(GridNioSession ses, GridException ex) throws GridException;

    /**
     * Invoked when a write request is performed on a session.
     *
     * @param ses Session on which message should be written.
     * @param msg Message being written.
     * @return Write future.
     * @throws GridNioException If GridNioException occurred while handling event.
     */
    public GridNioFuture<?> onSessionWrite(GridNioSession ses, Object msg) throws GridException;

    /**
     * Invoked when a new messages received.
     *
     * @param ses Session on which message was received.
     * @param msg Received message.
     * @throws GridException If GridException occurred while handling event.
     */
    public void onMessageReceived(GridNioSession ses, Object msg) throws GridException;

    /**
     * Invoked when a session close request is performed on session.
     *
     * @param ses Session to close.
     * @return Close future.
     * @throws GridException If GridException occurred while handling event.
     */
    public GridNioFuture<Boolean> onSessionClose(GridNioSession ses) throws GridException;

    /**
     * Called when session is idle for longer time that is
     * allowed by NIO server.
     *
     * @param ses Session that is idle.
     * @throws GridException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    public void onSessionIdleTimeout(GridNioSession ses) throws GridException;

    /**
     * Forwards session idle notification to the next logical filter in filter chain.
     *
     * @param ses Session instance.
     * @throws GridException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    public void proceedSessionIdleTimeout(GridNioSession ses) throws GridException;

    /**
     * Called when session has not empty write buffer that has not been fully
     * flushed during max timeout allowed by NIO server.
     *
     * @param ses Session that has timed out writes.
     * @throws GridException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    public void onSessionWriteTimeout(GridNioSession ses) throws GridException;

    /**
     * Forwards session write timeout notification to the next logical filter in filter chain.
     *
     * @param ses Session instance.
     * @throws GridException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    public void proceedSessionWriteTimeout(GridNioSession ses) throws GridException;

    /**
     * Pauses reads for session.
     *
     * @param ses Session.
     * @return Future for operation.
     * @throws GridException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    public GridNioFuture<?> proceedPauseReads(GridNioSession ses) throws GridException;

    /**
     * Pauses reads for session.
     *
     * @param ses Session.
     * @return Future for operation.
     * @throws GridException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    public GridNioFuture<?> onPauseReads(GridNioSession ses) throws GridException;

    /**
     * Resumes reads for session.
     *
     * @param ses Session.
     * @return Future for operation.
     * @throws GridException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    public GridNioFuture<?> proceedResumeReads(GridNioSession ses) throws GridException;

    /**
     * Resumes reads for session.
     *
     * @param ses Session.
     * @return Future for operation.
     * @throws GridException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    public GridNioFuture<?> onResumeReads(GridNioSession ses) throws GridException;
}
