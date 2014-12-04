/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.jetbrains.annotations.*;

import java.net.*;

/**
 * Internal API extension for {@link GridGgfs}.
 */
public interface GridGgfsEx extends GridGgfs {
    /**
     * Stops GGFS cleaning all used resources.
     */
    public void stop();

    /**
     * @return GGFS context.
     */
    public GridGgfsContext context();

    /**
     * Get handshake message.
     *
     * @return Handshake message.
     */
    public GridGgfsPaths proxyPaths();

    /** {@inheritDoc} */
    @Override GridGgfsInputStreamAdapter open(GridGgfsPath path, int bufSize, int seqReadsBeforePrefetch)
        throws GridException;

    /** {@inheritDoc} */
    @Override GridGgfsInputStreamAdapter open(GridGgfsPath path) throws GridException;

    /** {@inheritDoc} */
    @Override GridGgfsInputStreamAdapter open(GridGgfsPath path, int bufSize) throws GridException;

    /**
     * Gets global space counters.
     *
     * @return Tuple in which first component is used space on all nodes,
     *      second is available space on all nodes.
     * @throws GridException If task execution failed.
     */
    public GridGgfsStatus globalSpace() throws GridException;

    /**
     * Enables, disables or clears sampling flag.
     *
     * @param val {@code True} to turn on sampling, {@code false} to turn it off, {@code null} to clear sampling state.
     * @throws GridException If failed.
     */
    public void globalSampling(@Nullable Boolean val) throws GridException;

    /**
     * Get sampling state.
     *
     * @return {@code True} in case sampling is enabled, {@code false} otherwise, or {@code null} in case sampling
     * flag is not set.
     */
    @Nullable public Boolean globalSampling();

    /**
     * Get local metrics.
     *
     * @return Local metrics.
     */
    public GridGgfsLocalMetrics localMetrics();

    /**
     * Gets group block size, i.e. block size multiplied by group size in affinity mapper.
     *
     * @return Group block size.
     */
    public long groupBlockSize();

    /**
     * Asynchronously await for all entries existing in trash to be removed.
     *
     * @return Future which will be completed when all entries existed in trash by the time of invocation are removed.
     * @throws GridException If failed.
     */
    public GridFuture<?> awaitDeletesAsync() throws GridException;

    /**
     * Gets client file system log directory.
     *
     * @return Client file system log directory or {@code null} in case no client connections have been created yet.
     */
    @Nullable public String clientLogDirectory();

    /**
     * Sets client file system log directory.
     *
     * @param logDir Client file system log directory.
     */
    public void clientLogDirectory(String logDir);

    /**
     * Whether this path is excluded from evictions.
     *
     * @param path Path.
     * @param primary Whether the mode is PRIMARY.
     * @return {@code True} if path is excluded from evictions.
     */
    public boolean evictExclude(GridGgfsPath path, boolean primary);

    /**
     * Get next affinity key.
     *
     * @return Next affinity key.
     */
    public IgniteUuid nextAffinityKey();

    /**
     * Check whether the given path is proxy path.
     *
     * @param path Path.
     * @return {@code True} if proxy.
     */
    public boolean isProxy(URI path);
}
