/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.hadoop.impl;

import org.apache.commons.logging.*;
import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.kernal.ggfs.hadoop.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.kernal.ggfs.hadoop.impl.NewGridGgfsHadoopEndpoint.*;

/**
 * Wrapper for GGFS server.
 */
public class NewGridGgfsHadoopWrapper implements NewGridGgfsHadoop {
    /** Delegate. */
    private final AtomicReference<NewGridGgfsHadoopEx> delegateRef = new AtomicReference<>();

    /** Connection string. */
    private final NewGridGgfsHadoopEndpoint endpoint;

    /** Log directory. */
    private final String logDir;

    /** Logger. */
    private final Log log;

    /**
     * Constructor.
     *
     * @param connStr Connection string.
     * @param logDir Log directory for server.
     * @param log Current logger.
     */
    NewGridGgfsHadoopWrapper(String connStr, String logDir, Log log) throws IOException {
        this.endpoint = new NewGridGgfsHadoopEndpoint(connStr);
        this.logDir = logDir;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        NewGridGgfsHadoopEx delegate = delegateRef.get();

        if (delegate != null && delegateRef.compareAndSet(delegate, null))
            delegate.close();
    }

    /** {@inheritDoc} */
    @Override public GridGgfsFile info(final GridGgfsPath path) throws GridException, IOException {
        return withReconnectHandling(new FileSystemClosure<GridGgfsFile>() {
            @Override
            public GridGgfsFile apply(NewGridGgfsHadoopEx delegate) throws GridException,
                IOException {
                return delegate.info(path);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public GridGgfsFile update(final GridGgfsPath path, final Map<String, String> props) throws GridException,
        IOException {
        return withReconnectHandling(new FileSystemClosure<GridGgfsFile>() {
            @Override
            public GridGgfsFile apply(NewGridGgfsHadoopEx delegate) throws GridException,
                IOException {
                return delegate.update(path, props);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Boolean setTimes(final GridGgfsPath path, final long accessTime, final long modificationTime)
        throws GridException, IOException {
        return withReconnectHandling(new FileSystemClosure<Boolean>() {
            @Override public Boolean apply(NewGridGgfsHadoopEx delegate) throws GridException,
                IOException {
                return delegate.setTimes(path, accessTime, modificationTime);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Boolean rename(final GridGgfsPath src, final GridGgfsPath dest) throws GridException, IOException {
        return withReconnectHandling(new FileSystemClosure<Boolean>() {
            @Override public Boolean apply(NewGridGgfsHadoopEx delegate) throws GridException,
                IOException {
                return delegate.rename(src, dest);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Boolean delete(final GridGgfsPath path, final boolean recursive) throws GridException,
        IOException {
        return withReconnectHandling(new FileSystemClosure<Boolean>() {
            @Override public Boolean apply(NewGridGgfsHadoopEx delegate) throws GridException,
                IOException {
                return delegate.delete(path, recursive);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Collection<GridGgfsBlockLocation> affinity(final GridGgfsPath path, final long start,
        final long len) throws GridException, IOException {
        return withReconnectHandling(new FileSystemClosure<Collection<GridGgfsBlockLocation>>() {
            @Override public Collection<GridGgfsBlockLocation> apply(NewGridGgfsHadoopEx delegate)
                throws GridException, IOException {
                return delegate.affinity(path, start, len);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public GridGgfsPathSummary contentSummary(final GridGgfsPath path) throws GridException, IOException {
        return withReconnectHandling(new FileSystemClosure<GridGgfsPathSummary>() {
            @Override public GridGgfsPathSummary apply(NewGridGgfsHadoopEx delegate) throws GridException,
                IOException {
                return delegate.contentSummary(path);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Boolean mkdirs(final GridGgfsPath path, final Map<String, String> props) throws GridException,
        IOException {
        return withReconnectHandling(new FileSystemClosure<Boolean>() {
            @Override public Boolean apply(NewGridGgfsHadoopEx delegate) throws GridException,
                IOException {
                return delegate.mkdirs(path, props);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Collection<GridGgfsFile> listFiles(final GridGgfsPath path) throws GridException, IOException {
        return withReconnectHandling(new FileSystemClosure<Collection<GridGgfsFile>>() {
            @Override public Collection<GridGgfsFile> apply(NewGridGgfsHadoopEx delegate) throws GridException,
                IOException {
                return delegate.listFiles(path);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Collection<GridGgfsPath> listPaths(final GridGgfsPath path) throws GridException, IOException {
        return withReconnectHandling(new FileSystemClosure<Collection<GridGgfsPath>>() {
            @Override public Collection<GridGgfsPath> apply(NewGridGgfsHadoopEx delegate) throws GridException,
                IOException {
                return delegate.listPaths(path);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public GridGgfsStatus fsStatus() throws GridException, IOException {
        return withReconnectHandling(new FileSystemClosure<GridGgfsStatus>() {
            @Override public GridGgfsStatus apply(NewGridGgfsHadoopEx delegate) throws GridException, IOException {
                return delegate.fsStatus();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public NewGridGgfsHadoopStreamDelegate open(final GridGgfsPath path) throws GridException, IOException {
        return withReconnectHandling(new FileSystemClosure<NewGridGgfsHadoopStreamDelegate>() {
            @Override public NewGridGgfsHadoopStreamDelegate apply(NewGridGgfsHadoopEx delegate) throws GridException,
                IOException {
                return delegate.open(path);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public NewGridGgfsHadoopStreamDelegate open(final GridGgfsPath path, final int seqReadsBeforePrefetch)
        throws GridException, IOException {
        return withReconnectHandling(new FileSystemClosure<NewGridGgfsHadoopStreamDelegate>() {
            @Override public NewGridGgfsHadoopStreamDelegate apply(NewGridGgfsHadoopEx delegate) throws GridException,
                IOException {
                return delegate.open(path, seqReadsBeforePrefetch);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public NewGridGgfsHadoopStreamDelegate create(final GridGgfsPath path, final boolean overwrite,
        final boolean colocate, final int replication, final long blockSize, final @Nullable Map<String, String> props)
        throws GridException, IOException {
        return withReconnectHandling(new FileSystemClosure<NewGridGgfsHadoopStreamDelegate>() {
            @Override public NewGridGgfsHadoopStreamDelegate apply(NewGridGgfsHadoopEx delegate) throws GridException,
                IOException {
                return delegate.create(path, overwrite, colocate, replication, blockSize, props);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public NewGridGgfsHadoopStreamDelegate append(final GridGgfsPath path, final boolean create,
        final @Nullable Map<String, String> props) throws GridException, IOException {
        return withReconnectHandling(new FileSystemClosure<NewGridGgfsHadoopStreamDelegate>() {
            @Override public NewGridGgfsHadoopStreamDelegate apply(NewGridGgfsHadoopEx delegate) throws GridException,
                IOException {
                return delegate.append(path, create, props);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public byte[] readData(NewGridGgfsHadoopStreamDelegate delegate, long pos, int len,
        @Nullable byte[] outBuf, int outOff, int outLen) throws GridException, IOException {
        return delegate.hadoop().readData(delegate, pos, len, outBuf, outOff, outLen);
    }

    /** {@inheritDoc} */
    @Override public void writeData(NewGridGgfsHadoopStreamDelegate delegate, byte[] data, int off, int len)
        throws GridException, IOException {
        delegate.hadoop().writeData(delegate, data, off, len);
    }

    /** {@inheritDoc} */
    @Override public Boolean closeStream(NewGridGgfsHadoopStreamDelegate delegate) throws GridException,
        IOException {
        return delegate.hadoop().closeStream(delegate);
    }

    /**
     * Execute closure.
     *
     * @param clo Closure.
     * @return Result.
     * @throws IOException If failed.
     */
    private <T> T withReconnectHandling(final FileSystemClosure<T> clo) throws IOException {
        Exception err = null;

        for (int i = 0; i < 2; i++) {
            NewGridGgfsHadoopEx curDelegate = null;

            try {
                curDelegate = delegate();

                return clo.apply(curDelegate);
            }
            catch (GridGgfsHadoopCommunicationException e) {
                curDelegate.close();

                if (!delegateRef.compareAndSet(curDelegate, null))
                    // Close if was overwritten concurrently.
                    curDelegate.close();

                if (log.isDebugEnabled())
                    log.debug("Failed to send message to a server: " + e);

                err = e;
            }
            catch (GridException e) {
                throw new IOException(e);
            }
        }

        throw new IOException("Failed to establish connection with GGFS.", err);
    }

    /**
     * Get delegate creating it if needed.
     *
     * @return Delegate.
     */
    private NewGridGgfsHadoopEx delegate() throws IOException {
        GridException err = null;

        // 1. If delegate is set, return it immediately.
        NewGridGgfsHadoopEx curDelegate = delegateRef.get();

        if (curDelegate != null)
            return curDelegate;

        // 2. Guess that we are in the same VM.
        GridGgfsEx ggfs = null;

        if (endpoint.grid() == null) {
            try {
                Grid grid = G.grid();

                ggfs = (GridGgfsEx)grid.ggfs(endpoint.ggfs());
            }
            catch (Exception ignore) {
                // No-op.
            }
        }
        else {
            for (Grid grid : G.allGrids()) {
                try {
                    ggfs = (GridGgfsEx)grid.ggfs(endpoint.ggfs());

                    break;
                }
                catch (Exception ignore) {
                    // No-op.
                }
            }
        }

        if (ggfs != null) {
            try {
                NewGridGgfsHadoopEx res = new NewGridGgfsHadoopInProc(ggfs, log);

                res.handshake(logDir);

                curDelegate = res;
            }
            catch (GridException e) {
                log.debug("Failed to connect to in-proc GGFS, fallback to IPC mode.", e);

                err = e;
            }
        }

        // 3. Try connecting using shmem.
        if (curDelegate == null && !U.isWindows()) {
            try {
                NewGridGgfsHadoopEx res = new NewGridGgfsHadoopOutProc(log);

                res.handshake(logDir);

                curDelegate = res;
            }
            catch (GridException e) {
                log.debug("Failed to connect to out-proc local GGFS using shmem.", e);

                err = e;
            }
        }

        // 4. Try local TCP connection.
        if (curDelegate == null) {
            try {
                NewGridGgfsHadoopEx res = new NewGridGgfsHadoopOutProc(LOCALHOST, endpoint.port(), log);

                res.handshake(logDir);

                curDelegate = res;
            }
            catch (GridException e) {
                log.debug("Failed to connect to out-proc local GGFS using TCP.", e);

                err = e;
            }
        }

        // 5. Try remote TCP connection.
        if (curDelegate == null && !F.eq(LOCALHOST, endpoint.host())) {
            try {
                NewGridGgfsHadoopEx res = new NewGridGgfsHadoopOutProc(LOCALHOST, endpoint.port(), log);

                res.handshake(logDir);

                curDelegate = res;
            }
            catch (GridException e) {
                log.debug("Failed to connect to out-proc remote GGFS using TCP.", e);

                err = e;
            }
        }

        if (curDelegate != null)
            return curDelegate;
        else
            throw new IOException("Failed to connect to GGFS: " + endpoint, err);
    }

    /**
     * File system operation closure.
     */
    private static interface FileSystemClosure<T> {
        /**
         * Call closure body.
         *
         * @param delegate GGFS delegate.
         * @return Result.
         * @throws GridException If failed.
         * @throws IOException If failed.
         */
        public T apply(NewGridGgfsHadoopEx delegate) throws GridException, IOException;
    }

}
