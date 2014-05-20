/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.hadoop.impl;

import org.apache.commons.logging.*;
import org.apache.hadoop.fs.*;
import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.kernal.ggfs.hadoop.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.util.lang.*;
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
    private final AtomicReference<Delegate> delegateRef = new AtomicReference<>();

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
    public NewGridGgfsHadoopWrapper(String connStr, String logDir, Log log) throws IOException {
        try {
            this.endpoint = new NewGridGgfsHadoopEndpoint(connStr);
            this.logDir = logDir;
            this.log = log;
        }
        catch (IllegalArgumentException e) {
            throw new IOException("Failed to parse endpoint: " + connStr);
        }
    }

    /** {@inheritDoc} */
    @Override public GridGgfsHandshakeResponse handshake(String logDir) throws IOException {
        return withReconnectHandling(new FileSystemClosure<GridGgfsHandshakeResponse>() {
            @Override public GridGgfsHandshakeResponse apply(NewGridGgfsHadoopEx hadoop,
                GridGgfsHandshakeResponse hndResp) throws GridException, IOException {
                return hndResp;
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void close() {
        Delegate delegate = delegateRef.get();

        if (delegate != null && delegateRef.compareAndSet(delegate, null))
            delegate.hadoop.close();
    }

    /** {@inheritDoc} */
    @Override public GridGgfsFile info(final GridGgfsPath path) throws IOException {
        return withReconnectHandling(new FileSystemClosure<GridGgfsFile>() {
            @Override public GridGgfsFile apply(NewGridGgfsHadoopEx hadoop, GridGgfsHandshakeResponse hndResp)
                throws GridException, IOException {
                return hadoop.info(path);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public GridGgfsFile update(final GridGgfsPath path, final Map<String, String> props) throws IOException {
        return withReconnectHandling(new FileSystemClosure<GridGgfsFile>() {
            @Override public GridGgfsFile apply(NewGridGgfsHadoopEx hadoop, GridGgfsHandshakeResponse hndResp)
                throws GridException, IOException {
                return hadoop.update(path, props);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Boolean setTimes(final GridGgfsPath path, final long accessTime, final long modificationTime)
        throws IOException {
        return withReconnectHandling(new FileSystemClosure<Boolean>() {
            @Override public Boolean apply(NewGridGgfsHadoopEx hadoop, GridGgfsHandshakeResponse hndResp)
                throws GridException, IOException {
                return hadoop.setTimes(path, accessTime, modificationTime);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Boolean rename(final GridGgfsPath src, final GridGgfsPath dest) throws IOException {
        return withReconnectHandling(new FileSystemClosure<Boolean>() {
            @Override public Boolean apply(NewGridGgfsHadoopEx hadoop, GridGgfsHandshakeResponse hndResp)
                throws GridException, IOException {
                return hadoop.rename(src, dest);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Boolean delete(final GridGgfsPath path, final boolean recursive) throws IOException {
        return withReconnectHandling(new FileSystemClosure<Boolean>() {
            @Override public Boolean apply(NewGridGgfsHadoopEx hadoop, GridGgfsHandshakeResponse hndResp)
                throws GridException, IOException {
                return hadoop.delete(path, recursive);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Collection<GridGgfsBlockLocation> affinity(final GridGgfsPath path, final long start,
        final long len) throws IOException {
        return withReconnectHandling(new FileSystemClosure<Collection<GridGgfsBlockLocation>>() {
            @Override public Collection<GridGgfsBlockLocation> apply(NewGridGgfsHadoopEx hadoop,
                GridGgfsHandshakeResponse hndResp) throws GridException, IOException {
                return hadoop.affinity(path, start, len);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public GridGgfsPathSummary contentSummary(final GridGgfsPath path) throws IOException {
        return withReconnectHandling(new FileSystemClosure<GridGgfsPathSummary>() {
            @Override public GridGgfsPathSummary apply(NewGridGgfsHadoopEx hadoop, GridGgfsHandshakeResponse hndResp)
                throws GridException, IOException {
                return hadoop.contentSummary(path);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Boolean mkdirs(final GridGgfsPath path, final Map<String, String> props) throws IOException {
        return withReconnectHandling(new FileSystemClosure<Boolean>() {
            @Override public Boolean apply(NewGridGgfsHadoopEx hadoop, GridGgfsHandshakeResponse hndResp)
                throws GridException, IOException {
                return hadoop.mkdirs(path, props);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Collection<GridGgfsFile> listFiles(final GridGgfsPath path) throws IOException {
        return withReconnectHandling(new FileSystemClosure<Collection<GridGgfsFile>>() {
            @Override public Collection<GridGgfsFile> apply(NewGridGgfsHadoopEx hadoop,
                GridGgfsHandshakeResponse hndResp) throws GridException, IOException {
                return hadoop.listFiles(path);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Collection<GridGgfsPath> listPaths(final GridGgfsPath path) throws IOException {
        return withReconnectHandling(new FileSystemClosure<Collection<GridGgfsPath>>() {
            @Override public Collection<GridGgfsPath> apply(NewGridGgfsHadoopEx hadoop,
                GridGgfsHandshakeResponse hndResp) throws GridException, IOException {
                return hadoop.listPaths(path);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public GridGgfsStatus fsStatus() throws IOException {
        return withReconnectHandling(new FileSystemClosure<GridGgfsStatus>() {
            @Override public GridGgfsStatus apply(NewGridGgfsHadoopEx hadoop, GridGgfsHandshakeResponse hndResp)
                throws GridException, IOException {
                return hadoop.fsStatus();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public NewGridGgfsHadoopStreamDelegate open(final GridGgfsPath path) throws IOException {
        return withReconnectHandling(new FileSystemClosure<NewGridGgfsHadoopStreamDelegate>() {
            @Override public NewGridGgfsHadoopStreamDelegate apply(NewGridGgfsHadoopEx hadoop,
                GridGgfsHandshakeResponse hndResp) throws GridException, IOException {
                return hadoop.open(path);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public NewGridGgfsHadoopStreamDelegate open(final GridGgfsPath path, final int seqReadsBeforePrefetch)
        throws IOException {
        return withReconnectHandling(new FileSystemClosure<NewGridGgfsHadoopStreamDelegate>() {
            @Override public NewGridGgfsHadoopStreamDelegate apply(NewGridGgfsHadoopEx hadoop,
                GridGgfsHandshakeResponse hndResp) throws GridException, IOException {
                return hadoop.open(path, seqReadsBeforePrefetch);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public NewGridGgfsHadoopStreamDelegate create(final GridGgfsPath path, final boolean overwrite,
        final boolean colocate, final int replication, final long blockSize, final @Nullable Map<String, String> props)
        throws IOException {
        return withReconnectHandling(new FileSystemClosure<NewGridGgfsHadoopStreamDelegate>() {
            @Override public NewGridGgfsHadoopStreamDelegate apply(NewGridGgfsHadoopEx hadoop,
                GridGgfsHandshakeResponse hndResp) throws GridException, IOException {
                return hadoop.create(path, overwrite, colocate, replication, blockSize, props);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public NewGridGgfsHadoopStreamDelegate append(final GridGgfsPath path, final boolean create,
        final @Nullable Map<String, String> props) throws IOException {
        return withReconnectHandling(new FileSystemClosure<NewGridGgfsHadoopStreamDelegate>() {
            @Override public NewGridGgfsHadoopStreamDelegate apply(NewGridGgfsHadoopEx hadoop,
                GridGgfsHandshakeResponse hndResp) throws GridException, IOException {
                return hadoop.append(path, create, props);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public GridPlainFuture<byte[]> readData(NewGridGgfsHadoopStreamDelegate delegate, long pos, int len,
        @Nullable byte[] outBuf, int outOff, int outLen) {
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
     * Cast GG exception to appropriate IO exception.
     *
     * @param e Exception to cast.
     * @return Casted exception.
     */
    private static IOException cast(GridException e) {
        assert e != null;

        if (e instanceof GridGgfsFileNotFoundException)
            return new FileNotFoundException(e.getMessage());

        else if (e instanceof GridGgfsDirectoryNotEmptyException)
            return new PathIsNotEmptyDirectoryException(e.getMessage());

        else if (e instanceof GridGgfsParentNotDirectoryException)
            return new ParentNotDirectoryException(e.getMessage());

        else if (e instanceof GridGgfsPathAlreadyExistsException)
            return new PathExistsException(e.getMessage());

        else
            return new IOException(e);
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
            Delegate curDelegate = null;

            try {
                curDelegate = delegate();

                return clo.apply(curDelegate.hadoop, curDelegate.hndResp);
            }
            catch (GridGgfsHadoopCommunicationException e) {
                curDelegate.hadoop.close();

                if (!delegateRef.compareAndSet(curDelegate, null))
                    // Close if was overwritten concurrently.
                    curDelegate.hadoop.close();

                if (log.isDebugEnabled())
                    log.debug("Failed to send message to a server: " + e);

                err = e;
            }
            catch (GridException e) {
                throw cast(e);
            }
        }

        throw new IOException("Failed to establish connection with GGFS.", err);
    }

    /**
     * Get delegate creating it if needed.
     *
     * @return Delegate.
     */
    private Delegate delegate() throws IOException {
        GridException err = null;

        // 1. If delegate is set, return it immediately.
        Delegate curDelegate = delegateRef.get();

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
                NewGridGgfsHadoopEx hadoop = new NewGridGgfsHadoopInProc(ggfs, log);

                curDelegate = new Delegate(hadoop, hadoop.handshake(logDir));
            }
            catch (GridException e) {
                log.debug("Failed to connect to in-proc GGFS, fallback to IPC mode.", e);

                err = e;
            }
        }

        // 3. Try connecting using shmem.
        if (curDelegate == null && !U.isWindows()) {
            try {
                NewGridGgfsHadoopEx hadoop = new NewGridGgfsHadoopOutProc(log);

                curDelegate = new Delegate(hadoop, hadoop.handshake(logDir));
            }
            catch (GridException e) {
                log.debug("Failed to connect to out-proc local GGFS using shmem.", e);

                err = e;
            }
        }

        // 4. Try local TCP connection.
        if (curDelegate == null) {
            try {
                NewGridGgfsHadoopEx hadoop = new NewGridGgfsHadoopOutProc(LOCALHOST, endpoint.port(), log);

                curDelegate = new Delegate(hadoop, hadoop.handshake(logDir));
            }
            catch (GridException e) {
                log.debug("Failed to connect to out-proc local GGFS using TCP.", e);

                err = e;
            }
        }

        // 5. Try remote TCP connection.
        if (curDelegate == null && !F.eq(LOCALHOST, endpoint.host())) {
            try {
                NewGridGgfsHadoopEx hadoop = new NewGridGgfsHadoopOutProc(LOCALHOST, endpoint.port(), log);

                curDelegate = new Delegate(hadoop, hadoop.handshake(logDir));
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
         * @param hadoop RPC handler.
         * @param hndResp Handshake response.
         * @return Result.
         * @throws GridException If failed.
         * @throws IOException If failed.
         */
        public T apply(NewGridGgfsHadoopEx hadoop, GridGgfsHandshakeResponse hndResp) throws GridException, IOException;
    }

    /**
     * Delegate.
     */
    private static class Delegate {
        /** RPC handler. */
        private final NewGridGgfsHadoopEx hadoop;

        /** Handshake request. */
        private final GridGgfsHandshakeResponse hndResp;

        /**
         * Constructor.
         *
         * @param hadoop Hadoop.
         * @param hndResp Handshake response.
         */
        private Delegate(NewGridGgfsHadoopEx hadoop, GridGgfsHandshakeResponse hndResp) {
            this.hadoop = hadoop;
            this.hndResp = hndResp;
        }
    }

}
