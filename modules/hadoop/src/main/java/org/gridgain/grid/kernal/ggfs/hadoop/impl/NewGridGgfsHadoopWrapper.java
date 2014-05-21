/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.hadoop.impl;

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
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

import static org.gridgain.grid.ggfs.hadoop.GridGgfsHadoopParameters.*;
import static org.gridgain.grid.kernal.ggfs.hadoop.impl.NewGridGgfsHadoopEndpoint.*;
import static org.gridgain.grid.kernal.ggfs.hadoop.impl.NewGridGgfsHadoopUtils.*;

/**
 * Wrapper for GGFS server.
 */
public class NewGridGgfsHadoopWrapper implements NewGridGgfsHadoop {
    /** Delegate. */
    private final AtomicReference<Delegate> delegateRef = new AtomicReference<>();

    /** Authority. */
    private final String authority;

    /** Connection string. */
    private final NewGridGgfsHadoopEndpoint endpoint;

    /** Log directory. */
    private final String logDir;

    /** Configuration. */
    private final Configuration conf;

    /** Logger. */
    private final Log log;

    /**
     * Constructor.
     *
     * @param authority Authority (connection string).
     * @param logDir Log directory for server.
     * @param conf Configuration.
     * @param log Current logger.
     */
    public NewGridGgfsHadoopWrapper(String authority, String logDir, Configuration conf, Log log) throws IOException {
        try {
            this.authority = authority;
            this.endpoint = new NewGridGgfsHadoopEndpoint(authority);
            this.logDir = logDir;
            this.conf = conf;
            this.log = log;
        }
        catch (IllegalArgumentException e) {
            throw new IOException("Failed to parse endpoint: " + authority);
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
        }, path);
    }

    /** {@inheritDoc} */
    @Override public GridGgfsFile update(final GridGgfsPath path, final Map<String, String> props) throws IOException {
        return withReconnectHandling(new FileSystemClosure<GridGgfsFile>() {
            @Override public GridGgfsFile apply(NewGridGgfsHadoopEx hadoop, GridGgfsHandshakeResponse hndResp)
                throws GridException, IOException {
                return hadoop.update(path, props);
            }
        }, path);
    }

    /** {@inheritDoc} */
    @Override public Boolean setTimes(final GridGgfsPath path, final long accessTime, final long modificationTime)
        throws IOException {
        return withReconnectHandling(new FileSystemClosure<Boolean>() {
            @Override public Boolean apply(NewGridGgfsHadoopEx hadoop, GridGgfsHandshakeResponse hndResp)
                throws GridException, IOException {
                return hadoop.setTimes(path, accessTime, modificationTime);
            }
        }, path);
    }

    /** {@inheritDoc} */
    @Override public Boolean rename(final GridGgfsPath src, final GridGgfsPath dest) throws IOException {
        return withReconnectHandling(new FileSystemClosure<Boolean>() {
            @Override public Boolean apply(NewGridGgfsHadoopEx hadoop, GridGgfsHandshakeResponse hndResp)
                throws GridException, IOException {
                return hadoop.rename(src, dest);
            }
        }, src);
    }

    /** {@inheritDoc} */
    @Override public Boolean delete(final GridGgfsPath path, final boolean recursive) throws IOException {
        return withReconnectHandling(new FileSystemClosure<Boolean>() {
            @Override public Boolean apply(NewGridGgfsHadoopEx hadoop, GridGgfsHandshakeResponse hndResp)
                throws GridException, IOException {
                return hadoop.delete(path, recursive);
            }
        }, path);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridGgfsBlockLocation> affinity(final GridGgfsPath path, final long start,
        final long len) throws IOException {
        return withReconnectHandling(new FileSystemClosure<Collection<GridGgfsBlockLocation>>() {
            @Override public Collection<GridGgfsBlockLocation> apply(NewGridGgfsHadoopEx hadoop,
                GridGgfsHandshakeResponse hndResp) throws GridException, IOException {
                return hadoop.affinity(path, start, len);
            }
        }, path);
    }

    /** {@inheritDoc} */
    @Override public GridGgfsPathSummary contentSummary(final GridGgfsPath path) throws IOException {
        return withReconnectHandling(new FileSystemClosure<GridGgfsPathSummary>() {
            @Override public GridGgfsPathSummary apply(NewGridGgfsHadoopEx hadoop, GridGgfsHandshakeResponse hndResp)
                throws GridException, IOException {
                return hadoop.contentSummary(path);
            }
        }, path);
    }

    /** {@inheritDoc} */
    @Override public Boolean mkdirs(final GridGgfsPath path, final Map<String, String> props) throws IOException {
        return withReconnectHandling(new FileSystemClosure<Boolean>() {
            @Override public Boolean apply(NewGridGgfsHadoopEx hadoop, GridGgfsHandshakeResponse hndResp)
                throws GridException, IOException {
                return hadoop.mkdirs(path, props);
            }
        }, path);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridGgfsFile> listFiles(final GridGgfsPath path) throws IOException {
        return withReconnectHandling(new FileSystemClosure<Collection<GridGgfsFile>>() {
            @Override public Collection<GridGgfsFile> apply(NewGridGgfsHadoopEx hadoop,
                GridGgfsHandshakeResponse hndResp) throws GridException, IOException {
                return hadoop.listFiles(path);
            }
        }, path);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridGgfsPath> listPaths(final GridGgfsPath path) throws IOException {
        return withReconnectHandling(new FileSystemClosure<Collection<GridGgfsPath>>() {
            @Override public Collection<GridGgfsPath> apply(NewGridGgfsHadoopEx hadoop,
                GridGgfsHandshakeResponse hndResp) throws GridException, IOException {
                return hadoop.listPaths(path);
            }
        }, path);
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
        }, path);
    }

    /** {@inheritDoc} */
    @Override public NewGridGgfsHadoopStreamDelegate open(final GridGgfsPath path, final int seqReadsBeforePrefetch)
        throws IOException {
        return withReconnectHandling(new FileSystemClosure<NewGridGgfsHadoopStreamDelegate>() {
            @Override public NewGridGgfsHadoopStreamDelegate apply(NewGridGgfsHadoopEx hadoop,
                GridGgfsHandshakeResponse hndResp) throws GridException, IOException {
                return hadoop.open(path, seqReadsBeforePrefetch);
            }
        }, path);
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
        }, path);
    }

    /** {@inheritDoc} */
    @Override public NewGridGgfsHadoopStreamDelegate append(final GridGgfsPath path, final boolean create,
        final @Nullable Map<String, String> props) throws IOException {
        return withReconnectHandling(new FileSystemClosure<NewGridGgfsHadoopStreamDelegate>() {
            @Override public NewGridGgfsHadoopStreamDelegate apply(NewGridGgfsHadoopEx hadoop,
                GridGgfsHandshakeResponse hndResp) throws GridException, IOException {
                return hadoop.append(path, create, props);
            }
        }, path);
    }

    /**
     * Execute closure which is not path-specific.
     *
     * @param clo Closure.
     * @return Result.
     * @throws IOException If failed.
     */
    private <T> T withReconnectHandling(FileSystemClosure<T> clo) throws IOException {
        return withReconnectHandling(clo, null);
    }

    /**
     * Execute closure.
     *
     * @param clo Closure.
     * @param path Path for exceptions.
     * @return Result.
     * @throws IOException If failed.
     */
    private <T> T withReconnectHandling(final FileSystemClosure<T> clo, @Nullable GridGgfsPath path)
        throws IOException {
        Exception err = null;

        for (int i = 0; i < 2; i++) {
            Delegate curDelegate = null;

            boolean close = false;

            try {
                curDelegate = delegate();

                assert curDelegate != null;

                close = curDelegate.doomed;

                return clo.apply(curDelegate.hadoop, curDelegate.hndResp);
            }
            catch (GridGgfsHadoopCommunicationException e) {
                if (curDelegate != null && !curDelegate.doomed) {
                    // Try getting rid fo faulty delegate ASAP.
                    delegateRef.compareAndSet(curDelegate, null);

                    close = true;
                }

                if (log.isDebugEnabled())
                    log.debug("Failed to send message to a server: " + e);

                err = e;
            }
            catch (GridException e) {
                throw NewGridGgfsHadoopUtils.cast(e, path != null ? path.toString() : null);
            }
            finally {
                if (close) {
                    assert curDelegate != null;

                    curDelegate.close();
                }
            }
        }

        throw new IOException("Failed to communicate with GGFS.", err);
    }

    /**
     * Get delegate creating it if needed.
     *
     * @return Delegate.
     */
    private Delegate delegate() throws GridGgfsHadoopCommunicationException {
        Exception err = null;

        // 1. If delegate is set, return it immediately.
        Delegate curDelegate = delegateRef.get();

        if (curDelegate != null)
            return curDelegate;

        // 2. Guess that we are in the same VM.
        if (!parameter(conf, PARAM_GGFS_ENDPOINT_NO_EMBED, authority, false)) {
            GridGgfsEx ggfs = null;

            if (endpoint.grid() == null) {
                try {
                    Grid grid = G.grid();

                    ggfs = (GridGgfsEx)grid.ggfs(endpoint.ggfs());
                }
                catch (Exception e) {
                    err = e;
                }
            }
            else {
                for (Grid grid : G.allGrids()) {
                    try {
                        ggfs = (GridGgfsEx)grid.ggfs(endpoint.ggfs());

                        break;
                    }
                    catch (Exception e) {
                        err = e;
                    }
                }
            }

            if (ggfs != null) {
                try {
                    NewGridGgfsHadoopEx hadoop = new NewGridGgfsHadoopInProc(ggfs, log);

                    curDelegate = new Delegate(hadoop, hadoop.handshake(logDir));
                }
                catch (IOException | GridException e) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to connect to in-proc GGFS, fallback to IPC mode.", e);

                    err = e;
                }
            }
        }

        // 3. Try connecting using shmem.
        if (!parameter(conf, PARAM_GGFS_ENDPOINT_NO_LOCAL_SHMEM, authority, false)) {
            if (curDelegate == null && !U.isWindows()) {
                try {
                    NewGridGgfsHadoopEx hadoop = new NewGridGgfsHadoopOutProc(endpoint.port(), log);

                    curDelegate = new Delegate(hadoop, hadoop.handshake(logDir));
                }
                catch (IOException | GridException e) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to connect to out-proc local GGFS using shmem.", e);

                    err = e;
                }
            }
        }

        // 4. Try local TCP connection.
        boolean skipLocalTcp = parameter(conf, PARAM_GGFS_ENDPOINT_NO_LOCAL_TCP, authority, false);

        if (!skipLocalTcp) {
            if (curDelegate == null) {
                try {
                    NewGridGgfsHadoopEx hadoop = new NewGridGgfsHadoopOutProc(LOCALHOST, endpoint.port(), log);

                    curDelegate = new Delegate(hadoop, hadoop.handshake(logDir));
                }
                catch (IOException | GridException e) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to connect to out-proc local GGFS using TCP.", e);

                    err = e;
                }
            }
        }

        // 5. Try remote TCP connection.
        if (curDelegate == null && (skipLocalTcp || !F.eq(LOCALHOST, endpoint.host()))) {
            try {
                NewGridGgfsHadoopEx hadoop = new NewGridGgfsHadoopOutProc(LOCALHOST, endpoint.port(), log);

                curDelegate = new Delegate(hadoop, hadoop.handshake(logDir));
            }
            catch (IOException | GridException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to connect to out-proc remote GGFS using TCP.", e);

                err = e;
            }
        }

        if (curDelegate != null) {
            if (!delegateRef.compareAndSet(null, curDelegate))
                curDelegate.doomed = true;

            return curDelegate;
        }
        else
            throw new GridGgfsHadoopCommunicationException("Failed to connect to GGFS: " + endpoint, err);
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

        /** Close guard. */
        private final AtomicBoolean closeGuard = new AtomicBoolean();

        /** Whether this delegate must be closed at the end of the next invocation. */
        private boolean doomed;

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

        /**
         * Close underlying RPC handler.
         */
        private void close() {
            if (closeGuard.compareAndSet(false, true))
                hadoop.close();
        }
    }

}
