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

package org.apache.ignite.internal.fs.hadoop;

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.ignite.*;
import org.apache.ignite.ignitefs.*;
import org.apache.ignite.internal.processors.fs.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.internal.fs.hadoop.IgfsHadoopEndpoint.*;
import static org.apache.ignite.internal.fs.hadoop.IgfsHadoopUtils.*;

/**
 * Wrapper for GGFS server.
 */
public class IgfsHadoopWrapper implements IgfsHadoop {
    /** Delegate. */
    private final AtomicReference<Delegate> delegateRef = new AtomicReference<>();

    /** Authority. */
    private final String authority;

    /** Connection string. */
    private final IgfsHadoopEndpoint endpoint;

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
    public IgfsHadoopWrapper(String authority, String logDir, Configuration conf, Log log) throws IOException {
        try {
            this.authority = authority;
            this.endpoint = new IgfsHadoopEndpoint(authority);
            this.logDir = logDir;
            this.conf = conf;
            this.log = log;
        }
        catch (IgniteCheckedException e) {
            throw new IOException("Failed to parse endpoint: " + authority, e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgfsHandshakeResponse handshake(String logDir) throws IOException {
        return withReconnectHandling(new FileSystemClosure<IgfsHandshakeResponse>() {
            @Override public IgfsHandshakeResponse apply(IgfsHadoopEx hadoop,
                IgfsHandshakeResponse hndResp) throws IgniteCheckedException, IOException {
                return hndResp;
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void close(boolean force) {
        Delegate delegate = delegateRef.get();

        if (delegate != null && delegateRef.compareAndSet(delegate, null))
            delegate.close(force);
    }

    /** {@inheritDoc} */
    @Override public IgniteFsFile info(final IgniteFsPath path) throws IOException {
        return withReconnectHandling(new FileSystemClosure<IgniteFsFile>() {
            @Override public IgniteFsFile apply(IgfsHadoopEx hadoop, IgfsHandshakeResponse hndResp)
                throws IgniteCheckedException, IOException {
                return hadoop.info(path);
            }
        }, path);
    }

    /** {@inheritDoc} */
    @Override public IgniteFsFile update(final IgniteFsPath path, final Map<String, String> props) throws IOException {
        return withReconnectHandling(new FileSystemClosure<IgniteFsFile>() {
            @Override public IgniteFsFile apply(IgfsHadoopEx hadoop, IgfsHandshakeResponse hndResp)
                throws IgniteCheckedException, IOException {
                return hadoop.update(path, props);
            }
        }, path);
    }

    /** {@inheritDoc} */
    @Override public Boolean setTimes(final IgniteFsPath path, final long accessTime, final long modificationTime)
        throws IOException {
        return withReconnectHandling(new FileSystemClosure<Boolean>() {
            @Override public Boolean apply(IgfsHadoopEx hadoop, IgfsHandshakeResponse hndResp)
                throws IgniteCheckedException, IOException {
                return hadoop.setTimes(path, accessTime, modificationTime);
            }
        }, path);
    }

    /** {@inheritDoc} */
    @Override public Boolean rename(final IgniteFsPath src, final IgniteFsPath dest) throws IOException {
        return withReconnectHandling(new FileSystemClosure<Boolean>() {
            @Override public Boolean apply(IgfsHadoopEx hadoop, IgfsHandshakeResponse hndResp)
                throws IgniteCheckedException, IOException {
                return hadoop.rename(src, dest);
            }
        }, src);
    }

    /** {@inheritDoc} */
    @Override public Boolean delete(final IgniteFsPath path, final boolean recursive) throws IOException {
        return withReconnectHandling(new FileSystemClosure<Boolean>() {
            @Override public Boolean apply(IgfsHadoopEx hadoop, IgfsHandshakeResponse hndResp)
                throws IgniteCheckedException, IOException {
                return hadoop.delete(path, recursive);
            }
        }, path);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFsBlockLocation> affinity(final IgniteFsPath path, final long start,
        final long len) throws IOException {
        return withReconnectHandling(new FileSystemClosure<Collection<IgniteFsBlockLocation>>() {
            @Override public Collection<IgniteFsBlockLocation> apply(IgfsHadoopEx hadoop,
                IgfsHandshakeResponse hndResp) throws IgniteCheckedException, IOException {
                return hadoop.affinity(path, start, len);
            }
        }, path);
    }

    /** {@inheritDoc} */
    @Override public IgniteFsPathSummary contentSummary(final IgniteFsPath path) throws IOException {
        return withReconnectHandling(new FileSystemClosure<IgniteFsPathSummary>() {
            @Override public IgniteFsPathSummary apply(IgfsHadoopEx hadoop, IgfsHandshakeResponse hndResp)
                throws IgniteCheckedException, IOException {
                return hadoop.contentSummary(path);
            }
        }, path);
    }

    /** {@inheritDoc} */
    @Override public Boolean mkdirs(final IgniteFsPath path, final Map<String, String> props) throws IOException {
        return withReconnectHandling(new FileSystemClosure<Boolean>() {
            @Override public Boolean apply(IgfsHadoopEx hadoop, IgfsHandshakeResponse hndResp)
                throws IgniteCheckedException, IOException {
                return hadoop.mkdirs(path, props);
            }
        }, path);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFsFile> listFiles(final IgniteFsPath path) throws IOException {
        return withReconnectHandling(new FileSystemClosure<Collection<IgniteFsFile>>() {
            @Override public Collection<IgniteFsFile> apply(IgfsHadoopEx hadoop,
                IgfsHandshakeResponse hndResp) throws IgniteCheckedException, IOException {
                return hadoop.listFiles(path);
            }
        }, path);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFsPath> listPaths(final IgniteFsPath path) throws IOException {
        return withReconnectHandling(new FileSystemClosure<Collection<IgniteFsPath>>() {
            @Override public Collection<IgniteFsPath> apply(IgfsHadoopEx hadoop,
                IgfsHandshakeResponse hndResp) throws IgniteCheckedException, IOException {
                return hadoop.listPaths(path);
            }
        }, path);
    }

    /** {@inheritDoc} */
    @Override public IgfsStatus fsStatus() throws IOException {
        return withReconnectHandling(new FileSystemClosure<IgfsStatus>() {
            @Override public IgfsStatus apply(IgfsHadoopEx hadoop, IgfsHandshakeResponse hndResp)
                throws IgniteCheckedException, IOException {
                return hadoop.fsStatus();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgfsHadoopStreamDelegate open(final IgniteFsPath path) throws IOException {
        return withReconnectHandling(new FileSystemClosure<IgfsHadoopStreamDelegate>() {
            @Override public IgfsHadoopStreamDelegate apply(IgfsHadoopEx hadoop,
                IgfsHandshakeResponse hndResp) throws IgniteCheckedException, IOException {
                return hadoop.open(path);
            }
        }, path);
    }

    /** {@inheritDoc} */
    @Override public IgfsHadoopStreamDelegate open(final IgniteFsPath path, final int seqReadsBeforePrefetch)
        throws IOException {
        return withReconnectHandling(new FileSystemClosure<IgfsHadoopStreamDelegate>() {
            @Override public IgfsHadoopStreamDelegate apply(IgfsHadoopEx hadoop,
                IgfsHandshakeResponse hndResp) throws IgniteCheckedException, IOException {
                return hadoop.open(path, seqReadsBeforePrefetch);
            }
        }, path);
    }

    /** {@inheritDoc} */
    @Override public IgfsHadoopStreamDelegate create(final IgniteFsPath path, final boolean overwrite,
        final boolean colocate, final int replication, final long blockSize, @Nullable final Map<String, String> props)
        throws IOException {
        return withReconnectHandling(new FileSystemClosure<IgfsHadoopStreamDelegate>() {
            @Override public IgfsHadoopStreamDelegate apply(IgfsHadoopEx hadoop,
                IgfsHandshakeResponse hndResp) throws IgniteCheckedException, IOException {
                return hadoop.create(path, overwrite, colocate, replication, blockSize, props);
            }
        }, path);
    }

    /** {@inheritDoc} */
    @Override public IgfsHadoopStreamDelegate append(final IgniteFsPath path, final boolean create,
        @Nullable final Map<String, String> props) throws IOException {
        return withReconnectHandling(new FileSystemClosure<IgfsHadoopStreamDelegate>() {
            @Override public IgfsHadoopStreamDelegate apply(IgfsHadoopEx hadoop,
                IgfsHandshakeResponse hndResp) throws IgniteCheckedException, IOException {
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
    private <T> T withReconnectHandling(final FileSystemClosure<T> clo, @Nullable IgniteFsPath path)
        throws IOException {
        Exception err = null;

        for (int i = 0; i < 2; i++) {
            Delegate curDelegate = null;

            boolean close = false;
            boolean force = false;

            try {
                curDelegate = delegate();

                assert curDelegate != null;

                close = curDelegate.doomed;

                return clo.apply(curDelegate.hadoop, curDelegate.hndResp);
            }
            catch (IgfsHadoopCommunicationException e) {
                if (curDelegate != null && !curDelegate.doomed) {
                    // Try getting rid fo faulty delegate ASAP.
                    delegateRef.compareAndSet(curDelegate, null);

                    close = true;
                    force = true;
                }

                if (log.isDebugEnabled())
                    log.debug("Failed to send message to a server: " + e);

                err = e;
            }
            catch (IgniteCheckedException e) {
                throw IgfsHadoopUtils.cast(e, path != null ? path.toString() : null);
            }
            finally {
                if (close) {
                    assert curDelegate != null;

                    curDelegate.close(force);
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
    private Delegate delegate() throws IgfsHadoopCommunicationException {
        Exception err = null;

        // 1. If delegate is set, return it immediately.
        Delegate curDelegate = delegateRef.get();

        if (curDelegate != null)
            return curDelegate;

        // 2. Guess that we are in the same VM.
        if (!parameter(conf, PARAM_GGFS_ENDPOINT_NO_EMBED, authority, false)) {
            IgfsEx ggfs = null;

            if (endpoint.grid() == null) {
                try {
                    Ignite ignite = G.ignite();

                    ggfs = (IgfsEx)ignite.fileSystem(endpoint.ggfs());
                }
                catch (Exception e) {
                    err = e;
                }
            }
            else {
                for (Ignite ignite : G.allGrids()) {
                    try {
                        ggfs = (IgfsEx)ignite.fileSystem(endpoint.ggfs());

                        break;
                    }
                    catch (Exception e) {
                        err = e;
                    }
                }
            }

            if (ggfs != null) {
                IgfsHadoopEx hadoop = null;

                try {
                    hadoop = new IgfsHadoopInProc(ggfs, log);

                    curDelegate = new Delegate(hadoop, hadoop.handshake(logDir));
                }
                catch (IOException | IgniteCheckedException e) {
                    if (e instanceof IgfsHadoopCommunicationException)
                        hadoop.close(true);

                    if (log.isDebugEnabled())
                        log.debug("Failed to connect to in-proc GGFS, fallback to IPC mode.", e);

                    err = e;
                }
            }
        }

        // 3. Try connecting using shmem.
        if (!parameter(conf, PARAM_GGFS_ENDPOINT_NO_LOCAL_SHMEM, authority, false)) {
            if (curDelegate == null && !U.isWindows()) {
                IgfsHadoopEx hadoop = null;

                try {
                    hadoop = new IgfsHadoopOutProc(endpoint.port(), endpoint.grid(), endpoint.ggfs(), log);

                    curDelegate = new Delegate(hadoop, hadoop.handshake(logDir));
                }
                catch (IOException | IgniteCheckedException e) {
                    if (e instanceof IgfsHadoopCommunicationException)
                        hadoop.close(true);

                    if (log.isDebugEnabled())
                        log.debug("Failed to connect to out-proc local GGFS using shmem.", e);

                    err = e;
                }
            }
        }

        // 4. Try local TCP connection.
        boolean skipLocTcp = parameter(conf, PARAM_GGFS_ENDPOINT_NO_LOCAL_TCP, authority, false);

        if (!skipLocTcp) {
            if (curDelegate == null) {
                IgfsHadoopEx hadoop = null;

                try {
                    hadoop = new IgfsHadoopOutProc(LOCALHOST, endpoint.port(), endpoint.grid(), endpoint.ggfs(),
                        log);

                    curDelegate = new Delegate(hadoop, hadoop.handshake(logDir));
                }
                catch (IOException | IgniteCheckedException e) {
                    if (e instanceof IgfsHadoopCommunicationException)
                        hadoop.close(true);

                    if (log.isDebugEnabled())
                        log.debug("Failed to connect to out-proc local GGFS using TCP.", e);

                    err = e;
                }
            }
        }

        // 5. Try remote TCP connection.
        if (curDelegate == null && (skipLocTcp || !F.eq(LOCALHOST, endpoint.host()))) {
            IgfsHadoopEx hadoop = null;

            try {
                hadoop = new IgfsHadoopOutProc(endpoint.host(), endpoint.port(), endpoint.grid(), endpoint.ggfs(), log);

                curDelegate = new Delegate(hadoop, hadoop.handshake(logDir));
            }
            catch (IOException | IgniteCheckedException e) {
                if (e instanceof IgfsHadoopCommunicationException)
                    hadoop.close(true);

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
            throw new IgfsHadoopCommunicationException("Failed to connect to GGFS: " + endpoint, err);
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
         * @throws IgniteCheckedException If failed.
         * @throws IOException If failed.
         */
        public T apply(IgfsHadoopEx hadoop, IgfsHandshakeResponse hndResp) throws IgniteCheckedException, IOException;
    }

    /**
     * Delegate.
     */
    private static class Delegate {
        /** RPC handler. */
        private final IgfsHadoopEx hadoop;

        /** Handshake request. */
        private final IgfsHandshakeResponse hndResp;

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
        private Delegate(IgfsHadoopEx hadoop, IgfsHandshakeResponse hndResp) {
            this.hadoop = hadoop;
            this.hndResp = hndResp;
        }

        /**
         * Close underlying RPC handler.
         *
         * @param force Force flag.
         */
        private void close(boolean force) {
            if (closeGuard.compareAndSet(false, true))
                hadoop.close(force);
        }
    }
}
