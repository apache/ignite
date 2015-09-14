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

package org.apache.ignite.internal.processors.hadoop.igfs;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.igfs.IgfsBlockLocation;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.IgfsPathSummary;
import org.apache.ignite.internal.processors.igfs.IgfsEx;
import org.apache.ignite.internal.processors.igfs.IgfsHandshakeResponse;
import org.apache.ignite.internal.processors.igfs.IgfsStatus;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.hadoop.igfs.HadoopIgfsEndpoint.LOCALHOST;
import static org.apache.ignite.internal.processors.hadoop.igfs.HadoopIgfsUtils.PARAM_IGFS_ENDPOINT_NO_EMBED;
import static org.apache.ignite.internal.processors.hadoop.igfs.HadoopIgfsUtils.PARAM_IGFS_ENDPOINT_NO_LOCAL_SHMEM;
import static org.apache.ignite.internal.processors.hadoop.igfs.HadoopIgfsUtils.PARAM_IGFS_ENDPOINT_NO_LOCAL_TCP;
import static org.apache.ignite.internal.processors.hadoop.igfs.HadoopIgfsUtils.parameter;

/**
 * Wrapper for IGFS server.
 */
public class HadoopIgfsWrapper implements HadoopIgfs {
    /** Delegate. */
    private final AtomicReference<Delegate> delegateRef = new AtomicReference<>();

    /** Authority. */
    private final String authority;

    /** Connection string. */
    private final HadoopIgfsEndpoint endpoint;

    /** Log directory. */
    private final String logDir;

    /** Configuration. */
    private final Configuration conf;

    /** Logger. */
    private final Log log;

    /** The user name this wrapper works on behalf of. */
    private final String userName;

    /**
     * Constructor.
     *
     * @param authority Authority (connection string).
     * @param logDir Log directory for server.
     * @param conf Configuration.
     * @param log Current logger.
     */
    public HadoopIgfsWrapper(String authority, String logDir, Configuration conf, Log log, String user)
            throws IOException {
        try {
            this.authority = authority;
            this.endpoint = new HadoopIgfsEndpoint(authority);
            this.logDir = logDir;
            this.conf = conf;
            this.log = log;
            this.userName = user;
        }
        catch (IgniteCheckedException e) {
            throw new IOException("Failed to parse endpoint: " + authority, e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgfsHandshakeResponse handshake(String logDir) throws IOException {
        return withReconnectHandling(new FileSystemClosure<IgfsHandshakeResponse>() {
            @Override public IgfsHandshakeResponse apply(HadoopIgfsEx hadoop,
                IgfsHandshakeResponse hndResp) {
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
    @Override public IgfsFile info(final IgfsPath path) throws IOException {
        return withReconnectHandling(new FileSystemClosure<IgfsFile>() {
            @Override public IgfsFile apply(HadoopIgfsEx hadoop, IgfsHandshakeResponse hndResp)
                throws IgniteCheckedException, IOException {
                return hadoop.info(path);
            }
        }, path);
    }

    /** {@inheritDoc} */
    @Override public IgfsFile update(final IgfsPath path, final Map<String, String> props) throws IOException {
        return withReconnectHandling(new FileSystemClosure<IgfsFile>() {
            @Override public IgfsFile apply(HadoopIgfsEx hadoop, IgfsHandshakeResponse hndResp)
                throws IgniteCheckedException, IOException {
                return hadoop.update(path, props);
            }
        }, path);
    }

    /** {@inheritDoc} */
    @Override public Boolean setTimes(final IgfsPath path, final long accessTime, final long modificationTime)
        throws IOException {
        return withReconnectHandling(new FileSystemClosure<Boolean>() {
            @Override public Boolean apply(HadoopIgfsEx hadoop, IgfsHandshakeResponse hndResp)
                throws IgniteCheckedException, IOException {
                return hadoop.setTimes(path, accessTime, modificationTime);
            }
        }, path);
    }

    /** {@inheritDoc} */
    @Override public Boolean rename(final IgfsPath src, final IgfsPath dest) throws IOException {
        return withReconnectHandling(new FileSystemClosure<Boolean>() {
            @Override public Boolean apply(HadoopIgfsEx hadoop, IgfsHandshakeResponse hndResp)
                throws IgniteCheckedException, IOException {
                return hadoop.rename(src, dest);
            }
        }, src);
    }

    /** {@inheritDoc} */
    @Override public Boolean delete(final IgfsPath path, final boolean recursive) throws IOException {
        return withReconnectHandling(new FileSystemClosure<Boolean>() {
            @Override public Boolean apply(HadoopIgfsEx hadoop, IgfsHandshakeResponse hndResp)
                throws IgniteCheckedException, IOException {
                return hadoop.delete(path, recursive);
            }
        }, path);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsBlockLocation> affinity(final IgfsPath path, final long start,
        final long len) throws IOException {
        return withReconnectHandling(new FileSystemClosure<Collection<IgfsBlockLocation>>() {
            @Override public Collection<IgfsBlockLocation> apply(HadoopIgfsEx hadoop,
                IgfsHandshakeResponse hndResp) throws IgniteCheckedException, IOException {
                return hadoop.affinity(path, start, len);
            }
        }, path);
    }

    /** {@inheritDoc} */
    @Override public IgfsPathSummary contentSummary(final IgfsPath path) throws IOException {
        return withReconnectHandling(new FileSystemClosure<IgfsPathSummary>() {
            @Override public IgfsPathSummary apply(HadoopIgfsEx hadoop, IgfsHandshakeResponse hndResp)
                throws IgniteCheckedException, IOException {
                return hadoop.contentSummary(path);
            }
        }, path);
    }

    /** {@inheritDoc} */
    @Override public Boolean mkdirs(final IgfsPath path, final Map<String, String> props) throws IOException {
        return withReconnectHandling(new FileSystemClosure<Boolean>() {
            @Override public Boolean apply(HadoopIgfsEx hadoop, IgfsHandshakeResponse hndResp)
                throws IgniteCheckedException, IOException {
                return hadoop.mkdirs(path, props);
            }
        }, path);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsFile> listFiles(final IgfsPath path) throws IOException {
        return withReconnectHandling(new FileSystemClosure<Collection<IgfsFile>>() {
            @Override public Collection<IgfsFile> apply(HadoopIgfsEx hadoop,
                IgfsHandshakeResponse hndResp) throws IgniteCheckedException, IOException {
                return hadoop.listFiles(path);
            }
        }, path);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsPath> listPaths(final IgfsPath path) throws IOException {
        return withReconnectHandling(new FileSystemClosure<Collection<IgfsPath>>() {
            @Override public Collection<IgfsPath> apply(HadoopIgfsEx hadoop,
                IgfsHandshakeResponse hndResp) throws IgniteCheckedException, IOException {
                return hadoop.listPaths(path);
            }
        }, path);
    }

    /** {@inheritDoc} */
    @Override public IgfsStatus fsStatus() throws IOException {
        return withReconnectHandling(new FileSystemClosure<IgfsStatus>() {
            @Override public IgfsStatus apply(HadoopIgfsEx hadoop, IgfsHandshakeResponse hndResp)
                throws IgniteCheckedException, IOException {
                return hadoop.fsStatus();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public HadoopIgfsStreamDelegate open(final IgfsPath path) throws IOException {
        return withReconnectHandling(new FileSystemClosure<HadoopIgfsStreamDelegate>() {
            @Override public HadoopIgfsStreamDelegate apply(HadoopIgfsEx hadoop,
                IgfsHandshakeResponse hndResp) throws IgniteCheckedException, IOException {
                return hadoop.open(path);
            }
        }, path);
    }

    /** {@inheritDoc} */
    @Override public HadoopIgfsStreamDelegate open(final IgfsPath path, final int seqReadsBeforePrefetch)
        throws IOException {
        return withReconnectHandling(new FileSystemClosure<HadoopIgfsStreamDelegate>() {
            @Override public HadoopIgfsStreamDelegate apply(HadoopIgfsEx hadoop,
                IgfsHandshakeResponse hndResp) throws IgniteCheckedException, IOException {
                return hadoop.open(path, seqReadsBeforePrefetch);
            }
        }, path);
    }

    /** {@inheritDoc} */
    @Override public HadoopIgfsStreamDelegate create(final IgfsPath path, final boolean overwrite,
        final boolean colocate, final int replication, final long blockSize, @Nullable final Map<String, String> props)
        throws IOException {
        return withReconnectHandling(new FileSystemClosure<HadoopIgfsStreamDelegate>() {
            @Override public HadoopIgfsStreamDelegate apply(HadoopIgfsEx hadoop,
                IgfsHandshakeResponse hndResp) throws IgniteCheckedException, IOException {
                return hadoop.create(path, overwrite, colocate, replication, blockSize, props);
            }
        }, path);
    }

    /** {@inheritDoc} */
    @Override public HadoopIgfsStreamDelegate append(final IgfsPath path, final boolean create,
        @Nullable final Map<String, String> props) throws IOException {
        return withReconnectHandling(new FileSystemClosure<HadoopIgfsStreamDelegate>() {
            @Override public HadoopIgfsStreamDelegate apply(HadoopIgfsEx hadoop,
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
    private <T> T withReconnectHandling(final FileSystemClosure<T> clo, @Nullable IgfsPath path)
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
            catch (HadoopIgfsCommunicationException e) {
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
                throw HadoopIgfsUtils.cast(e, path != null ? path.toString() : null);
            }
            finally {
                if (close) {
                    assert curDelegate != null;

                    curDelegate.close(force);
                }
            }
        }

        throw new IOException("Failed to communicate with IGFS.", err);
    }

    /**
     * Get delegate creating it if needed.
     *
     * @return Delegate.
     */
    private Delegate delegate() throws HadoopIgfsCommunicationException {
        // 1. If delegate is set, return it immediately.
        Delegate curDelegate = delegateRef.get();

        if (curDelegate != null)
            return curDelegate;

        final List<IgniteBiTuple<String,Exception>> errors = new ArrayList<>(5);

        // 2. Guess that we are in the same VM.
        try {
            curDelegate = inproc();
        }
        catch (Exception e) {
            errors.add(new IgniteBiTuple<>("in-proc", e));
        }

        // 3. Try connecting using shmem.
        if (curDelegate == null) {
            try {
                curDelegate = shmem();
            }
            catch (Exception e) {
                errors.add(new IgniteBiTuple<>("shmem", e));
            }
        }

        boolean skipLocTcp = parameter(conf, PARAM_IGFS_ENDPOINT_NO_LOCAL_TCP, authority, false);

        // 4. Try local TCP connection.
        if (curDelegate == null) {
            try {
                curDelegate = localTcp(skipLocTcp);
            }
            catch (Exception e) {
                errors.add(new IgniteBiTuple<>("local TCP", e));
            }
        }

        // 5. Try remote TCP connection.
        if (curDelegate == null) {
            try {
                curDelegate = remoteTcp(skipLocTcp);
            }
            catch (Exception e) {
                errors.add(new IgniteBiTuple<>("remote TCP", e));
            }
        }

        if (curDelegate != null) {
            if (!delegateRef.compareAndSet(null, curDelegate))
                curDelegate.doomed = true;

            return curDelegate;
        }
        else {
            StringBuilder sb = new StringBuilder("Failed to connect to IGFS: ")
                .append(endpoint);

            Exception err = null;

            if (errors.isEmpty())
                sb.append("No communication method enabled to communicate with IGFS.");
            else {
                for (IgniteBiTuple<String, Exception> t2 : errors) {
                    sb.append("Communication method ")
                        .append(t2.get1())
                        .append(" failed: ")
                        .append(t2.get2())
                        .append("\n");
                }

                err = errors.get(errors.size() - 1).get2();
            }

            throw new HadoopIgfsCommunicationException(sb.toString(), err);
        }
    }

    /**
     * In-proc method.
     *
     * @return The delegate
     * @throws Exception On error.
     */
    private Delegate inproc() throws Exception {
        if (!parameter(conf, PARAM_IGFS_ENDPOINT_NO_EMBED, authority, false)) {
            IgfsEx igfs = null;

            if (endpoint.grid() == null) {
                Ignite ignite = G.ignite();

                igfs = (IgfsEx)ignite.fileSystem(endpoint.igfs());
            }
            else {
                for (Ignite ignite : G.allGrids()) {
                    igfs = (IgfsEx)ignite.fileSystem(endpoint.igfs());

                    break;
                }
            }

            if (igfs != null) {
                HadoopIgfsEx hadoop = null;

                try {
                    hadoop = new HadoopIgfsInProc(igfs, log, userName);

                    return new Delegate(hadoop, hadoop.handshake(logDir));
                }
                catch (IOException | IgniteCheckedException e) {
                    if (e instanceof HadoopIgfsCommunicationException)
                        if (hadoop != null)
                            hadoop.close(true);

                    if (log.isDebugEnabled())
                        log.debug("Failed to connect to in-proc IGFS, fallback to IPC mode.", e);

                    throw e;
                }
            }
        }

        return null;
    }

    /**
     * Shmem method
     *
     * @return The delegate
     * @throws Exception On error.
     */
    private Delegate shmem() throws Exception {
        if (!parameter(conf, PARAM_IGFS_ENDPOINT_NO_LOCAL_SHMEM, authority, false)) {
            if (!U.isWindows()) {
                HadoopIgfsEx hadoop = null;

                try {
                    hadoop = new HadoopIgfsOutProc(endpoint.port(), endpoint.grid(), endpoint.igfs(), log, userName);

                    return new Delegate(hadoop, hadoop.handshake(logDir));
                }
                catch (IOException | IgniteCheckedException e) {
                    if (e instanceof HadoopIgfsCommunicationException)
                        hadoop.close(true);

                    if (log.isDebugEnabled())
                        log.debug("Failed to connect to out-proc local IGFS using shmem.", e);

                    throw e;
                }
            }
        }

        return null;
    }

    /**
     * Local TCP method
     *
     * @return The delegate
     * @throws Exception On error.
     */
    private Delegate localTcp(boolean skipLocTcp) throws Exception {
        if (!skipLocTcp) {
                HadoopIgfsEx hadoop = null;

                try {
                    hadoop = new HadoopIgfsOutProc(LOCALHOST, endpoint.port(), endpoint.grid(), endpoint.igfs(),
                        log, userName);

                    return new Delegate(hadoop, hadoop.handshake(logDir));
                }
                catch (IOException | IgniteCheckedException e) {
                    if (e instanceof HadoopIgfsCommunicationException)
                        hadoop.close(true);

                    if (log.isDebugEnabled())
                        log.debug("Failed to connect to out-proc local IGFS using TCP.", e);

                    throw e;
                }
        }

        return null;
    }

    /**
     * Remote TCP method.
     *
     * @return The delegate
     * @throws Exception On error.
     */
    private Delegate remoteTcp(boolean skipLocTcp)  throws Exception {
        if (skipLocTcp || !F.eq(LOCALHOST, endpoint.host())) {
            HadoopIgfsEx hadoop = null;

            try {
                hadoop = new HadoopIgfsOutProc(endpoint.host(), endpoint.port(), endpoint.grid(), endpoint.igfs(),
                    log, userName);

                return new Delegate(hadoop, hadoop.handshake(logDir));
            }
            catch (IOException | IgniteCheckedException e) {
                if (e instanceof HadoopIgfsCommunicationException)
                    hadoop.close(true);

                if (log.isDebugEnabled())
                    log.debug("Failed to connect to out-proc remote IGFS using TCP.", e);

                throw e;
            }
        }

        return null;
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
        public T apply(HadoopIgfsEx hadoop, IgfsHandshakeResponse hndResp) throws IgniteCheckedException, IOException;
    }

    /**
     * Delegate.
     */
    private static class Delegate {
        /** RPC handler. */
        private final HadoopIgfsEx hadoop;

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
        private Delegate(HadoopIgfsEx hadoop, IgfsHandshakeResponse hndResp) {
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