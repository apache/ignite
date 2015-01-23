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
import org.apache.ignite.*;
import org.apache.ignite.fs.*;
import org.apache.ignite.internal.fs.common.*;
import org.apache.ignite.internal.processors.fs.*;
import org.apache.ignite.internal.util.lang.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static org.apache.ignite.internal.fs.common.GridGgfsIpcCommand.*;

/**
 * Communication with external process (TCP or shmem).
 */
public class GridGgfsHadoopOutProc implements GridGgfsHadoopEx, GridGgfsHadoopIpcIoListener {
    /** Expected result is boolean. */
    private static final GridPlainClosure<GridPlainFuture<GridGgfsMessage>, Boolean> BOOL_RES = createClosure();

    /** Expected result is boolean. */
    private static final GridPlainClosure<GridPlainFuture<GridGgfsMessage>, Long> LONG_RES = createClosure();

    /** Expected result is {@code GridGgfsFile}. */
    private static final GridPlainClosure<GridPlainFuture<GridGgfsMessage>, IgniteFsFile> FILE_RES = createClosure();

    /** Expected result is {@code GridGgfsHandshakeResponse} */
    private static final GridPlainClosure<GridPlainFuture<GridGgfsMessage>,
        GridGgfsHandshakeResponse> HANDSHAKE_RES = createClosure();

    /** Expected result is {@code GridGgfsStatus} */
    private static final GridPlainClosure<GridPlainFuture<GridGgfsMessage>, GridGgfsStatus> STATUS_RES =
        createClosure();

    /** Expected result is {@code GridGgfsFile}. */
    private static final GridPlainClosure<GridPlainFuture<GridGgfsMessage>,
        GridGgfsInputStreamDescriptor> STREAM_DESCRIPTOR_RES = createClosure();

    /** Expected result is {@code GridGgfsFile}. */
    private static final GridPlainClosure<GridPlainFuture<GridGgfsMessage>,
        Collection<IgniteFsFile>> FILE_COL_RES = createClosure();

    /** Expected result is {@code GridGgfsFile}. */
    private static final GridPlainClosure<GridPlainFuture<GridGgfsMessage>,
        Collection<IgniteFsPath>> PATH_COL_RES = createClosure();

    /** Expected result is {@code GridGgfsPathSummary}. */
    private static final GridPlainClosure<GridPlainFuture<GridGgfsMessage>, IgniteFsPathSummary> SUMMARY_RES =
        createClosure();

    /** Expected result is {@code GridGgfsFile}. */
    private static final GridPlainClosure<GridPlainFuture<GridGgfsMessage>,
        Collection<IgniteFsBlockLocation>> BLOCK_LOCATION_COL_RES = createClosure();

    /** Grid name. */
    private final String grid;

    /** GGFS name. */
    private final String ggfs;

    /** Client log. */
    private final Log log;

    /** Client IO. */
    private final GridGgfsHadoopIpcIo io;

    /** Event listeners. */
    private final Map<Long, GridGgfsHadoopStreamEventListener> lsnrs = new ConcurrentHashMap8<>();

    /**
     * Constructor for TCP endpoint.
     *
     * @param host Host.
     * @param port Port.
     * @param grid Grid name.
     * @param ggfs GGFS name.
     * @param log Client logger.
     * @throws IOException If failed.
     */
    public GridGgfsHadoopOutProc(String host, int port, String grid, String ggfs, Log log) throws IOException {
        this(host, port, grid, ggfs, false, log);
    }

    /**
     * Constructor for shmem endpoint.
     *
     * @param port Port.
     * @param grid Grid name.
     * @param ggfs GGFS name.
     * @param log Client logger.
     * @throws IOException If failed.
     */
    public GridGgfsHadoopOutProc(int port, String grid, String ggfs, Log log) throws IOException {
        this(null, port, grid, ggfs, true, log);
    }

    /**
     * Constructor.
     *
     * @param host Host.
     * @param port Port.
     * @param grid Grid name.
     * @param ggfs GGFS name.
     * @param shmem Shared memory flag.
     * @param log Client logger.
     * @throws IOException If failed.
     */
    private GridGgfsHadoopOutProc(String host, int port, String grid, String ggfs, boolean shmem, Log log)
        throws IOException {
        assert host != null && !shmem || host == null && shmem :
            "Invalid arguments [host=" + host + ", port=" + port + ", shmem=" + shmem + ']';

        String endpoint = host != null ? host + ":" + port : "shmem:" + port;

        this.grid = grid;
        this.ggfs = ggfs;
        this.log = log;

        io = GridGgfsHadoopIpcIo.get(log, endpoint);

        io.addEventListener(this);
    }

    /** {@inheritDoc} */
    @Override public GridGgfsHandshakeResponse handshake(String logDir) throws IgniteCheckedException {
        final GridGgfsHandshakeRequest req = new GridGgfsHandshakeRequest();

        req.gridName(grid);
        req.ggfsName(ggfs);
        req.logDirectory(logDir);

        return io.send(req).chain(HANDSHAKE_RES).get();
    }

    /** {@inheritDoc} */
    @Override public void close(boolean force) {
        assert io != null;

        io.removeEventListener(this);

        if (force)
            io.forceClose();
        else
            io.release();
    }

    /** {@inheritDoc} */
    @Override public IgniteFsFile info(IgniteFsPath path) throws IgniteCheckedException {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(INFO);
        msg.path(path);

        return io.send(msg).chain(FILE_RES).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteFsFile update(IgniteFsPath path, Map<String, String> props) throws IgniteCheckedException {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(UPDATE);
        msg.path(path);
        msg.properties(props);

        return io.send(msg).chain(FILE_RES).get();
    }

    /** {@inheritDoc} */
    @Override public Boolean setTimes(IgniteFsPath path, long accessTime, long modificationTime) throws IgniteCheckedException {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(SET_TIMES);
        msg.path(path);
        msg.accessTime(accessTime);
        msg.modificationTime(modificationTime);

        return io.send(msg).chain(BOOL_RES).get();
    }

    /** {@inheritDoc} */
    @Override public Boolean rename(IgniteFsPath src, IgniteFsPath dest) throws IgniteCheckedException {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(RENAME);
        msg.path(src);
        msg.destinationPath(dest);

        return io.send(msg).chain(BOOL_RES).get();
    }

    /** {@inheritDoc} */
    @Override public Boolean delete(IgniteFsPath path, boolean recursive) throws IgniteCheckedException {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(DELETE);
        msg.path(path);
        msg.flag(recursive);

        return io.send(msg).chain(BOOL_RES).get();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFsBlockLocation> affinity(IgniteFsPath path, long start, long len)
        throws IgniteCheckedException {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(AFFINITY);
        msg.path(path);
        msg.start(start);
        msg.length(len);

        return io.send(msg).chain(BLOCK_LOCATION_COL_RES).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteFsPathSummary contentSummary(IgniteFsPath path) throws IgniteCheckedException {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(PATH_SUMMARY);
        msg.path(path);

        return io.send(msg).chain(SUMMARY_RES).get();
    }

    /** {@inheritDoc} */
    @Override public Boolean mkdirs(IgniteFsPath path, Map<String, String> props) throws IgniteCheckedException {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(MAKE_DIRECTORIES);
        msg.path(path);
        msg.properties(props);

        return io.send(msg).chain(BOOL_RES).get();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFsFile> listFiles(IgniteFsPath path) throws IgniteCheckedException {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(LIST_FILES);
        msg.path(path);

        return io.send(msg).chain(FILE_COL_RES).get();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFsPath> listPaths(IgniteFsPath path) throws IgniteCheckedException {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(LIST_PATHS);
        msg.path(path);

        return io.send(msg).chain(PATH_COL_RES).get();
    }

    /** {@inheritDoc} */
    @Override public GridGgfsStatus fsStatus() throws IgniteCheckedException {
        return io.send(new GridGgfsStatusRequest()).chain(STATUS_RES).get();
    }

    /** {@inheritDoc} */
    @Override public GridGgfsHadoopStreamDelegate open(IgniteFsPath path) throws IgniteCheckedException {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(OPEN_READ);
        msg.path(path);
        msg.flag(false);

        GridGgfsInputStreamDescriptor rmtDesc = io.send(msg).chain(STREAM_DESCRIPTOR_RES).get();

        return new GridGgfsHadoopStreamDelegate(this, rmtDesc.streamId(), rmtDesc.length());
    }

    /** {@inheritDoc} */
    @Override public GridGgfsHadoopStreamDelegate open(IgniteFsPath path,
        int seqReadsBeforePrefetch) throws IgniteCheckedException {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(OPEN_READ);
        msg.path(path);
        msg.flag(true);
        msg.sequentialReadsBeforePrefetch(seqReadsBeforePrefetch);

        GridGgfsInputStreamDescriptor rmtDesc = io.send(msg).chain(STREAM_DESCRIPTOR_RES).get();

        return new GridGgfsHadoopStreamDelegate(this, rmtDesc.streamId(), rmtDesc.length());
    }

    /** {@inheritDoc} */
    @Override public GridGgfsHadoopStreamDelegate create(IgniteFsPath path, boolean overwrite, boolean colocate,
        int replication, long blockSize, @Nullable Map<String, String> props) throws IgniteCheckedException {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(OPEN_CREATE);
        msg.path(path);
        msg.flag(overwrite);
        msg.colocate(colocate);
        msg.properties(props);
        msg.replication(replication);
        msg.blockSize(blockSize);

        Long streamId = io.send(msg).chain(LONG_RES).get();

        return new GridGgfsHadoopStreamDelegate(this, streamId);
    }

    /** {@inheritDoc} */
    @Override public GridGgfsHadoopStreamDelegate append(IgniteFsPath path, boolean create,
        @Nullable Map<String, String> props) throws IgniteCheckedException {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(OPEN_APPEND);
        msg.path(path);
        msg.flag(create);
        msg.properties(props);

        Long streamId = io.send(msg).chain(LONG_RES).get();

        return new GridGgfsHadoopStreamDelegate(this, streamId);
    }

    /** {@inheritDoc} */
    @Override public GridPlainFuture<byte[]> readData(GridGgfsHadoopStreamDelegate desc, long pos, int len,
        final @Nullable byte[] outBuf, final int outOff, final int outLen) {
        assert len > 0;

        final GridGgfsStreamControlRequest msg = new GridGgfsStreamControlRequest();

        msg.command(READ_BLOCK);
        msg.streamId((long) desc.target());
        msg.position(pos);
        msg.length(len);

        try {
            return io.send(msg, outBuf, outOff, outLen);
        }
        catch (IgniteCheckedException e) {
            return new GridPlainFutureAdapter<>(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeData(GridGgfsHadoopStreamDelegate desc, byte[] data, int off, int len)
        throws IOException {
        final GridGgfsStreamControlRequest msg = new GridGgfsStreamControlRequest();

        msg.command(WRITE_BLOCK);
        msg.streamId((long) desc.target());
        msg.data(data);
        msg.position(off);
        msg.length(len);

        try {
            io.sendPlain(msg);
        }
        catch (IgniteCheckedException e) {
            throw GridGgfsHadoopUtils.cast(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void flush(GridGgfsHadoopStreamDelegate delegate) throws IOException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void closeStream(GridGgfsHadoopStreamDelegate desc) throws IOException {
        final GridGgfsStreamControlRequest msg = new GridGgfsStreamControlRequest();

        msg.command(CLOSE);
        msg.streamId((long)desc.target());

        try {
            io.send(msg).chain(BOOL_RES).get();
        }
        catch (IgniteCheckedException e) {
            throw GridGgfsHadoopUtils.cast(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void addEventListener(GridGgfsHadoopStreamDelegate desc,
        GridGgfsHadoopStreamEventListener lsnr) {
        long streamId = desc.target();

        GridGgfsHadoopStreamEventListener lsnr0 = lsnrs.put(streamId, lsnr);

        assert lsnr0 == null || lsnr0 == lsnr;

        if (log.isDebugEnabled())
            log.debug("Added stream event listener [streamId=" + streamId + ']');
    }

    /** {@inheritDoc} */
    @Override public void removeEventListener(GridGgfsHadoopStreamDelegate desc) {
        long streamId = desc.target();

        GridGgfsHadoopStreamEventListener lsnr0 = lsnrs.remove(streamId);

        if (lsnr0 != null && log.isDebugEnabled())
            log.debug("Removed stream event listener [streamId=" + streamId + ']');
    }

    /** {@inheritDoc} */
    @Override public void onClose() {
        for (GridGgfsHadoopStreamEventListener lsnr : lsnrs.values()) {
            try {
                lsnr.onClose();
            }
            catch (IgniteCheckedException e) {
                log.warn("Got exception from stream event listener (will ignore): " + lsnr, e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onError(long streamId, String errMsg) {
        GridGgfsHadoopStreamEventListener lsnr = lsnrs.get(streamId);

        if (lsnr != null)
            lsnr.onError(errMsg);
        else
            log.warn("Received write error response for not registered output stream (will ignore) " +
                "[streamId= " + streamId + ']');
    }

    /**
     * Creates conversion closure for given type.
     *
     * @param <T> Type of expected result.
     * @return Conversion closure.
     */
    @SuppressWarnings("unchecked")
    private static <T> GridPlainClosure<GridPlainFuture<GridGgfsMessage>, T> createClosure() {
        return new GridPlainClosure<GridPlainFuture<GridGgfsMessage>, T>() {
            @Override public T apply(GridPlainFuture<GridGgfsMessage> fut) throws IgniteCheckedException {
                GridGgfsControlResponse res = (GridGgfsControlResponse)fut.get();

                if (res.hasError())
                    res.throwError();

                return (T)res.response();
            }
        };
    }
}
