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
import java.util.Collection;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.igfs.IgfsBlockLocation;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.IgfsPathSummary;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.igfs.common.IgfsControlResponse;
import org.apache.ignite.internal.igfs.common.IgfsHandshakeRequest;
import org.apache.ignite.internal.igfs.common.IgfsMessage;
import org.apache.ignite.internal.igfs.common.IgfsPathControlRequest;
import org.apache.ignite.internal.igfs.common.IgfsStatusRequest;
import org.apache.ignite.internal.igfs.common.IgfsStreamControlRequest;
import org.apache.ignite.internal.processors.igfs.IgfsHandshakeResponse;
import org.apache.ignite.internal.processors.igfs.IgfsInputStreamDescriptor;
import org.apache.ignite.internal.processors.igfs.IgfsStatus;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.lang.IgniteClosure;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.internal.igfs.common.IgfsIpcCommand.AFFINITY;
import static org.apache.ignite.internal.igfs.common.IgfsIpcCommand.CLOSE;
import static org.apache.ignite.internal.igfs.common.IgfsIpcCommand.DELETE;
import static org.apache.ignite.internal.igfs.common.IgfsIpcCommand.INFO;
import static org.apache.ignite.internal.igfs.common.IgfsIpcCommand.LIST_FILES;
import static org.apache.ignite.internal.igfs.common.IgfsIpcCommand.LIST_PATHS;
import static org.apache.ignite.internal.igfs.common.IgfsIpcCommand.MAKE_DIRECTORIES;
import static org.apache.ignite.internal.igfs.common.IgfsIpcCommand.OPEN_APPEND;
import static org.apache.ignite.internal.igfs.common.IgfsIpcCommand.OPEN_CREATE;
import static org.apache.ignite.internal.igfs.common.IgfsIpcCommand.OPEN_READ;
import static org.apache.ignite.internal.igfs.common.IgfsIpcCommand.PATH_SUMMARY;
import static org.apache.ignite.internal.igfs.common.IgfsIpcCommand.READ_BLOCK;
import static org.apache.ignite.internal.igfs.common.IgfsIpcCommand.RENAME;
import static org.apache.ignite.internal.igfs.common.IgfsIpcCommand.SET_TIMES;
import static org.apache.ignite.internal.igfs.common.IgfsIpcCommand.UPDATE;
import static org.apache.ignite.internal.igfs.common.IgfsIpcCommand.WRITE_BLOCK;

/**
 * Communication with external process (TCP or shmem).
 */
public class HadoopIgfsOutProc implements HadoopIgfsEx, HadoopIgfsIpcIoListener {
    /** Expected result is boolean. */
    private static final IgniteClosure<IgniteInternalFuture<IgfsMessage>, Boolean> BOOL_RES = createClosure();

    /** Expected result is boolean. */
    private static final IgniteClosure<IgniteInternalFuture<IgfsMessage>, Long> LONG_RES = createClosure();

    /** Expected result is {@code IgfsFile}. */
    private static final IgniteClosure<IgniteInternalFuture<IgfsMessage>, IgfsFile> FILE_RES = createClosure();

    /** Expected result is {@code IgfsHandshakeResponse} */
    private static final IgniteClosure<IgniteInternalFuture<IgfsMessage>,
        IgfsHandshakeResponse> HANDSHAKE_RES = createClosure();

    /** Expected result is {@code IgfsStatus} */
    private static final IgniteClosure<IgniteInternalFuture<IgfsMessage>, IgfsStatus> STATUS_RES =
        createClosure();

    /** Expected result is {@code IgfsFile}. */
    private static final IgniteClosure<IgniteInternalFuture<IgfsMessage>,
        IgfsInputStreamDescriptor> STREAM_DESCRIPTOR_RES = createClosure();

    /** Expected result is {@code IgfsFile}. */
    private static final IgniteClosure<IgniteInternalFuture<IgfsMessage>,
        Collection<IgfsFile>> FILE_COL_RES = createClosure();

    /** Expected result is {@code IgfsFile}. */
    private static final IgniteClosure<IgniteInternalFuture<IgfsMessage>,
        Collection<IgfsPath>> PATH_COL_RES = createClosure();

    /** Expected result is {@code IgfsPathSummary}. */
    private static final IgniteClosure<IgniteInternalFuture<IgfsMessage>, IgfsPathSummary> SUMMARY_RES =
        createClosure();

    /** Expected result is {@code IgfsFile}. */
    private static final IgniteClosure<IgniteInternalFuture<IgfsMessage>,
        Collection<IgfsBlockLocation>> BLOCK_LOCATION_COL_RES = createClosure();

    /** Grid name. */
    private final String grid;

    /** IGFS name. */
    private final String igfs;

    /** The user this out proc is performing on behalf of. */
    private final String userName;

    /** Client log. */
    private final Log log;

    /** Client IO. */
    private final HadoopIgfsIpcIo io;

    /** Event listeners. */
    private final Map<Long, HadoopIgfsStreamEventListener> lsnrs = new ConcurrentHashMap8<>();

    /**
     * Constructor for TCP endpoint.
     *
     * @param host Host.
     * @param port Port.
     * @param grid Grid name.
     * @param igfs IGFS name.
     * @param log Client logger.
     * @throws IOException If failed.
     */
    public HadoopIgfsOutProc(String host, int port, String grid, String igfs, Log log, String user) throws IOException {
        this(host, port, grid, igfs, false, log, user);
    }

    /**
     * Constructor for shmem endpoint.
     *
     * @param port Port.
     * @param grid Grid name.
     * @param igfs IGFS name.
     * @param log Client logger.
     * @throws IOException If failed.
     */
    public HadoopIgfsOutProc(int port, String grid, String igfs, Log log, String user) throws IOException {
        this(null, port, grid, igfs, true, log, user);
    }

    /**
     * Constructor.
     *
     * @param host Host.
     * @param port Port.
     * @param grid Grid name.
     * @param igfs IGFS name.
     * @param shmem Shared memory flag.
     * @param log Client logger.
     * @throws IOException If failed.
     */
    private HadoopIgfsOutProc(String host, int port, String grid, String igfs, boolean shmem, Log log, String user)
        throws IOException {
        assert host != null && !shmem || host == null && shmem :
            "Invalid arguments [host=" + host + ", port=" + port + ", shmem=" + shmem + ']';

        String endpoint = host != null ? host + ":" + port : "shmem:" + port;

        this.grid = grid;
        this.igfs = igfs;
        this.log = log;
        this.userName = IgfsUtils.fixUserName(user);

        io = HadoopIgfsIpcIo.get(log, endpoint);

        io.addEventListener(this);
    }

    /** {@inheritDoc} */
    @Override public IgfsHandshakeResponse handshake(String logDir) throws IgniteCheckedException {
        final IgfsHandshakeRequest req = new IgfsHandshakeRequest();

        req.gridName(grid);
        req.igfsName(igfs);
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
    @Override public IgfsFile info(IgfsPath path) throws IgniteCheckedException {
        final IgfsPathControlRequest msg = new IgfsPathControlRequest();

        msg.command(INFO);
        msg.path(path);
        msg.userName(userName);

        return io.send(msg).chain(FILE_RES).get();
    }

    /** {@inheritDoc} */
    @Override public IgfsFile update(IgfsPath path, Map<String, String> props) throws IgniteCheckedException {
        final IgfsPathControlRequest msg = new IgfsPathControlRequest();

        msg.command(UPDATE);
        msg.path(path);
        msg.properties(props);
        msg.userName(userName);

        return io.send(msg).chain(FILE_RES).get();
    }

    /** {@inheritDoc} */
    @Override public Boolean setTimes(IgfsPath path, long accessTime, long modificationTime) throws IgniteCheckedException {
        final IgfsPathControlRequest msg = new IgfsPathControlRequest();

        msg.command(SET_TIMES);
        msg.path(path);
        msg.accessTime(accessTime);
        msg.modificationTime(modificationTime);
        msg.userName(userName);

        return io.send(msg).chain(BOOL_RES).get();
    }

    /** {@inheritDoc} */
    @Override public Boolean rename(IgfsPath src, IgfsPath dest) throws IgniteCheckedException {
        final IgfsPathControlRequest msg = new IgfsPathControlRequest();

        msg.command(RENAME);
        msg.path(src);
        msg.destinationPath(dest);
        msg.userName(userName);

        return io.send(msg).chain(BOOL_RES).get();
    }

    /** {@inheritDoc} */
    @Override public Boolean delete(IgfsPath path, boolean recursive) throws IgniteCheckedException {
        final IgfsPathControlRequest msg = new IgfsPathControlRequest();

        msg.command(DELETE);
        msg.path(path);
        msg.flag(recursive);
        msg.userName(userName);

        return io.send(msg).chain(BOOL_RES).get();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsBlockLocation> affinity(IgfsPath path, long start, long len)
        throws IgniteCheckedException {
        final IgfsPathControlRequest msg = new IgfsPathControlRequest();

        msg.command(AFFINITY);
        msg.path(path);
        msg.start(start);
        msg.length(len);
        msg.userName(userName);

        return io.send(msg).chain(BLOCK_LOCATION_COL_RES).get();
    }

    /** {@inheritDoc} */
    @Override public IgfsPathSummary contentSummary(IgfsPath path) throws IgniteCheckedException {
        final IgfsPathControlRequest msg = new IgfsPathControlRequest();

        msg.command(PATH_SUMMARY);
        msg.path(path);
        msg.userName(userName);

        return io.send(msg).chain(SUMMARY_RES).get();
    }

    /** {@inheritDoc} */
    @Override public Boolean mkdirs(IgfsPath path, Map<String, String> props) throws IgniteCheckedException {
        final IgfsPathControlRequest msg = new IgfsPathControlRequest();

        msg.command(MAKE_DIRECTORIES);
        msg.path(path);
        msg.properties(props);
        msg.userName(userName);

        return io.send(msg).chain(BOOL_RES).get();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsFile> listFiles(IgfsPath path) throws IgniteCheckedException {
        final IgfsPathControlRequest msg = new IgfsPathControlRequest();

        msg.command(LIST_FILES);
        msg.path(path);
        msg.userName(userName);

        return io.send(msg).chain(FILE_COL_RES).get();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsPath> listPaths(IgfsPath path) throws IgniteCheckedException {
        final IgfsPathControlRequest msg = new IgfsPathControlRequest();

        msg.command(LIST_PATHS);
        msg.path(path);
        msg.userName(userName);

        return io.send(msg).chain(PATH_COL_RES).get();
    }

    /** {@inheritDoc} */
    @Override public IgfsStatus fsStatus() throws IgniteCheckedException {
        return io.send(new IgfsStatusRequest()).chain(STATUS_RES).get();
    }

    /** {@inheritDoc} */
    @Override public HadoopIgfsStreamDelegate open(IgfsPath path) throws IgniteCheckedException {
        final IgfsPathControlRequest msg = new IgfsPathControlRequest();

        msg.command(OPEN_READ);
        msg.path(path);
        msg.flag(false);
        msg.userName(userName);

        IgfsInputStreamDescriptor rmtDesc = io.send(msg).chain(STREAM_DESCRIPTOR_RES).get();

        return new HadoopIgfsStreamDelegate(this, rmtDesc.streamId(), rmtDesc.length());
    }

    /** {@inheritDoc} */
    @Override public HadoopIgfsStreamDelegate open(IgfsPath path,
        int seqReadsBeforePrefetch) throws IgniteCheckedException {
        final IgfsPathControlRequest msg = new IgfsPathControlRequest();

        msg.command(OPEN_READ);
        msg.path(path);
        msg.flag(true);
        msg.sequentialReadsBeforePrefetch(seqReadsBeforePrefetch);
        msg.userName(userName);

        IgfsInputStreamDescriptor rmtDesc = io.send(msg).chain(STREAM_DESCRIPTOR_RES).get();

        return new HadoopIgfsStreamDelegate(this, rmtDesc.streamId(), rmtDesc.length());
    }

    /** {@inheritDoc} */
    @Override public HadoopIgfsStreamDelegate create(IgfsPath path, boolean overwrite, boolean colocate,
        int replication, long blockSize, @Nullable Map<String, String> props) throws IgniteCheckedException {
        final IgfsPathControlRequest msg = new IgfsPathControlRequest();

        msg.command(OPEN_CREATE);
        msg.path(path);
        msg.flag(overwrite);
        msg.colocate(colocate);
        msg.properties(props);
        msg.replication(replication);
        msg.blockSize(blockSize);
        msg.userName(userName);

        Long streamId = io.send(msg).chain(LONG_RES).get();

        return new HadoopIgfsStreamDelegate(this, streamId);
    }

    /** {@inheritDoc} */
    @Override public HadoopIgfsStreamDelegate append(IgfsPath path, boolean create,
        @Nullable Map<String, String> props) throws IgniteCheckedException {
        final IgfsPathControlRequest msg = new IgfsPathControlRequest();

        msg.command(OPEN_APPEND);
        msg.path(path);
        msg.flag(create);
        msg.properties(props);
        msg.userName(userName);

        Long streamId = io.send(msg).chain(LONG_RES).get();

        return new HadoopIgfsStreamDelegate(this, streamId);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<byte[]> readData(HadoopIgfsStreamDelegate desc, long pos, int len,
        final @Nullable byte[] outBuf, final int outOff, final int outLen) {
        assert len > 0;

        final IgfsStreamControlRequest msg = new IgfsStreamControlRequest();

        msg.command(READ_BLOCK);
        msg.streamId((long) desc.target());
        msg.position(pos);
        msg.length(len);

        try {
            return io.send(msg, outBuf, outOff, outLen);
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeData(HadoopIgfsStreamDelegate desc, byte[] data, int off, int len)
        throws IOException {
        final IgfsStreamControlRequest msg = new IgfsStreamControlRequest();

        msg.command(WRITE_BLOCK);
        msg.streamId((long) desc.target());
        msg.data(data);
        msg.position(off);
        msg.length(len);

        try {
            io.sendPlain(msg);
        }
        catch (IgniteCheckedException e) {
            throw HadoopIgfsUtils.cast(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void flush(HadoopIgfsStreamDelegate delegate) throws IOException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void closeStream(HadoopIgfsStreamDelegate desc) throws IOException {
        final IgfsStreamControlRequest msg = new IgfsStreamControlRequest();

        msg.command(CLOSE);
        msg.streamId((long)desc.target());

        try {
            io.send(msg).chain(BOOL_RES).get();
        }
        catch (IgniteCheckedException e) {
            throw HadoopIgfsUtils.cast(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void addEventListener(HadoopIgfsStreamDelegate desc,
        HadoopIgfsStreamEventListener lsnr) {
        long streamId = desc.target();

        HadoopIgfsStreamEventListener lsnr0 = lsnrs.put(streamId, lsnr);

        assert lsnr0 == null || lsnr0 == lsnr;

        if (log.isDebugEnabled())
            log.debug("Added stream event listener [streamId=" + streamId + ']');
    }

    /** {@inheritDoc} */
    @Override public void removeEventListener(HadoopIgfsStreamDelegate desc) {
        long streamId = desc.target();

        HadoopIgfsStreamEventListener lsnr0 = lsnrs.remove(streamId);

        if (lsnr0 != null && log.isDebugEnabled())
            log.debug("Removed stream event listener [streamId=" + streamId + ']');
    }

    /** {@inheritDoc} */
    @Override public void onClose() {
        for (HadoopIgfsStreamEventListener lsnr : lsnrs.values()) {
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
        HadoopIgfsStreamEventListener lsnr = lsnrs.get(streamId);

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
    private static <T> IgniteClosure<IgniteInternalFuture<IgfsMessage>, T> createClosure() {
        return new IgniteClosure<IgniteInternalFuture<IgfsMessage>, T>() {
            @Override public T apply(IgniteInternalFuture<IgfsMessage> fut) {
                try {
                    IgfsControlResponse res = (IgfsControlResponse)fut.get();

                    if (res.hasError())
                        res.throwError();

                    return (T)res.response();
                }
                catch (IgfsException | IgniteCheckedException e) {
                    throw new GridClosureException(e);
                }
            }
        };
    }

    /** {@inheritDoc} */
    @Override public String user() {
        return userName;
    }
}