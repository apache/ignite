/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.hadoop;

import org.apache.commons.logging.*;
import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.kernal.ggfs.common.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.util.lang.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.kernal.ggfs.common.GridGgfsIpcCommand.*;

/**
 * Communication with external process (TCP or shmem).
 */
public class GridGgfsHadoopOutProc implements GridGgfsHadoopEx, GridGgfsHadoopIpcIoListener {
    /** Expected result is boolean. */
    private static final GridPlainClosure<GridPlainFuture<GridGgfsMessage>, Boolean> BOOL_RES = createClosure();

    /** Expected result is boolean. */
    private static final GridPlainClosure<GridPlainFuture<GridGgfsMessage>, Long> LONG_RES = createClosure();

    /** Expected result is {@code GridGgfsFile}. */
    private static final GridPlainClosure<GridPlainFuture<GridGgfsMessage>, GridGgfsFile> FILE_RES = createClosure();

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
        Collection<GridGgfsFile>> FILE_COL_RES = createClosure();

    /** Expected result is {@code GridGgfsFile}. */
    private static final GridPlainClosure<GridPlainFuture<GridGgfsMessage>,
        Collection<GridGgfsPath>> PATH_COL_RES = createClosure();

    /** Expected result is {@code GridGgfsPathSummary}. */
    private static final GridPlainClosure<GridPlainFuture<GridGgfsMessage>, GridGgfsPathSummary> SUMMARY_RES =
        createClosure();

    /** Expected result is {@code GridGgfsFile}. */
    private static final GridPlainClosure<GridPlainFuture<GridGgfsMessage>,
        Collection<GridGgfsBlockLocation>> BLOCK_LOCATION_COL_RES = createClosure();

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
    @Override public GridGgfsHandshakeResponse handshake(String logDir) throws GridException {
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
    @Override public GridGgfsFile info(GridGgfsPath path) throws GridException {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(INFO);
        msg.path(path);

        return io.send(msg).chain(FILE_RES).get();
    }

    /** {@inheritDoc} */
    @Override public GridGgfsFile update(GridGgfsPath path, Map<String, String> props) throws GridException {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(UPDATE);
        msg.path(path);
        msg.properties(props);

        return io.send(msg).chain(FILE_RES).get();
    }

    /** {@inheritDoc} */
    @Override public Boolean setTimes(GridGgfsPath path, long accessTime, long modificationTime) throws GridException {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(SET_TIMES);
        msg.path(path);
        msg.accessTime(accessTime);
        msg.modificationTime(modificationTime);

        return io.send(msg).chain(BOOL_RES).get();
    }

    /** {@inheritDoc} */
    @Override public Boolean rename(GridGgfsPath src, GridGgfsPath dest) throws GridException {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(RENAME);
        msg.path(src);
        msg.destinationPath(dest);

        return io.send(msg).chain(BOOL_RES).get();
    }

    /** {@inheritDoc} */
    @Override public Boolean delete(GridGgfsPath path, boolean recursive) throws GridException {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(DELETE);
        msg.path(path);
        msg.flag(recursive);

        return io.send(msg).chain(BOOL_RES).get();
    }

    /** {@inheritDoc} */
    @Override public Collection<GridGgfsBlockLocation> affinity(GridGgfsPath path, long start, long len)
        throws GridException {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(AFFINITY);
        msg.path(path);
        msg.start(start);
        msg.length(len);

        return io.send(msg).chain(BLOCK_LOCATION_COL_RES).get();
    }

    /** {@inheritDoc} */
    @Override public GridGgfsPathSummary contentSummary(GridGgfsPath path) throws GridException {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(PATH_SUMMARY);
        msg.path(path);

        return io.send(msg).chain(SUMMARY_RES).get();
    }

    /** {@inheritDoc} */
    @Override public Boolean mkdirs(GridGgfsPath path, Map<String, String> props) throws GridException {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(MAKE_DIRECTORIES);
        msg.path(path);
        msg.properties(props);

        return io.send(msg).chain(BOOL_RES).get();
    }

    /** {@inheritDoc} */
    @Override public Collection<GridGgfsFile> listFiles(GridGgfsPath path) throws GridException {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(LIST_FILES);
        msg.path(path);

        return io.send(msg).chain(FILE_COL_RES).get();
    }

    /** {@inheritDoc} */
    @Override public Collection<GridGgfsPath> listPaths(GridGgfsPath path) throws GridException {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(LIST_PATHS);
        msg.path(path);

        return io.send(msg).chain(PATH_COL_RES).get();
    }

    /** {@inheritDoc} */
    @Override public GridGgfsStatus fsStatus() throws GridException {
        return io.send(new GridGgfsStatusRequest()).chain(STATUS_RES).get();
    }

    /** {@inheritDoc} */
    @Override public GridGgfsHadoopStreamDelegate open(GridGgfsPath path) throws GridException {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(OPEN_READ);
        msg.path(path);
        msg.flag(false);

        GridGgfsInputStreamDescriptor rmtDesc = io.send(msg).chain(STREAM_DESCRIPTOR_RES).get();

        return new GridGgfsHadoopStreamDelegate(this, rmtDesc.streamId(), rmtDesc.length());
    }

    /** {@inheritDoc} */
    @Override public GridGgfsHadoopStreamDelegate open(GridGgfsPath path,
        int seqReadsBeforePrefetch) throws GridException {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(OPEN_READ);
        msg.path(path);
        msg.flag(true);
        msg.sequentialReadsBeforePrefetch(seqReadsBeforePrefetch);

        GridGgfsInputStreamDescriptor rmtDesc = io.send(msg).chain(STREAM_DESCRIPTOR_RES).get();

        return new GridGgfsHadoopStreamDelegate(this, rmtDesc.streamId(), rmtDesc.length());
    }

    /** {@inheritDoc} */
    @Override public GridGgfsHadoopStreamDelegate create(GridGgfsPath path, boolean overwrite, boolean colocate,
        int replication, long blockSize, @Nullable Map<String, String> props) throws GridException {
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
    @Override public GridGgfsHadoopStreamDelegate append(GridGgfsPath path, boolean create,
        @Nullable Map<String, String> props) throws GridException {
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
        catch (GridException e) {
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
        catch (GridException e) {
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
        catch (GridException e) {
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
            catch (GridException e) {
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
            @Override public T apply(GridPlainFuture<GridGgfsMessage> fut) throws GridException {
                GridGgfsControlResponse res = (GridGgfsControlResponse)fut.get();

                if (res.hasError())
                    res.throwError();

                return (T)res.response();
            }
        };
    }
}
