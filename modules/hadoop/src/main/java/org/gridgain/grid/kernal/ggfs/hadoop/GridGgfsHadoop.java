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
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.lang.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.kernal.ggfs.common.GridGgfsIpcCommand.*;

/**
 * GGFS client. Responsible for sending and receiving messages.
 *
 * In prototype version, mostly goes pseudo-code.
 */
public class GridGgfsHadoop implements GridGgfsHadoopIpcIoListener {
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

    /** Client log. */
    private final Log log;

    /** Endpoint string. */
    private final String endpoint;

    /** Client IO. */
    private final AtomicReference<GridGgfsHadoopIpcIo> clientIo = new AtomicReference<>();

    /** Event listeners. */
    private final Map<Long, GridGgfsHadoopStreamEventListener> lsnrs = new ConcurrentHashMap8<>();

    /**
     * @param log Client logger.
     * @param endpoint Endpoint string.
     * @throws IOException If failed to start IPC IO.
     */
    public GridGgfsHadoop(Log log, String endpoint) throws IOException {
        this.log = log;
        this.endpoint = endpoint;

        ipcIo(); // Initializes the clientIo reference.
    }

    /**
     * Perform handshake request.
     *
     * @param logDir Log directory.
     * @return Handshake response.
     */
    public GridPlainFuture<GridGgfsHandshakeResponse> handshake(String logDir) {
        final GridGgfsHandshakeRequest req = new GridGgfsHandshakeRequest();

        req.logDirectory(logDir);

        return withReconnectHandling(new CX1<GridGgfsHadoopIpcIo, GridPlainFuture<GridGgfsHandshakeResponse>>() {
            @Override public GridPlainFuture<GridGgfsHandshakeResponse> applyx(GridGgfsHadoopIpcIo io)
                throws GridException {
                return io.send(req).chain(HANDSHAKE_RES);
            }
        });
    }

    /**
     * Performs status request.
     *
     * @return Status response.
     */
    public GridPlainFuture<GridGgfsStatus> fsStatus() {
        return withReconnectHandling(new CX1<GridGgfsHadoopIpcIo, GridPlainFuture<GridGgfsStatus>>() {
            @Override public GridPlainFuture<GridGgfsStatus> applyx(GridGgfsHadoopIpcIo io)
                throws GridException {
                return io.send(new GridGgfsStatusRequest()).chain(STATUS_RES);
            }
        });
    }

    /**
     * Gets path summary.
     *
     * @param path Path to get summary for.
     * @return Future that will be completed when summary is received.
     */
    public GridPlainFuture<GridGgfsPathSummary> contentSummary(GridGgfsPath path) {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(PATH_SUMMARY);
        msg.path(path);

        return withReconnectHandling(new CX1<GridGgfsHadoopIpcIo, GridPlainFuture<GridGgfsPathSummary>>() {
            @Override public GridPlainFuture<GridGgfsPathSummary> applyx(GridGgfsHadoopIpcIo io)
                throws GridException {
                return io.send(msg).chain(SUMMARY_RES);
            }
        });
    }

    /**
     * Command to check if given path exists.
     *
     * @param path Path to check.
     * @return Future for exists operation.
     */
    public GridPlainFuture<Boolean> exists(GridGgfsPath path) {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(EXISTS);
        msg.path(path);

        return withReconnectHandling(new CX1<GridGgfsHadoopIpcIo, GridPlainFuture<Boolean>>() {
            @Override public GridPlainFuture<Boolean> applyx(GridGgfsHadoopIpcIo io) throws GridException {
                return io.send(msg).chain(BOOL_RES);
            }
        });
    }

    /**
     * Command to retrieve file info for some GGFS path.
     *
     * @param path Path to get file info for.
     * @return Future for info operation.
     */
    public GridPlainFuture<GridGgfsFile> info(GridGgfsPath path) {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(INFO);
        msg.path(path);

        return withReconnectHandling(new CX1<GridGgfsHadoopIpcIo, GridPlainFuture<GridGgfsFile>>() {
            @Override public GridPlainFuture<GridGgfsFile> applyx(GridGgfsHadoopIpcIo io) throws GridException {
                return io.send(msg).chain(FILE_RES);
            }
        });
    }

    /**
     * Command to update file properties.
     *
     * @param path GGFS path to update properties.
     * @param props Properties to update.
     * @return Future for update operation.
     */
    public GridPlainFuture<GridGgfsFile> update(GridGgfsPath path, Map<String, String> props) {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(UPDATE);
        msg.path(path);
        msg.properties(props);

        return withReconnectHandling(new CX1<GridGgfsHadoopIpcIo, GridPlainFuture<GridGgfsFile>>() {
            @Override public GridPlainFuture<GridGgfsFile> applyx(GridGgfsHadoopIpcIo io) throws GridException {
                return io.send(msg).chain(FILE_RES);
            }
        });
    }

    /**
     * Command to rename given path.
     *
     * @param src Source path.
     * @param dest Destination path.
     * @return Future for rename operation.
     */
    public GridPlainFuture<Boolean> rename(GridGgfsPath src, GridGgfsPath dest) {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(RENAME);
        msg.path(src);
        msg.destinationPath(dest);

        return withReconnectHandling(new CX1<GridGgfsHadoopIpcIo, GridPlainFuture<Boolean>>() {
            @Override public GridPlainFuture<Boolean> applyx(GridGgfsHadoopIpcIo io) throws GridException {
                return io.send(msg).chain(BOOL_RES);
            }
        });
    }

    /**
     * Command to delete given path.
     *
     * @param path Path to delete.
     * @param recursive {@code True} if deletion is recursive.
     * @return Future for delete operation.
     */
    public GridPlainFuture<Boolean> delete(GridGgfsPath path, boolean recursive) {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(DELETE);
        msg.path(path);
        msg.flag(recursive);

        return withReconnectHandling(new CX1<GridGgfsHadoopIpcIo, GridPlainFuture<Boolean>>() {
            @Override public GridPlainFuture<Boolean> applyx(GridGgfsHadoopIpcIo io) throws GridException {
                return io.send(msg).chain(BOOL_RES);
            }
        });
    }

    /**
     * Command to create directories.
     *
     * @param path Path to create.
     * @return Future for mkdirs operation.
     */
    public GridPlainFuture<Boolean> mkdirs(GridGgfsPath path, Map<String, String> props) {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(MAKE_DIRECTORIES);
        msg.path(path);
        msg.properties(props);

        return withReconnectHandling(new CX1<GridGgfsHadoopIpcIo, GridPlainFuture<Boolean>>() {
            @Override public GridPlainFuture<Boolean> applyx(GridGgfsHadoopIpcIo io) throws GridException {
                return io.send(msg).chain(BOOL_RES);
            }
        });
    }

    /**
     * Command to get directory listing.
     *
     * @param path Path to list.
     * @return Future for listPaths operation.
     */
    public GridPlainFuture<Collection<GridGgfsPath>> listPaths(GridGgfsPath path) {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(LIST_PATHS);
        msg.path(path);

        return withReconnectHandling(new CX1<GridGgfsHadoopIpcIo, GridPlainFuture<Collection<GridGgfsPath>>>() {
            @Override public GridPlainFuture<Collection<GridGgfsPath>> applyx(GridGgfsHadoopIpcIo io)
                throws GridException {
                return io.send(msg).chain(PATH_COL_RES);
            }
        });
    }

    /**
     * Command to get list of files in directory.
     *
     * @param path Path to list.
     * @return Future for listFiles operation.
     */
    public GridPlainFuture<Collection<GridGgfsFile>> listFiles(GridGgfsPath path) {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(LIST_FILES);
        msg.path(path);

        return withReconnectHandling(new CX1<GridGgfsHadoopIpcIo, GridPlainFuture<Collection<GridGgfsFile>>>() {
            @Override public GridPlainFuture<Collection<GridGgfsFile>> applyx(GridGgfsHadoopIpcIo io)
                throws GridException {
                return io.send(msg).chain(FILE_COL_RES);
            }
        });
    }

    /**
     * Command to get affinity for given path, offset and length.
     *
     * @param path Path to get affinity for.
     * @param start Start position (offset).
     * @param len Data length.
     * @return Future for affinity command.
     */
    public GridPlainFuture<Collection<GridGgfsBlockLocation>> affinity(GridGgfsPath path, long start, long len) {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(AFFINITY);
        msg.path(path);
        msg.start(start);
        msg.length(len);

        return withReconnectHandling(
            new CX1<GridGgfsHadoopIpcIo, GridPlainFuture<Collection<GridGgfsBlockLocation>>>() {
                @Override public GridPlainFuture<Collection<GridGgfsBlockLocation>> applyx(GridGgfsHadoopIpcIo io)
                    throws GridException {
                    return io.send(msg).chain(BLOCK_LOCATION_COL_RES);
                }
            });
    }

    /**
     * Sets last access time and last modification time for a file.
     *
     * @param path Path to update times.
     * @param accessTime Last access time to set.
     * @param modificationTime Last modification time to set.
     */
    public GridPlainFuture<Boolean> setTimes(GridGgfsPath path, long accessTime, long modificationTime) {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(SET_TIMES);
        msg.path(path);
        msg.accessTime(accessTime);
        msg.modificationTime(modificationTime);

        return withReconnectHandling(new CX1<GridGgfsHadoopIpcIo, GridPlainFuture<Boolean>>() {
            @Override public GridPlainFuture<Boolean> applyx(GridGgfsHadoopIpcIo io)
                throws GridException {
                return io.send(msg).chain(BOOL_RES);
            }
        });
    }

    /**
     * Command to open file for reading.
     *
     * @param path File path to open.
     * @return Future for open operation.
     */
    public GridPlainFuture<GridGgfsInputStreamDescriptor> open(GridGgfsPath path) {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(OPEN_READ);
        msg.path(path);
        msg.flag(false);

        return withReconnectHandling(new CX1<GridGgfsHadoopIpcIo, GridPlainFuture<GridGgfsInputStreamDescriptor>>() {
            @Override public GridPlainFuture<GridGgfsInputStreamDescriptor> applyx(GridGgfsHadoopIpcIo io)
                throws GridException {
                return io.send(msg).chain(STREAM_DESCRIPTOR_RES);
            }
        });
    }

    /**
     * Command to open file for reading.
     *
     * @param path File path to open.
     * @return Future for open operation.
     */
    public GridPlainFuture<GridGgfsInputStreamDescriptor> open(GridGgfsPath path, int seqReadsBeforePrefetch) {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(OPEN_READ);
        msg.path(path);
        msg.flag(true);
        msg.sequentialReadsBeforePrefetch(seqReadsBeforePrefetch);

        return withReconnectHandling(new CX1<GridGgfsHadoopIpcIo, GridPlainFuture<GridGgfsInputStreamDescriptor>>() {
            @Override public GridPlainFuture<GridGgfsInputStreamDescriptor> applyx(GridGgfsHadoopIpcIo io)
                throws GridException {
                return io.send(msg).chain(STREAM_DESCRIPTOR_RES);
            }
        });
    }

    /**
     * Command to create file and open it for output.
     *
     * @param path Path to file.
     * @param overwrite If {@code true} then old file contents will be lost.
     * @param colocate If {@code true} and called on data node, file will be written on that node.
     * @param replication Replication factor.
     * @param props File properties for creation.
     * @return Future for create operation.
     */
    public GridPlainFuture<Long> create(GridGgfsPath path, boolean overwrite, boolean colocate, int replication,
        long blockSize, @Nullable Map<String, String> props) {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(OPEN_CREATE);
        msg.path(path);
        msg.flag(overwrite);
        msg.colocate(colocate);
        msg.properties(props);
        msg.replication(replication);
        msg.blockSize(blockSize);

        return withReconnectHandling(new CX1<GridGgfsHadoopIpcIo, GridPlainFuture<Long>>() {
            @Override public GridPlainFuture<Long> applyx(GridGgfsHadoopIpcIo io) throws GridException {
                return io.send(msg).chain(LONG_RES);
            }
        });
    }

    /**
     * Open file for output appending data to the end of a file.
     *
     * @param path Path to file.
     * @param create If {@code true}, file will be created if does not exist.
     * @param props File properties.
     * @return Future for append operation.
     */
    public GridPlainFuture<Long> append(GridGgfsPath path, boolean create,
        @Nullable Map<String, String> props) {
        final GridGgfsPathControlRequest msg = new GridGgfsPathControlRequest();

        msg.command(OPEN_APPEND);
        msg.path(path);
        msg.flag(create);
        msg.properties(props);

        return withReconnectHandling(new CX1<GridGgfsHadoopIpcIo, GridPlainFuture<Long>>() {
            @Override public GridPlainFuture<Long> applyx(GridGgfsHadoopIpcIo io) throws GridException {
                return io.send(msg).chain(LONG_RES);
            }
        });
    }

    /**
     * Asynchronously reads specified amount of bytes from opened input stream.
     *
     * @param pos Position to read from.
     * @param len Data length to read.
     * @param outBuf Optional output buffer. If buffer length is less then {@code len}, all remaining
     * bytes will be read into new allocated buffer of length {len - outBuf.length} and this buffer will
     * be the result of read future.
     *
     * @return Read future.
     */
    public GridPlainFuture<byte[]> readData(long streamId, long pos, int len,
        @Nullable final byte[] outBuf, final int outOff, final int outLen) {
        assert len > 0;

        final GridGgfsStreamControlRequest msg = new GridGgfsStreamControlRequest();

        msg.command(READ_BLOCK);
        msg.streamId(streamId);
        msg.position(pos);
        msg.length(len);

        return withReconnectHandling(new CX1<GridGgfsHadoopIpcIo, GridPlainFuture<byte[]>>() {
            @Override public GridPlainFuture<byte[]> applyx(GridGgfsHadoopIpcIo io) throws GridException {
                return io.send(msg, outBuf, outOff, outLen);
            }
        });
    }

    /**
     * Writes data to the stream with given streamId. This method does not return any future since
     * no response to write request is sent.
     *
     * @param streamId Stream ID to write to.
     * @param data Data to write.
     * @throws GridException If write failed.
     */
    public void writeData(long streamId, byte[] data, int off, int len) throws GridException {
        final GridGgfsStreamControlRequest msg = new GridGgfsStreamControlRequest();

        msg.command(WRITE_BLOCK);
        msg.streamId(streamId);
        msg.data(data);
        msg.position(off);
        msg.length(len);

        withReconnectHandling(new CX1<GridGgfsHadoopIpcIo, GridPlainFuture<Void>>() {
            @Override public GridPlainFuture<Void> applyx(GridGgfsHadoopIpcIo io) throws GridException {
                io.sendPlain(msg);

                return new GridPlainFutureAdapter<>();
            }
        });
    }

    /**
     * Command to close stream with given stream ID.
     *
     * @param streamId Stream ID to close.
     * @return Future for close operation.
     */
    public GridPlainFuture<Boolean> closeStream(long streamId) {
        final GridGgfsStreamControlRequest msg = new GridGgfsStreamControlRequest();

        msg.command(CLOSE);
        msg.streamId(streamId);

        return withReconnectHandling(new CX1<GridGgfsHadoopIpcIo, GridPlainFuture<Boolean>>() {
            @Override public GridPlainFuture<Boolean> applyx(GridGgfsHadoopIpcIo io) throws GridException {
                return io.send(msg).chain(BOOL_RES);
            }
        });
    }

    /**
     * Adds event listener that will be invoked when connection with server is lost or remote error has occurred.
     * If connection is closed already, callback will be invoked synchronously inside this method.
     *
     * @param streamId Stream ID.
     * @param lsnr Event listener.
     */
    public void addEventListener(long streamId, GridGgfsHadoopStreamEventListener lsnr) {
        GridGgfsHadoopStreamEventListener lsnr0 = lsnrs.put(streamId, lsnr);

        assert lsnr0 == null || lsnr0 == lsnr;

        if (log.isDebugEnabled())
            log.debug("Added stream event listener [streamId=" + streamId + ']');
    }

    /**
     * Removes event listener that will be invoked when connection with server is lost or remote error has occurred.
     *
     * @param streamId Stream ID.
     */
    public void removeEventListener(Long streamId) {
        GridGgfsHadoopStreamEventListener lsnr0 = lsnrs.remove(streamId);

        if (lsnr0 != null && log.isDebugEnabled())
            log.debug("Removed stream event listener [streamId=" + streamId + ']');
    }

    /**
     * Closes the client.
     */
    public void close() {
        GridGgfsHadoopIpcIo io = clientIo.get();

        if (io != null) {
            io.removeEventListener(this);

            io.release();
        }
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

        if (lsnr != null) {
            try {
                lsnr.onError(errMsg);
            }
            catch (GridException e) {
                log.warn("Got exception from stream event listener (will ignore): " + lsnr, e);
            }
        }
        else
            log.warn("Received write error response for not registered output stream (will ignore) " +
                "[streamId= " + streamId + ']');
    }

    /**
     * Performs an operation with reconnect attempt in case of failure.
     *
     * @param c Out closure, which performs an operation.
     * @param <T> Result type.
     * @return Operation result.
     */
    private <T> GridPlainFuture<T> withReconnectHandling(
        final GridClosureX<GridGgfsHadoopIpcIo, GridPlainFuture<T>> c) {
        Exception err = null;

        for (int i = 0; i < 2; i++) {
            GridGgfsHadoopIpcIo locIo = null;

            try {
                locIo = ipcIo();

                return c.applyx(locIo);
            }
            catch (GridGgfsIoException e) {
                // Always force close to remove from cache.
                locIo.forceClose();

                clientIo.compareAndSet(locIo, null);

                // Always output in debug.
                if (log.isDebugEnabled())
                    log.debug("Failed to send message to a server: " + e);

                err = e;
            }
            catch (IOException e) {
                return new GridPlainFutureAdapter<>(e);
            }
            catch (GridException e) {
                return new GridPlainFutureAdapter<>(e);
            }
        }

        if (err == null)
            throw new AssertionError(); // Should never happen. We are here => we've got exception.

        log.error("Failed to send message to a server.", err);

        return new GridPlainFutureAdapter<>(err);
    }

    /**
     * @return GGFS IO.
     * @throws IOException If failed.
     */
    private GridGgfsHadoopIpcIo ipcIo() throws IOException {
        while (true) {
            GridGgfsHadoopIpcIo io = clientIo.get();

            if (io != null)
                return io;

            GridGgfsHadoopIpcIo newIo = GridGgfsHadoopIpcIo.get(log, endpoint);

            newIo.addEventListener(this);

            if (!clientIo.compareAndSet(null, newIo))
                newIo.release();
            else
                return newIo;
        }
    }

    /**
     * Creates conversion closure for given type.
     *
     * @param <T> Type of expected result.
     * @return Conversion closure.
     */
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
