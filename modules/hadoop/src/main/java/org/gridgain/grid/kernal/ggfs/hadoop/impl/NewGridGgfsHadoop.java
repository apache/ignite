/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.hadoop.impl;

import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.kernal.ggfs.hadoop.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.util.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Facade for communication with grid.
 */
public interface NewGridGgfsHadoop {
    /**
     * Perform handshake.
     *
     * @param logDir Log directory.
     * @return Future with handshake result.
     */
    public GridPlainFuture<GridGgfsHandshakeResponse> handshake(String logDir);

    /**
     * Close connection.
     */
    public void close();

    /**
     * Command to retrieve file info for some GGFS path.
     *
     * @param path Path to get file info for.
     * @return Future for info operation.
     */
    public GridPlainFuture<GridGgfsFile> info(GridGgfsPath path);

    /**
     * Command to update file properties.
     *
     * @param path GGFS path to update properties.
     * @param props Properties to update.
     * @return Future for update operation.
     */
    public GridPlainFuture<GridGgfsFile> update(GridGgfsPath path, Map<String, String> props);

    /**
     * Sets last access time and last modification time for a file.
     *
     * @param path Path to update times.
     * @param accessTime Last access time to set.
     * @param modificationTime Last modification time to set.
     */
    public GridPlainFuture<Boolean> setTimes(GridGgfsPath path, long accessTime, long modificationTime);

    /**
     * Command to rename given path.
     *
     * @param src Source path.
     * @param dest Destination path.
     * @return Future for rename operation.
     */
    public GridPlainFuture<Boolean> rename(GridGgfsPath src, GridGgfsPath dest);

    /**
     * Command to delete given path.
     *
     * @param path Path to delete.
     * @param recursive {@code True} if deletion is recursive.
     * @return Future for delete operation.
     */
    public GridPlainFuture<Boolean> delete(GridGgfsPath path, boolean recursive);

    /**
     * Command to get affinity for given path, offset and length.
     *
     * @param path Path to get affinity for.
     * @param start Start position (offset).
     * @param len Data length.
     * @return Future for affinity command.
     */
    public GridPlainFuture<Collection<GridGgfsBlockLocation>> affinity(GridGgfsPath path, long start, long len);

    /**
     * Command to create directories.
     *
     * @param path Path to create.
     * @return Future for mkdirs operation.
     */
    public GridPlainFuture<Boolean> mkdirs(GridGgfsPath path, Map<String, String> props);

    /**
     * Command to get list of files in directory.
     *
     * @param path Path to list.
     * @return Future for listFiles operation.
     */
    public GridPlainFuture<Collection<GridGgfsFile>> listFiles(GridGgfsPath path);

    /**
     * Performs status request.
     *
     * @return Status response.
     */
    public GridPlainFuture<GridGgfsStatus> fsStatus();

    /**
     * Command to open file for reading.
     *
     * @param path File path to open.
     * @return Future for open operation.
     */
    public GridPlainFuture<GridGgfsInputStreamDescriptor> open(GridGgfsPath path, int seqReadsBeforePrefetch);

    /**
     * Open file for output appending data to the end of a file.
     *
     * @param path Path to file.
     * @param create If {@code true}, file will be created if does not exist.
     * @param props File properties.
     * @return Future for append operation.
     */
    public GridPlainFuture<Long> append(GridGgfsPath path, boolean create, @Nullable Map<String, String> props);

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
    public GridPlainFuture<byte[]> readData(long streamId, long pos, int len, @Nullable final byte[] outBuf,
        final int outOff, final int outLen);

    /**
     * Writes data to the stream with given streamId. This method does not return any future since
     * no response to write request is sent.
     *
     * @param streamId Stream ID to write to.
     * @param data Data to write.
     * @throws org.gridgain.grid.GridException If write failed.
     */
    public void writeData(long streamId, byte[] data, int off, int len) throws GridException;

    /**
     * Close server stream.
     *
     * @param desc Stream descriptor.
     * @return Close future.
     */
    public GridPlainFuture<Boolean> closeStream(NewGridGgfsHadoopStreamDescriptor desc);

    /**
     * Adds event listener that will be invoked when connection with server is lost or remote error has occurred.
     * If connection is closed already, callback will be invoked synchronously inside this method.
     *
     * @param desc Stream descriptor.
     * @param lsnr Event listener.
     */
    public void addEventListener(NewGridGgfsHadoopStreamDescriptor desc, GridGgfsStreamEventListener lsnr);

    /**
     * Removes event listener that will be invoked when connection with server is lost or remote error has occurred.
     *
     * @param desc Stream descriptor.
     */
    public void removeEventListener(NewGridGgfsHadoopStreamDescriptor desc);
}
