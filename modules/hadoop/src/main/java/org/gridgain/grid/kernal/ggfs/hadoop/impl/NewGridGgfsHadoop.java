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
    public GridGgfsHandshakeResponse handshake(String logDir);

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
    public GridGgfsFile info(GridGgfsPath path);

    /**
     * Command to update file properties.
     *
     * @param path GGFS path to update properties.
     * @param props Properties to update.
     * @return Future for update operation.
     */
    public GridGgfsFile update(GridGgfsPath path, Map<String, String> props);

    /**
     * Sets last access time and last modification time for a file.
     *
     * @param path Path to update times.
     * @param accessTime Last access time to set.
     * @param modificationTime Last modification time to set.
     */
    public Boolean setTimes(GridGgfsPath path, long accessTime, long modificationTime);

    /**
     * Command to rename given path.
     *
     * @param src Source path.
     * @param dest Destination path.
     * @return Future for rename operation.
     */
    public Boolean rename(GridGgfsPath src, GridGgfsPath dest);

    /**
     * Command to delete given path.
     *
     * @param path Path to delete.
     * @param recursive {@code True} if deletion is recursive.
     * @return Future for delete operation.
     */
    public Boolean delete(GridGgfsPath path, boolean recursive);

    /**
     * Command to get affinity for given path, offset and length.
     *
     * @param path Path to get affinity for.
     * @param start Start position (offset).
     * @param len Data length.
     * @return Future for affinity command.
     */
    public Collection<GridGgfsBlockLocation> affinity(GridGgfsPath path, long start, long len);

    /**
     * Gets path summary.
     *
     * @param path Path to get summary for.
     * @return Future that will be completed when summary is received.
     */
    public GridGgfsPathSummary contentSummary(GridGgfsPath path);

    /**
     * Command to create directories.
     *
     * @param path Path to create.
     * @return Future for mkdirs operation.
     */
    public Boolean mkdirs(GridGgfsPath path, Map<String, String> props);

    /**
     * Command to get list of files in directory.
     *
     * @param path Path to list.
     * @return Future for listFiles operation.
     */
    public Collection<GridGgfsFile> listFiles(GridGgfsPath path);

    /**
     * Command to get directory listing.
     *
     * @param path Path to list.
     * @return Future for listPaths operation.
     */
    public Collection<GridGgfsPath> listPaths(GridGgfsPath path);

    /**
     * Performs status request.
     *
     * @return Status response.
     */
    public GridGgfsStatus fsStatus();

    /**
     * Command to open file for reading.
     *
     * @param path File path to open.
     * @return Future for open operation.
     */
    public GridGgfsInputStreamDescriptor open(GridGgfsPath path);

    /**
     * Command to open file for reading.
     *
     * @param path File path to open.
     * @return Future for open operation.
     */
    public GridGgfsInputStreamDescriptor open(GridGgfsPath path, int seqReadsBeforePrefetch);

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
    public Long create(GridGgfsPath path, boolean overwrite, boolean colocate, int replication,
        long blockSize, @Nullable Map<String, String> props);

    /**
     * Open file for output appending data to the end of a file.
     *
     * @param path Path to file.
     * @param create If {@code true}, file will be created if does not exist.
     * @param props File properties.
     * @return Future for append operation.
     */
    public Long append(GridGgfsPath path, boolean create, @Nullable Map<String, String> props);

    /**
     * Asynchronously reads specified amount of bytes from opened input stream.
     *
     * @param desc Stream descriptor.
     * @param pos Position to read from.
     * @param len Data length to read.
     * @param outBuf Optional output buffer. If buffer length is less then {@code len}, all remaining
     *     bytes will be read into new allocated buffer of length {len - outBuf.length} and this buffer will
     *     be the result of read future.
     * @param outOff Output offset.
     * @param outLen Output length.
     *
     * @return Read future.
     */
    public byte[] readData(NewGridGgfsHadoopStreamDescriptor desc, long pos, int len,
        @Nullable final byte[] outBuf, final int outOff, final int outLen);

    /**
     * Writes data to the stream with given streamId. This method does not return any future since
     * no response to write request is sent.
     *
     * @param desc Stream descriptor.
     * @param data Data to write.
     * @param off Offset.
     * @param len Length.
     * @throws GridException If write failed.
     */
    public void writeData(NewGridGgfsHadoopStreamDescriptor desc, byte[] data, int off, int len) throws GridException;

    /**
     * Close server stream.
     *
     * @param desc Stream descriptor.
     * @return Close future.
     */
    public Boolean closeStream(NewGridGgfsHadoopStreamDescriptor desc);

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
