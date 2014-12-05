/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.hadoop;

import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Facade for communication with grid.
 */
public interface GridGgfsHadoop {
    /**
     * Perform handshake.
     *
     * @param logDir Log directory.
     * @return Future with handshake result.
     * @throws org.gridgain.grid.GridException If failed.
     */
    public GridGgfsHandshakeResponse handshake(String logDir) throws GridException, IOException;

    /**
     * Close connection.
     *
     * @param force Force flag.
     */
    public void close(boolean force);

    /**
     * Command to retrieve file info for some GGFS path.
     *
     * @param path Path to get file info for.
     * @return Future for info operation.
     * @throws GridException If failed.
     */
    public IgniteFsFile info(IgniteFsPath path) throws GridException, IOException;

    /**
     * Command to update file properties.
     *
     * @param path GGFS path to update properties.
     * @param props Properties to update.
     * @return Future for update operation.
     * @throws GridException If failed.
     */
    public IgniteFsFile update(IgniteFsPath path, Map<String, String> props) throws GridException, IOException;

    /**
     * Sets last access time and last modification time for a file.
     *
     * @param path Path to update times.
     * @param accessTime Last access time to set.
     * @param modificationTime Last modification time to set.
     * @throws GridException If failed.
     */
    public Boolean setTimes(IgniteFsPath path, long accessTime, long modificationTime) throws GridException,
        IOException;

    /**
     * Command to rename given path.
     *
     * @param src Source path.
     * @param dest Destination path.
     * @return Future for rename operation.
     * @throws GridException If failed.
     */
    public Boolean rename(IgniteFsPath src, IgniteFsPath dest) throws GridException, IOException;

    /**
     * Command to delete given path.
     *
     * @param path Path to delete.
     * @param recursive {@code True} if deletion is recursive.
     * @return Future for delete operation.
     * @throws GridException If failed.
     */
    public Boolean delete(IgniteFsPath path, boolean recursive) throws GridException, IOException;

    /**
     * Command to get affinity for given path, offset and length.
     *
     * @param path Path to get affinity for.
     * @param start Start position (offset).
     * @param len Data length.
     * @return Future for affinity command.
     * @throws GridException If failed.
     */
    public Collection<IgniteFsBlockLocation> affinity(IgniteFsPath path, long start, long len) throws GridException,
        IOException;

    /**
     * Gets path summary.
     *
     * @param path Path to get summary for.
     * @return Future that will be completed when summary is received.
     * @throws GridException If failed.
     */
    public IgniteFsPathSummary contentSummary(IgniteFsPath path) throws GridException, IOException;

    /**
     * Command to create directories.
     *
     * @param path Path to create.
     * @return Future for mkdirs operation.
     * @throws GridException If failed.
     */
    public Boolean mkdirs(IgniteFsPath path, Map<String, String> props) throws GridException, IOException;

    /**
     * Command to get list of files in directory.
     *
     * @param path Path to list.
     * @return Future for listFiles operation.
     * @throws GridException If failed.
     */
    public Collection<IgniteFsFile> listFiles(IgniteFsPath path) throws GridException, IOException;

    /**
     * Command to get directory listing.
     *
     * @param path Path to list.
     * @return Future for listPaths operation.
     * @throws GridException If failed.
     */
    public Collection<IgniteFsPath> listPaths(IgniteFsPath path) throws GridException, IOException;

    /**
     * Performs status request.
     *
     * @return Status response.
     * @throws GridException If failed.
     */
    public GridGgfsStatus fsStatus() throws GridException, IOException;

    /**
     * Command to open file for reading.
     *
     * @param path File path to open.
     * @return Future for open operation.
     * @throws GridException If failed.
     */
    public GridGgfsHadoopStreamDelegate open(IgniteFsPath path) throws GridException, IOException;

    /**
     * Command to open file for reading.
     *
     * @param path File path to open.
     * @return Future for open operation.
     * @throws GridException If failed.
     */
    public GridGgfsHadoopStreamDelegate open(IgniteFsPath path, int seqReadsBeforePrefetch) throws GridException,
        IOException;

    /**
     * Command to create file and open it for output.
     *
     * @param path Path to file.
     * @param overwrite If {@code true} then old file contents will be lost.
     * @param colocate If {@code true} and called on data node, file will be written on that node.
     * @param replication Replication factor.
     * @param props File properties for creation.
     * @return Stream descriptor.
     * @throws GridException If failed.
     */
    public GridGgfsHadoopStreamDelegate create(IgniteFsPath path, boolean overwrite, boolean colocate,
        int replication, long blockSize, @Nullable Map<String, String> props) throws GridException, IOException;

    /**
     * Open file for output appending data to the end of a file.
     *
     * @param path Path to file.
     * @param create If {@code true}, file will be created if does not exist.
     * @param props File properties.
     * @return Stream descriptor.
     * @throws GridException If failed.
     */
    public GridGgfsHadoopStreamDelegate append(IgniteFsPath path, boolean create,
        @Nullable Map<String, String> props) throws GridException, IOException;
}
