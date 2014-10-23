/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.ggfs;

import org.gridgain.grid.*;
import org.gridgain.grid.design.*;
import org.gridgain.grid.ggfs.mapreduce.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * <b>G</b>rid<b>G</b>ain <b>F</b>ile <b>S</b>ystem API. It provides a typical file system "view" on a particular cache:
 * <ul>
 *     <li>list directories or get information for a single path</li>
 *     <li>create/move/delete files or directories</li>
 *     <li>write/read data streams into/from files</li>
 * </ul>
 * The data of each file is split on separate data blocks and stored in the cache.
 * You can access file's data with a standard Java streaming API. Moreover, for each part
 * of the file you can calculate an affinity and process file's content on corresponding
 * nodes to escape unnecessary networking.
 * <p/>
 * This API is fully thread-safe and you can use it from several threads.
 * <h1 class="header">GGFS Configuration</h1>
 * The simplest way to run a GridGain node with configured file system is to pass
 * special configuration file included in GridGain distribution to {@code ggstart.sh} or
 * {@code ggstart.bat} scripts, like this: {@code ggstart.sh config/hadoop/default-config.xml}
 * <p>
 * {@code GGFS} can be started as a data node or as a client node. Data node is responsible for
 * caching data, while client node is responsible for basic file system operations and accessing
 * data nodes remotely. When used as Hadoop file system, clients nodes usually started together
 * with {@code job-submitter} or {@code job-scheduler} processes, while data nodes are usually
 * started together with Hadoop {@code task-tracker} processes.
 * <h1 class="header">Integration With Hadoop</h1>
 * In addition to direct file system API, {@code GGFS} can be integrated with {@code Hadoop} by
 * plugging in as {@code Hadoop FileSystem}. Refer to
 * {@code org.gridgain.grid.ggfs.hadoop.v1.GridGgfsHadoopFileSystem} or
 * {@code org.gridgain.grid.ggfs.hadoop.v2.GridGgfsHadoopFileSystem} for more information.
 * <p>
 * <b>NOTE:</b> integration with Hadoop is available only in {@code In-Memory Accelerator For Hadoop} edition.
 */
public interface GridGgfs extends GridGgfsFileSystem, GridAsyncSupport<GridGgfs> {
    /** GGFS scheme name. */
    public static final String GGFS_SCHEME = "ggfs";

    /** File property: prefer writes to local node. */
    public static final String PROP_PREFER_LOCAL_WRITES = "locWrite";

    /**
     * Gets GGFS name.
     *
     * @return GGFS name.
     */
    @Nullable public String name();

    /**
     * Gets GGFS configuration.
     *
     * @return GGFS configuration.
     */
    public GridGgfsConfiguration configuration();

    /**
     * Gets summary (total number of files, total number of directories and total length)
     * for a given path.
     *
     * @param path Path to get information for.
     * @return Summary object.
     * @throws GridGgfsFileNotFoundException If path is not found.
     * @throws GridException If failed.
     */
    public GridGgfsPathSummary summary(GridGgfsPath path) throws GridException;

    /**
     * Opens a file for reading.
     *
     * @param path File path to read.
     * @return File input stream to read data from.
     * @throws GridException In case of error.
     * @throws GridGgfsFileNotFoundException If path doesn't exist.
     */
    public GridGgfsInputStream open(GridGgfsPath path) throws GridException;

    /**
     * Opens a file for reading.
     *
     * @param path File path to read.
     * @param bufSize Read buffer size (bytes) or {@code zero} to use default value.
     * @return File input stream to read data from.
     * @throws GridException In case of error.
     * @throws GridGgfsFileNotFoundException If path doesn't exist.
     */
    @Override public GridGgfsInputStream open(GridGgfsPath path, int bufSize) throws GridException;

    /**
     * Opens a file for reading.
     *
     * @param path File path to read.
     * @param bufSize Read buffer size (bytes) or {@code zero} to use default value.
     * @param seqReadsBeforePrefetch Amount of sequential reads before prefetch is started.
     * @return File input stream to read data from.
     * @throws GridException In case of error.
     * @throws GridGgfsFileNotFoundException If path doesn't exist.
     */
    public GridGgfsInputStream open(GridGgfsPath path, int bufSize, int seqReadsBeforePrefetch) throws GridException;

    /**
     * Creates a file and opens it for writing.
     *
     * @param path File path to create.
     * @param overwrite Overwrite file if it already exists. Note: you cannot overwrite an existent directory.
     * @return File output stream to write data to.
     * @throws GridException In case of error.
     */
    @Override public GridGgfsOutputStream create(GridGgfsPath path, boolean overwrite) throws GridException;

    /**
     * Creates a file and opens it for writing.
     *
     * @param path File path to create.
     * @param bufSize Write buffer size (bytes) or {@code zero} to use default value.
     * @param overwrite Overwrite file if it already exists. Note: you cannot overwrite an existent directory.
     * @param replication Replication factor.
     * @param blockSize Block size.
     * @param props File properties to set.
     * @return File output stream to write data to.
     * @throws GridException In case of error.
     */
    @Override public GridGgfsOutputStream create(GridGgfsPath path, int bufSize, boolean overwrite, int replication,
        long blockSize, @Nullable Map<String, String> props) throws GridException;

    /**
     * Creates a file and opens it for writing.
     *
     * @param path File path to create.
     * @param bufSize Write buffer size (bytes) or {@code zero} to use default value.
     * @param overwrite Overwrite file if it already exists. Note: you cannot overwrite an existent directory.
     * @param affKey Affinity key used to store file blocks. If not {@code null}, the whole file will be
     *      stored on node where {@code affKey} resides.
     * @param replication Replication factor.
     * @param blockSize Block size.
     * @param props File properties to set.
     * @return File output stream to write data to.
     * @throws GridException In case of error.
     */
    public GridGgfsOutputStream create(GridGgfsPath path, int bufSize, boolean overwrite,
        @Nullable GridUuid affKey, int replication, long blockSize, @Nullable Map<String, String> props)
        throws GridException;

    /**
     * Opens an output stream to an existing file for appending data.
     *
     * @param path File path to append.
     * @param create Create file if it doesn't exist yet.
     * @return File output stream to append data to.
     * @throws GridException In case of error.
     * @throws GridGgfsFileNotFoundException If path doesn't exist and create flag is {@code false}.
     */
    public GridGgfsOutputStream append(GridGgfsPath path, boolean create) throws GridException;

    /**
     * Opens an output stream to an existing file for appending data.
     *
     * @param path File path to append.
     * @param bufSize Write buffer size (bytes) or {@code zero} to use default value.
     * @param create Create file if it doesn't exist yet.
     * @param props File properties to set only in case it file was just created.
     * @return File output stream to append data to.
     * @throws GridException In case of error.
     * @throws GridGgfsFileNotFoundException If path doesn't exist and create flag is {@code false}.
     */
    @Override public GridGgfsOutputStream append(GridGgfsPath path, int bufSize, boolean create,
        @Nullable Map<String, String> props) throws GridException;

    /**
     * Sets last access time and last modification time for a given path. If argument is {@code null},
     * corresponding time will not be changed.
     *
     * @param path Path to update.
     * @param accessTime Optional last access time to set. Value {@code -1} does not update access time.
     * @param modificationTime Optional last modification time to set. Value {@code -1} does not update
     *      modification time.
     * @throws GridGgfsFileNotFoundException If target was not found.
     * @throws GridException If error occurred.
     */
    public void setTimes(GridGgfsPath path, long accessTime, long modificationTime) throws GridException;

    /**
     * Gets affinity block locations for data blocks of the file, i.e. the nodes, on which the blocks
     * are stored.
     *
     * @param path File path to get affinity for.
     * @param start Position in the file to start affinity resolution from.
     * @param len Size of data in the file to resolve affinity for.
     * @return Affinity block locations.
     * @throws GridException In case of error.
     * @throws GridGgfsFileNotFoundException If path doesn't exist.
     */
    public Collection<GridGgfsBlockLocation> affinity(GridGgfsPath path, long start, long len) throws GridException;

    /**
     * Get affinity block locations for data blocks of the file. In case {@code maxLen} parameter is set and
     * particular block location length is greater than this value, block locations will be split into smaller
     * chunks.
     *
     * @param path File path to get affinity for.
     * @param start Position in the file to start affinity resolution from.
     * @param len Size of data in the file to resolve affinity for.
     * @param maxLen Maximum length of a single returned block location length.
     * @return Affinity block locations.
     * @throws GridException In case of error.
     * @throws GridGgfsFileNotFoundException If path doesn't exist.
     */
    public Collection<GridGgfsBlockLocation> affinity(GridGgfsPath path, long start, long len, long maxLen)
        throws GridException;

    /**
     * Gets metrics snapshot for this file system.
     *
     * @return Metrics.
     * @throws GridException In case of error.
     */
    public GridGgfsMetrics metrics() throws GridException;

    /**
     * Resets metrics for this file system.
     *
     * @throws GridException In case of error.
     */
    public void resetMetrics() throws GridException;

    /**
     * Determines size of the file denoted by provided path. In case if path is a directory, then
     * total size of all containing entries will be calculated recursively.
     *
     * @param path File system path.
     * @return Total size.
     * @throws GridException In case of error.
     */
    public long size(GridGgfsPath path) throws GridException;

    /**
     * Formats the file system removing all existing entries from it.
     *
     * @throws GridException In case format has failed.
     */
    public void format() throws GridException;

    /**
     * Executes GGFS task.
     *
     * @param task Task to execute.
     * @param rslvr Optional resolver to control split boundaries.
     * @param paths Collection of paths to be processed within this task.
     * @param arg Optional task argument.
     * @return Task result.
     * @throws GridException If execution failed.
     */
    public <T, R> R execute(GridGgfsTask<T, R> task, @Nullable GridGgfsRecordResolver rslvr,
        Collection<GridGgfsPath> paths, @Nullable T arg) throws GridException;

    /**
     * Executes GGFS task with overridden maximum range length (see
     * {@link GridGgfsConfiguration#getMaximumTaskRangeLength()} for more information).
     *
     * @param task Task to execute.
     * @param rslvr Optional resolver to control split boundaries.
     * @param paths Collection of paths to be processed within this task.
     * @param skipNonExistentFiles Whether to skip non existent files. If set to {@code true} non-existent files will
     *     be ignored. Otherwise an exception will be thrown.
     * @param maxRangeLen Optional maximum range length. If {@code 0}, then by default all consecutive
     *      GGFS blocks will be included.
     * @param arg Optional task argument.
     * @return Task result.
     * @throws GridException If execution failed.
     */
    public <T, R> R execute(GridGgfsTask<T, R> task, @Nullable GridGgfsRecordResolver rslvr,
        Collection<GridGgfsPath> paths, boolean skipNonExistentFiles, long maxRangeLen, @Nullable T arg)
        throws GridException;

    /**
     * Executes GGFS task.
     *
     * @param taskCls Task class to execute.
     * @param rslvr Optional resolver to control split boundaries.
     * @param paths Collection of paths to be processed within this task.
     * @param arg Optional task argument.
     * @return Task result.
     * @throws GridException If execution failed.
     */
    public <T, R> R execute(Class<? extends GridGgfsTask<T, R>> taskCls,
        @Nullable GridGgfsRecordResolver rslvr, Collection<GridGgfsPath> paths, @Nullable T arg) throws GridException;

    /**
     * Executes GGFS task with overridden maximum range length (see
     * {@link GridGgfsConfiguration#getMaximumTaskRangeLength()} for more information).
     *
     * @param taskCls Task class to execute.
     * @param rslvr Optional resolver to control split boundaries.
     * @param paths Collection of paths to be processed within this task.
     * @param skipNonExistentFiles Whether to skip non existent files. If set to {@code true} non-existent files will
     *     be ignored. Otherwise an exception will be thrown.
     * @param maxRangeLen Maximum range length.
     * @param arg Optional task argument.
     * @return Task result.
     * @throws GridException If execution failed.
     */
    public <T, R> R execute(Class<? extends GridGgfsTask<T, R>> taskCls,
        @Nullable GridGgfsRecordResolver rslvr, Collection<GridGgfsPath> paths, boolean skipNonExistentFiles,
        long maxRangeLen, @Nullable T arg) throws GridException;
}
