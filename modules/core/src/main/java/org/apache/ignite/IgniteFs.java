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

package org.apache.ignite;

import org.apache.ignite.configuration.*;
import org.apache.ignite.ignitefs.*;
import org.apache.ignite.ignitefs.mapreduce.*;
import org.apache.ignite.lang.*;
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
 * The simplest way to run a Ignite node with configured file system is to pass
 * special configuration file included in Ignite distribution to {@code ignite.sh} or
 * {@code ignite.bat} scripts, like this: {@code ignite.sh config/hadoop/default-config.xml}
 * <p>
 * {@code GGFS} can be started as a data node or as a client node. Data node is responsible for
 * caching data, while client node is responsible for basic file system operations and accessing
 * data nodes remotely. When used as Hadoop file system, clients nodes usually started together
 * with {@code job-submitter} or {@code job-scheduler} processes, while data nodes are usually
 * started together with Hadoop {@code task-tracker} processes.
 * <h1 class="header">Integration With Hadoop</h1>
 * In addition to direct file system API, {@code GGFS} can be integrated with {@code Hadoop} by
 * plugging in as {@code Hadoop FileSystem}. Refer to
 * {@code org.apache.ignite.ignitefs.hadoop.v1.GridGgfsHadoopFileSystem} or
 * {@code org.apache.ignite.ignitefs.hadoop.v2.GridGgfsHadoopFileSystem} for more information.
 * <p>
 * <b>NOTE:</b> integration with Hadoop is available only in {@code In-Memory Accelerator For Hadoop} edition.
 */
public interface IgniteFs extends Igfs, IgniteAsyncSupport {
    /** GGFS scheme name. */
    public static final String GGFS_SCHEME = "ggfs";

    /** File property: prefer writes to local node. */
    public static final String PROP_PREFER_LOCAL_WRITES = "locWrite";

    /**
     * Gets IgniteFS name.
     *
     * @return IgniteFS name, or {@code null} for default file system.
     */
    public String name();

    /**
     * Gets GGFS configuration.
     *
     * @return GGFS configuration.
     */
    public IgfsConfiguration configuration();

    /**
     * Gets summary (total number of files, total number of directories and total length)
     * for a given path.
     *
     * @param path Path to get information for.
     * @return Summary object.
     * @throws org.apache.ignite.ignitefs.IgfsFileNotFoundException If path is not found.
     * @throws IgniteException If failed.
     */
    public IgfsPathSummary summary(IgfsPath path) throws IgniteException;

    /**
     * Opens a file for reading.
     *
     * @param path File path to read.
     * @return File input stream to read data from.
     * @throws IgniteException In case of error.
     * @throws org.apache.ignite.ignitefs.IgfsFileNotFoundException If path doesn't exist.
     */
    public IgfsInputStream open(IgfsPath path) throws IgniteException;

    /**
     * Opens a file for reading.
     *
     * @param path File path to read.
     * @param bufSize Read buffer size (bytes) or {@code zero} to use default value.
     * @return File input stream to read data from.
     * @throws IgniteException In case of error.
     * @throws org.apache.ignite.ignitefs.IgfsFileNotFoundException If path doesn't exist.
     */
    @Override public IgfsInputStream open(IgfsPath path, int bufSize) throws IgniteException;

    /**
     * Opens a file for reading.
     *
     * @param path File path to read.
     * @param bufSize Read buffer size (bytes) or {@code zero} to use default value.
     * @param seqReadsBeforePrefetch Amount of sequential reads before prefetch is started.
     * @return File input stream to read data from.
     * @throws IgniteException In case of error.
     * @throws org.apache.ignite.ignitefs.IgfsFileNotFoundException If path doesn't exist.
     */
    public IgfsInputStream open(IgfsPath path, int bufSize, int seqReadsBeforePrefetch) throws IgniteException;

    /**
     * Creates a file and opens it for writing.
     *
     * @param path File path to create.
     * @param overwrite Overwrite file if it already exists. Note: you cannot overwrite an existent directory.
     * @return File output stream to write data to.
     * @throws IgniteException In case of error.
     */
    @Override public IgfsOutputStream create(IgfsPath path, boolean overwrite) throws IgniteException;

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
     * @throws IgniteException In case of error.
     */
    @Override public IgfsOutputStream create(IgfsPath path, int bufSize, boolean overwrite, int replication,
        long blockSize, @Nullable Map<String, String> props) throws IgniteException;

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
     * @throws IgniteException In case of error.
     */
    public IgfsOutputStream create(IgfsPath path, int bufSize, boolean overwrite,
        @Nullable IgniteUuid affKey, int replication, long blockSize, @Nullable Map<String, String> props)
        throws IgniteException;

    /**
     * Opens an output stream to an existing file for appending data.
     *
     * @param path File path to append.
     * @param create Create file if it doesn't exist yet.
     * @return File output stream to append data to.
     * @throws IgniteException In case of error.
     * @throws org.apache.ignite.ignitefs.IgfsFileNotFoundException If path doesn't exist and create flag is {@code false}.
     */
    public IgfsOutputStream append(IgfsPath path, boolean create) throws IgniteException;

    /**
     * Opens an output stream to an existing file for appending data.
     *
     * @param path File path to append.
     * @param bufSize Write buffer size (bytes) or {@code zero} to use default value.
     * @param create Create file if it doesn't exist yet.
     * @param props File properties to set only in case it file was just created.
     * @return File output stream to append data to.
     * @throws IgniteException In case of error.
     * @throws org.apache.ignite.ignitefs.IgfsFileNotFoundException If path doesn't exist and create flag is {@code false}.
     */
    @Override public IgfsOutputStream append(IgfsPath path, int bufSize, boolean create,
        @Nullable Map<String, String> props) throws IgniteException;

    /**
     * Sets last access time and last modification time for a given path. If argument is {@code null},
     * corresponding time will not be changed.
     *
     * @param path Path to update.
     * @param accessTime Optional last access time to set. Value {@code -1} does not update access time.
     * @param modificationTime Optional last modification time to set. Value {@code -1} does not update
     *      modification time.
     * @throws org.apache.ignite.ignitefs.IgfsFileNotFoundException If target was not found.
     * @throws IgniteException If error occurred.
     */
    public void setTimes(IgfsPath path, long accessTime, long modificationTime) throws IgniteException;

    /**
     * Gets affinity block locations for data blocks of the file, i.e. the nodes, on which the blocks
     * are stored.
     *
     * @param path File path to get affinity for.
     * @param start Position in the file to start affinity resolution from.
     * @param len Size of data in the file to resolve affinity for.
     * @return Affinity block locations.
     * @throws IgniteException In case of error.
     * @throws org.apache.ignite.ignitefs.IgfsFileNotFoundException If path doesn't exist.
     */
    public Collection<IgfsBlockLocation> affinity(IgfsPath path, long start, long len) throws IgniteException;

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
     * @throws IgniteException In case of error.
     * @throws org.apache.ignite.ignitefs.IgfsFileNotFoundException If path doesn't exist.
     */
    public Collection<IgfsBlockLocation> affinity(IgfsPath path, long start, long len, long maxLen)
        throws IgniteException;

    /**
     * Gets metrics snapshot for this file system.
     *
     * @return Metrics.
     * @throws IgniteException In case of error.
     */
    public IgfsMetrics metrics() throws IgniteException;

    /**
     * Resets metrics for this file system.
     *
     * @throws IgniteException In case of error.
     */
    public void resetMetrics() throws IgniteException;

    /**
     * Determines size of the file denoted by provided path. In case if path is a directory, then
     * total size of all containing entries will be calculated recursively.
     *
     * @param path File system path.
     * @return Total size.
     * @throws IgniteException In case of error.
     */
    public long size(IgfsPath path) throws IgniteException;

    /**
     * Formats the file system removing all existing entries from it.
     * <p>
     * Supports asynchronous execution (see {@link IgniteAsyncSupport}).
     *
     * @throws IgniteException In case format has failed.
     */
    @IgniteAsyncSupported
    public void format() throws IgniteException;

    /**
     * Executes GGFS task.
     * <p>
     * Supports asynchronous execution (see {@link IgniteAsyncSupport}).
     *
     * @param task Task to execute.
     * @param rslvr Optional resolver to control split boundaries.
     * @param paths Collection of paths to be processed within this task.
     * @param arg Optional task argument.
     * @return Task result.
     * @throws IgniteException If execution failed.
     */
    @IgniteAsyncSupported
    public <T, R> R execute(IgfsTask<T, R> task, @Nullable IgfsRecordResolver rslvr,
        Collection<IgfsPath> paths, @Nullable T arg) throws IgniteException;

    /**
     * Executes GGFS task with overridden maximum range length (see
     * {@link org.apache.ignite.configuration.IgfsConfiguration#getMaximumTaskRangeLength()} for more information).
     * <p>
     * Supports asynchronous execution (see {@link IgniteAsyncSupport}).
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
     * @throws IgniteException If execution failed.
     */
    @IgniteAsyncSupported
    public <T, R> R execute(IgfsTask<T, R> task, @Nullable IgfsRecordResolver rslvr,
        Collection<IgfsPath> paths, boolean skipNonExistentFiles, long maxRangeLen, @Nullable T arg)
        throws IgniteException;

    /**
     * Executes GGFS task.
     * <p>
     * Supports asynchronous execution (see {@link IgniteAsyncSupport}).
     *
     * @param taskCls Task class to execute.
     * @param rslvr Optional resolver to control split boundaries.
     * @param paths Collection of paths to be processed within this task.
     * @param arg Optional task argument.
     * @return Task result.
     * @throws IgniteException If execution failed.
     */
    @IgniteAsyncSupported
    public <T, R> R execute(Class<? extends IgfsTask<T, R>> taskCls,
        @Nullable IgfsRecordResolver rslvr, Collection<IgfsPath> paths, @Nullable T arg) throws IgniteException;

    /**
     * Executes GGFS task with overridden maximum range length (see
     * {@link org.apache.ignite.configuration.IgfsConfiguration#getMaximumTaskRangeLength()} for more information).
     * <p>
     * Supports asynchronous execution (see {@link IgniteAsyncSupport}).
     *
     * @param taskCls Task class to execute.
     * @param rslvr Optional resolver to control split boundaries.
     * @param paths Collection of paths to be processed within this task.
     * @param skipNonExistentFiles Whether to skip non existent files. If set to {@code true} non-existent files will
     *     be ignored. Otherwise an exception will be thrown.
     * @param maxRangeLen Maximum range length.
     * @param arg Optional task argument.
     * @return Task result.
     * @throws IgniteException If execution failed.
     */
    @IgniteAsyncSupported
    public <T, R> R execute(Class<? extends IgfsTask<T, R>> taskCls,
        @Nullable IgfsRecordResolver rslvr, Collection<IgfsPath> paths, boolean skipNonExistentFiles,
        long maxRangeLen, @Nullable T arg) throws IgniteException;

    /** {@inheritDoc} */
    @Override public IgniteFs withAsync();
}
