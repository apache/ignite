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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.igfs.IgfsBlockLocation;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.IgfsPathSummary;
import org.apache.ignite.internal.processors.igfs.IgfsHandshakeResponse;
import org.apache.ignite.internal.processors.igfs.IgfsStatus;
import org.jetbrains.annotations.Nullable;

/**
 * Facade for communication with grid.
 */
public interface HadoopIgfs {
    /**
     * Perform handshake.
     *
     * @param logDir Log directory.
     * @return Future with handshake result.
     * @throws IgniteCheckedException If failed.
     */
    public IgfsHandshakeResponse handshake(String logDir) throws IgniteCheckedException, IOException;

    /**
     * Close connection.
     *
     * @param force Force flag.
     */
    public void close(boolean force);

    /**
     * Command to retrieve file info for some IGFS path.
     *
     * @param path Path to get file info for.
     * @return Future for info operation.
     * @throws IgniteCheckedException If failed.
     */
    public IgfsFile info(IgfsPath path) throws IgniteCheckedException, IOException;

    /**
     * Command to update file properties.
     *
     * @param path IGFS path to update properties.
     * @param props Properties to update.
     * @return Future for update operation.
     * @throws IgniteCheckedException If failed.
     */
    public IgfsFile update(IgfsPath path, Map<String, String> props) throws IgniteCheckedException, IOException;

    /**
     * Sets last access time and last modification time for a file.
     *
     * @param path Path to update times.
     * @param accessTime Last access time to set.
     * @param modificationTime Last modification time to set.
     * @throws IgniteCheckedException If failed.
     */
    public Boolean setTimes(IgfsPath path, long accessTime, long modificationTime) throws IgniteCheckedException,
        IOException;

    /**
     * Command to rename given path.
     *
     * @param src Source path.
     * @param dest Destination path.
     * @return Future for rename operation.
     * @throws IgniteCheckedException If failed.
     */
    public Boolean rename(IgfsPath src, IgfsPath dest) throws IgniteCheckedException, IOException;

    /**
     * Command to delete given path.
     *
     * @param path Path to delete.
     * @param recursive {@code True} if deletion is recursive.
     * @return Future for delete operation.
     * @throws IgniteCheckedException If failed.
     */
    public Boolean delete(IgfsPath path, boolean recursive) throws IgniteCheckedException, IOException;

    /**
     * Command to get affinity for given path, offset and length.
     *
     * @param path Path to get affinity for.
     * @param start Start position (offset).
     * @param len Data length.
     * @return Future for affinity command.
     * @throws IgniteCheckedException If failed.
     */
    public Collection<IgfsBlockLocation> affinity(IgfsPath path, long start, long len) throws IgniteCheckedException,
        IOException;

    /**
     * Gets path summary.
     *
     * @param path Path to get summary for.
     * @return Future that will be completed when summary is received.
     * @throws IgniteCheckedException If failed.
     */
    public IgfsPathSummary contentSummary(IgfsPath path) throws IgniteCheckedException, IOException;

    /**
     * Command to create directories.
     *
     * @param path Path to create.
     * @return Future for mkdirs operation.
     * @throws IgniteCheckedException If failed.
     */
    public Boolean mkdirs(IgfsPath path, Map<String, String> props) throws IgniteCheckedException, IOException;

    /**
     * Command to get list of files in directory.
     *
     * @param path Path to list.
     * @return Future for listFiles operation.
     * @throws IgniteCheckedException If failed.
     */
    public Collection<IgfsFile> listFiles(IgfsPath path) throws IgniteCheckedException, IOException;

    /**
     * Command to get directory listing.
     *
     * @param path Path to list.
     * @return Future for listPaths operation.
     * @throws IgniteCheckedException If failed.
     */
    public Collection<IgfsPath> listPaths(IgfsPath path) throws IgniteCheckedException, IOException;

    /**
     * Performs status request.
     *
     * @return Status response.
     * @throws IgniteCheckedException If failed.
     */
    public IgfsStatus fsStatus() throws IgniteCheckedException, IOException;

    /**
     * Command to open file for reading.
     *
     * @param path File path to open.
     * @return Future for open operation.
     * @throws IgniteCheckedException If failed.
     */
    public HadoopIgfsStreamDelegate open(IgfsPath path) throws IgniteCheckedException, IOException;

    /**
     * Command to open file for reading.
     *
     * @param path File path to open.
     * @return Future for open operation.
     * @throws IgniteCheckedException If failed.
     */
    public HadoopIgfsStreamDelegate open(IgfsPath path, int seqReadsBeforePrefetch) throws IgniteCheckedException,
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
     * @throws IgniteCheckedException If failed.
     */
    public HadoopIgfsStreamDelegate create(IgfsPath path, boolean overwrite, boolean colocate,
        int replication, long blockSize, @Nullable Map<String, String> props) throws IgniteCheckedException, IOException;

    /**
     * Open file for output appending data to the end of a file.
     *
     * @param path Path to file.
     * @param create If {@code true}, file will be created if does not exist.
     * @param props File properties.
     * @return Stream descriptor.
     * @throws IgniteCheckedException If failed.
     */
    public HadoopIgfsStreamDelegate append(IgfsPath path, boolean create,
        @Nullable Map<String, String> props) throws IgniteCheckedException, IOException;
}