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

package org.apache.ignite.igfs.secondary;

import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsPath;
import org.jetbrains.annotations.Nullable;

/**
 * Secondary file system interface.
 */
public interface IgfsSecondaryFileSystem {
    /**
     * Checks if the specified path exists.
     *
     * @param path Path to check for existence.
     * @return {@code True} if such file exists, otherwise - {@code false}.
     * @throws IgniteException In case of error.
     */
    public boolean exists(IgfsPath path);

    /**
     * Updates file information for the specified path. Existent properties, not listed in the passed collection,
     * will not be affected. Other properties will be added or overwritten. Passed properties with {@code null} values
     * will be removed from the stored properties or ignored if they don't exist in the file info.
     * <p>
     * When working in {@code DUAL_SYNC} or {@code DUAL_ASYNC} modes with Hadoop secondary file system only the
     * following properties will be updated on the secondary file system:
     * <ul>
     * <li>{@code usrName} - file owner name;</li>
     * <li>{@code grpName} - file owner group;</li>
     * <li>{@code permission} - Unix-style string representing file permissions.</li>
     * </ul>
     *
     * @param path File path to set properties for.
     * @param props Properties to update.
     * @return File information for specified path or {@code null} if such path does not exist.
     * @throws IgniteException In case of error.
     */
    public IgfsFile update(IgfsPath path, Map<String, String> props) throws IgniteException;

    /**
     * Renames/moves a file.
     * <p>
     * You are free to rename/move data files as you wish, but directories can be only renamed.
     * You cannot move the directory between different parent directories.
     * <p>
     * Examples:
     * <ul>
     *     <li>"/work/file.txt" => "/home/project/Presentation Scenario.txt"</li>
     *     <li>"/work" => "/work-2012.bkp"</li>
     *     <li>"/work" => "<strike>/backups/work</strike>" - such operation is restricted for directories.</li>
     * </ul>
     *
     * @param src Source file path to rename.
     * @param dest Destination file path. If destination path is a directory, then source file will be placed
     *     into destination directory with original name.
     * @throws IgniteException In case of error.
     * @throws org.apache.ignite.igfs.IgfsPathNotFoundException If source file doesn't exist.
     */
    public void rename(IgfsPath src, IgfsPath dest) throws IgniteException;

    /**
     * Deletes file.
     *
     * @param path File path to delete.
     * @param recursive Delete non-empty directories recursively.
     * @return {@code True} in case of success, {@code false} otherwise.
     * @throws IgniteException In case of error.
     */
    public boolean delete(IgfsPath path, boolean recursive) throws IgniteException;

    /**
     * Creates directories under specified path.
     *
     * @param path Path of directories chain to create.
     * @throws IgniteException In case of error.
     */
    public void mkdirs(IgfsPath path) throws IgniteException;

    /**
     * Creates directories under specified path with the specified properties.
     *
     * @param path Path of directories chain to create.
     * @param props Metadata properties to set on created directories.
     * @throws IgniteException In case of error.
     */
    public void mkdirs(IgfsPath path, @Nullable Map<String, String> props) throws IgniteException;

    /**
     * Lists file paths under the specified path.
     *
     * @param path Path to list files under.
     * @return List of paths under the specified path.
     * @throws IgniteException In case of error.
     * @throws org.apache.ignite.igfs.IgfsPathNotFoundException If path doesn't exist.
     */
    public Collection<IgfsPath> listPaths(IgfsPath path) throws IgniteException;

    /**
     * Lists files under the specified path.
     *
     * @param path Path to list files under.
     * @return List of files under the specified path.
     * @throws IgniteException In case of error.
     * @throws org.apache.ignite.igfs.IgfsPathNotFoundException If path doesn't exist.
     */
    public Collection<IgfsFile> listFiles(IgfsPath path) throws IgniteException;

    /**
     * Opens a file for reading.
     *
     * @param path File path to read.
     * @param bufSize Read buffer size (bytes) or {@code zero} to use default value.
     * @return File input stream to read data from.
     * @throws IgniteException In case of error.
     * @throws org.apache.ignite.igfs.IgfsPathNotFoundException If path doesn't exist.
     */
    public IgfsSecondaryFileSystemPositionedReadable open(IgfsPath path, int bufSize) throws IgniteException;

    /**
     * Creates a file and opens it for writing.
     *
     * @param path File path to create.
     * @param overwrite Overwrite file if it already exists. Note: you cannot overwrite an existent directory.
     * @return File output stream to write data to.
     * @throws IgniteException In case of error.
     */
    public OutputStream create(IgfsPath path, boolean overwrite) throws IgniteException;

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
    public OutputStream create(IgfsPath path, int bufSize, boolean overwrite, int replication, long blockSize,
       @Nullable Map<String, String> props) throws IgniteException;

    /**
     * Opens an output stream to an existing file for appending data.
     *
     * @param path File path to append.
     * @param bufSize Write buffer size (bytes) or {@code zero} to use default value.
     * @param create Create file if it doesn't exist yet.
     * @param props File properties to set only in case it file was just created.
     * @return File output stream to append data to.
     * @throws IgniteException In case of error.
     * @throws org.apache.ignite.igfs.IgfsPathNotFoundException If path doesn't exist and create flag is {@code false}.
     */
    public OutputStream append(IgfsPath path, int bufSize, boolean create, @Nullable Map<String, String> props)
        throws IgniteException;

    /**
     * Gets file information for the specified path.
     *
     * @param path Path to get information for.
     * @return File information for specified path or {@code null} if such path does not exist.
     * @throws IgniteException In case of error.
     */
    public IgfsFile info(IgfsPath path) throws IgniteException;

    /**
     * Gets used space in bytes.
     *
     * @return Used space in bytes.
     * @throws IgniteException In case of error.
     */
    public long usedSpaceSize() throws IgniteException;
}