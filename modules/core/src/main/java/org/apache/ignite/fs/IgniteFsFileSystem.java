/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.fs;

import org.apache.ignite.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Common file system interface. It provides a typical generalized "view" of any file system:
 * <ul>
 *     <li>list directories or get information for a single path</li>
 *     <li>create/move/delete files or directories</li>
 *     <li>write/read data streams into/from files</li>
 * </ul>
 *
 * This is the minimum of functionality that is needed to work as secondary file system in dual modes of GGFS.
 */
public interface IgniteFsFileSystem {
    /** File property: user name. */
    public static final String PROP_USER_NAME = "usrName";

    /** File property: group name. */
    public static final String PROP_GROUP_NAME = "grpName";

    /** File property: permission. */
    public static final String PROP_PERMISSION = "permission";

    /**
     * Checks if the specified path exists in the file system.
     *
     * @param path Path to check for existence in the file system.
     * @return {@code True} if such file exists, otherwise - {@code false}.
     * @throws IgniteCheckedException In case of error.
     */
    public boolean exists(IgniteFsPath path) throws IgniteCheckedException;

    /**
     * Updates file information for the specified path. Existent properties, not listed in the passed collection,
     * will not be affected. Other properties will be added or overwritten. Passed properties with {@code null} values
     * will be removed from the stored properties or ignored if they don't exist in the file info.
     * <p>
     * When working in {@code DUAL_SYNC} or {@code DUAL_ASYNC} modes only the following properties will be propagated
     * to the secondary file system:
     * <ul>
     * <li>{@code usrName} - file owner name;</li>
     * <li>{@code grpName} - file owner group;</li>
     * <li>{@code permission} - Unix-style string representing file permissions.</li>
     * </ul>
     *
     * @param path File path to set properties for.
     * @param props Properties to update.
     * @return File information for specified path or {@code null} if such path does not exist.
     * @throws IgniteCheckedException In case of error.
     */
    @Nullable public IgniteFsFile update(IgniteFsPath path, Map<String, String> props) throws IgniteCheckedException;

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
     * @throws IgniteCheckedException In case of error.
     * @throws IgniteFsFileNotFoundException If source file doesn't exist.
     */
    public void rename(IgniteFsPath src, IgniteFsPath dest) throws IgniteCheckedException;

    /**
     * Deletes file.
     *
     * @param path File path to delete.
     * @param recursive Delete non-empty directories recursively.
     * @return {@code True} in case of success, {@code false} otherwise.
     * @throws IgniteCheckedException In case of error.
     */
    boolean delete(IgniteFsPath path, boolean recursive) throws IgniteCheckedException;

    /**
     * Creates directories under specified path.
     *
     * @param path Path of directories chain to create.
     * @throws IgniteCheckedException In case of error.
     */
    public void mkdirs(IgniteFsPath path) throws IgniteCheckedException;

    /**
     * Creates directories under specified path with the specified properties.
     *
     * @param path Path of directories chain to create.
     * @param props Metadata properties to set on created directories.
     * @throws IgniteCheckedException In case of error.
     */
    public void mkdirs(IgniteFsPath path, @Nullable Map<String, String> props) throws IgniteCheckedException;

    /**
     * Lists file paths under the specified path.
     *
     * @param path Path to list files under.
     * @return List of files under the specified path.
     * @throws IgniteCheckedException In case of error.
     * @throws IgniteFsFileNotFoundException If path doesn't exist.
     */
    public Collection<IgniteFsPath> listPaths(IgniteFsPath path) throws IgniteCheckedException;

    /**
     * Lists files under the specified path.
     *
     * @param path Path to list files under.
     * @return List of files under the specified path.
     * @throws IgniteCheckedException In case of error.
     * @throws IgniteFsFileNotFoundException If path doesn't exist.
     */
    public Collection<IgniteFsFile> listFiles(IgniteFsPath path) throws IgniteCheckedException;

    /**
     * Opens a file for reading.
     *
     * @param path File path to read.
     * @param bufSize Read buffer size (bytes) or {@code zero} to use default value.
     * @return File input stream to read data from.
     * @throws IgniteCheckedException In case of error.
     * @throws IgniteFsFileNotFoundException If path doesn't exist.
     */
    public IgniteFsReader open(IgniteFsPath path, int bufSize) throws IgniteCheckedException;

    /**
     * Creates a file and opens it for writing.
     *
     * @param path File path to create.
     * @param overwrite Overwrite file if it already exists. Note: you cannot overwrite an existent directory.
     * @return File output stream to write data to.
     * @throws IgniteCheckedException In case of error.
     */
    public OutputStream create(IgniteFsPath path, boolean overwrite) throws IgniteCheckedException;

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
     * @throws IgniteCheckedException In case of error.
     */
    public OutputStream create(IgniteFsPath path, int bufSize, boolean overwrite, int replication, long blockSize,
       @Nullable Map<String, String> props) throws IgniteCheckedException;

    /**
     * Opens an output stream to an existing file for appending data.
     *
     * @param path File path to append.
     * @param bufSize Write buffer size (bytes) or {@code zero} to use default value.
     * @param create Create file if it doesn't exist yet.
     * @param props File properties to set only in case it file was just created.
     * @return File output stream to append data to.
     * @throws IgniteCheckedException In case of error.
     * @throws IgniteFsFileNotFoundException If path doesn't exist and create flag is {@code false}.
     */
    public OutputStream append(IgniteFsPath path, int bufSize, boolean create, @Nullable Map<String, String> props)
        throws IgniteCheckedException;

    /**
     * Gets file information for the specified path.
     *
     * @param path Path to get information for.
     * @return File information for specified path or {@code null} if such path does not exist.
     * @throws IgniteCheckedException In case of error.
     */
    @Nullable public IgniteFsFile info(IgniteFsPath path) throws IgniteCheckedException;

    /**
     * Gets used space in bytes.
     *
     * @return Used space in bytes.
     * @throws IgniteCheckedException In case of error.
     */
    public long usedSpaceSize() throws IgniteCheckedException;

    /**
     * Gets the implementation specific properties of file system.
     *
     * @return Map of properties.
     */
    public Map<String,String> properties();
}
