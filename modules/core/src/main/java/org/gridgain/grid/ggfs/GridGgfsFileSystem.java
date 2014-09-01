/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.ggfs;

import org.gridgain.grid.*;
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
public interface GridGgfsFileSystem extends Closeable {
    /** Property: user name. */
    public static final String PROP_USER_NAME = "usrName";

    /** Property: group name. */
    public static final String PROP_GROUP_NAME = "grpName";

    /** Property: permission. */
    public static final String PROP_PERMISSION = "permission";

    /**
     * Checks if the specified path exists in the file system.
     *
     * @param path Path to check for existence in the file system.
     * @return {@code True} if such file exists, otherwise - {@code false}.
     * @throws GridException In case of error.
     */
    public boolean exists(GridGgfsPath path) throws GridException;

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
     * @throws GridException In case of error.
     */
    @Nullable public GridGgfsFile update(GridGgfsPath path, Map<String, String> props) throws GridException;

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
     * @throws GridException In case of error.
     * @throws GridGgfsFileNotFoundException If source file doesn't exist.
     */
    public void rename(GridGgfsPath src, GridGgfsPath dest) throws GridException;

    /**
     * Deletes file.
     *
     * @param path File path to delete.
     * @param recursive Delete non-empty directories recursively.
     * @return {@code True} in case of success, {@code false} otherwise.
     * @throws GridException In case of error.
     */
    boolean delete(GridGgfsPath path, boolean recursive) throws GridException;

    /**
     * Creates directories under specified path.
     *
     * @param path Path of directories chain to create.
     * @throws GridException In case of error.
     */
    public void mkdirs(GridGgfsPath path) throws GridException;

    /**
     * Creates directories under specified path with the specified properties.
     *
     * @param path Path of directories chain to create.
     * @param props Metadata properties to set on created directories.
     * @throws GridException In case of error.
     */
    public void mkdirs(GridGgfsPath path, @Nullable Map<String, String> props) throws GridException;

    /**
     * Lists file paths under the specified path.
     *
     * @param path Path to list files under.
     * @return List of files under the specified path.
     * @throws GridException In case of error.
     * @throws GridGgfsFileNotFoundException If path doesn't exist.
     */
    public Collection<GridGgfsPath> listPaths(GridGgfsPath path) throws GridException;

    /**
     * Lists files under the specified path.
     *
     * @param path Path to list files under.
     * @return List of files under the specified path.
     * @throws GridException In case of error.
     * @throws GridGgfsFileNotFoundException If path doesn't exist.
     */
    public Collection<GridGgfsFile> listFiles(GridGgfsPath path) throws GridException;

    /**
     * Open file to read data through {@link GridGgfsReader} - the simplest data input interface.
     *
     * @param path Path to the file to open.
     * @param bufSize Buffer size.
     *
     * @return Data reader.
     */
    public GridGgfsReader openFile(GridGgfsPath path, int bufSize);

    /**
     * Create file to write data through {@link GridGgfsWriter}.
     *
     * @param path Path to the file to open.
     * @param overwrite
     * @return
     * @throws GridException
     */

    /**
     * Create file to write data through {@link GridGgfsWriter}.
     *
     * @param path File path to create.
     * @param overwrite Overwrite file if it already exists.
     * @return File output writer to write data to.
     * @throws GridException In case of error.
     */
    public GridGgfsWriter createFile(GridGgfsPath path, boolean overwrite) throws GridException;

    /**
     * Create file to write data through {@link GridGgfsWriter}.
     *
     * @param path File path to create.
     * @param props File properties to set.
     * @param overwrite Overwrite file if it already exists.
     * @param bufSize Write buffer size (bytes) or {@code zero} to use default value.
     * @param replication Replication factor.
     * @param blockSize Block size.
     * @return File output writer to write data to.
     * @throws GridException In case of error.
     */
    public GridGgfsWriter createFile(GridGgfsPath path, Map<String, String> props, boolean overwrite, int bufSize,
        short replication, long blockSize) throws GridException;

    /**
     * Opens the simplest output writer to an existing file for appending data.
     *
     * @param path File path to append.
     * @param bufSize Write buffer size (bytes) or {@code zero} to use default value.
     * @return File output writer to write data to.
     * @throws GridException In case of error.
     */
    public GridGgfsWriter appendFile(GridGgfsPath path, int bufSize) throws GridException;

    /**
     * Return a file status object that represents the path.
     *
     * @param path The path we want information from.
     * @return FileStatus object or {@code null} if file not exists.
     * @throws GridException In case of error.
     */
    @Nullable public GridGgfsFileStatus getFileStatus(GridGgfsPath path) throws GridException;

    /**
     * Gets used space in bytes.
     *
     * @return Used space in bytes.
     * @throws GridException In case of error.
     */
    public long usedSpaceSize() throws GridException;

    /**
     * Gets the file system URI if this set.
     *
     * @return URI or {@code null}.
     */
    @Nullable public String uri();

    /**
     * Gets the implementation specific properties of file system.
     *
     * @return
     */
    @Nullable public Map<String,String> properties();
}
