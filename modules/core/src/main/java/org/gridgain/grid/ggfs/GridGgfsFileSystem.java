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
 *
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
     * @throws org.gridgain.grid.GridException In case of error.
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
     * @throws org.gridgain.grid.GridException In case of error.
     */
    @Nullable GridGgfsFile update(GridGgfsPath path, Map<String, String> props) throws GridException;

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
     * @throws org.gridgain.grid.GridException In case of error.
     * @throws org.gridgain.grid.ggfs.GridGgfsFileNotFoundException If source file doesn't exist.
     */
    void rename(GridGgfsPath src, GridGgfsPath dest) throws GridException;

    /**
     * Deletes file.
     *
     * @param path File path to delete.
     * @param recursive Delete non-empty directories recursively.
     * @return {@code True} in case of success, {@code false} otherwise.
     * @throws org.gridgain.grid.GridException In case of error.
     */
    boolean delete(GridGgfsPath path, boolean recursive) throws GridException;

    /**
     * Creates directories under specified path.
     *
     * @param path Path of directories chain to create.
     * @throws org.gridgain.grid.GridException In case of error.
     */
    void mkdirs(GridGgfsPath path) throws GridException;

    /**
     * Creates directories under specified path with the specified properties.
     *
     * @param path Path of directories chain to create.
     * @param props Metadata properties to set on created directories.
     * @throws org.gridgain.grid.GridException In case of error.
     */
    void mkdirs(GridGgfsPath path, @Nullable Map<String, String> props) throws GridException;

    /**
     * Lists file paths under the specified path.
     *
     * @param path Path to list files under.
     * @return List of files under the specified path.
     * @throws org.gridgain.grid.GridException In case of error.
     * @throws org.gridgain.grid.ggfs.GridGgfsFileNotFoundException If path doesn't exist.
     */
    Collection<GridGgfsPath> listPaths(GridGgfsPath path) throws GridException;

    /**
     * Lists files under the specified path.
     *
     * @param path Path to list files under.
     * @return List of files under the specified path.
     * @throws org.gridgain.grid.GridException In case of error.
     * @throws org.gridgain.grid.ggfs.GridGgfsFileNotFoundException If path doesn't exist.
     */
    Collection<GridGgfsFile> listFiles(GridGgfsPath path) throws GridException;

    /**
     *
     * @param path
     * @param bufSize
     * @return
     */
    GridGgfsReader openFile(GridGgfsPath path, int bufSize);

    /**
     *
     * @param path
     * @param overwrite
     * @return
     * @throws GridException
     */
    GridGgfsWriter createFile(GridGgfsPath path, boolean overwrite) throws GridException;

    /**
     *
     * @param path
     * @param props
     * @param overwrite
     * @param bufSize
     * @param replication
     * @param blockSize
     * @return
     * @throws GridException
     */
    GridGgfsWriter createFile(GridGgfsPath path, Map<String, String> props, boolean overwrite, int bufSize,
        short replication, long blockSize) throws GridException;

    /**
     *
     * @param path
     * @param bufSize
     * @return
     * @throws GridException
     */
    GridGgfsWriter appendFile(GridGgfsPath path, int bufSize) throws GridException;

    /**
     *
     * @param path
     * @return
     * @throws GridException
     */
    @Nullable GridGgfsFileStatus getFileStatus(GridGgfsPath path) throws GridException;

    long usedSpaceSize() throws GridException;
}
