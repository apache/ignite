/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.ggfs;

import org.jetbrains.annotations.*;

import java.util.*;

/**
 * {@code GGFS} file or directory descriptor. For example, to get information about
 * a file you would use the following code:
 * <pre name="code" class="java">
 *     GridGgfsPath filePath = new GridGgfsPath("my/working/dir", "file.txt");
 *
 *     // Get metadata about file.
 *     GridGgfsFile file = ggfs.info(filePath);
 * </pre>
 */
public interface GridGgfsFile {
    /**
     * Gets path to file.
     *
     * @return Path to file.
     */
    public GridGgfsPath path();

    /**
     * Check this file is a data file.
     *
     * @return {@code True} if this is a data file.
     */
    public boolean isFile();

    /**
     * Check this file is a directory.
     *
     * @return {@code True} if this is a directory.
     */
    public boolean isDirectory();

    /**
     * Gets file's length.
     *
     * @return File's length or {@code zero} for directories.
     */
    public long length();

    /**
     * Gets file's data block size.
     *
     * @return File's data block size or {@code zero} for directories.
     */
    public int blockSize();

    /**
     * Gets file group block size (i.e. block size * group size).
     *
     * @return File group block size.
     */
    public long groupBlockSize();

    /**
     * Gets file last access time. File last access time is not updated automatically due to
     * performance considerations and can be updated on demand with
     * {@link org.apache.ignite.IgniteFs#setTimes(GridGgfsPath, long, long)} method.
     * <p>
     * By default last access time equals file creation time.
     *
     * @return Last access time.
     */
    public long accessTime();

    /**
     * Gets file last modification time. File modification time is updated automatically on each file write and
     * append.
     *
     * @return Last modification time.
     */
    public long modificationTime();

    /**
     * Get file's property for specified name.
     *
     * @param name Name of the property.
     * @return File's property for specified name.
     * @throws IllegalArgumentException If requested property was not found.
     */
    public String property(String name) throws IllegalArgumentException;

    /**
     * Get file's property for specified name.
     *
     * @param name Name of the property.
     * @param dfltVal Default value if requested property was not found.
     * @return File's property for specified name.
     */
    @Nullable public String property(String name, @Nullable String dfltVal);

    /**
     * Get properties of the file.
     *
     * @return Properties of the file.
     */
    public Map<String, String> properties();
}
