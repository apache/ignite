/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.ggfs;

import java.util.*;

/**
 * Interface that represents the client side information for a file.
 */
public interface GridGgfsFileStatus {
    /**
     * Is this a directory?
     *
     * @return {@code true} If this is a directory.
     */
    public boolean isDirectory();

    /**
     * Get the block size of the file.
     *
     * @return The number of bytes.
     */
    public int blockSize();

    /**
     * Get the length of this file, in bytes.
     *
     * @return The length of this file, in bytes.
     */
    public long length();

    /**
     * Get properties of the file.
     *
     * @return Properties of the file.
     */
    public Map<String,String> properties();
}
