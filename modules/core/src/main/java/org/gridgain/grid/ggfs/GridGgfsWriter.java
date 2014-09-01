/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.ggfs;

import org.gridgain.grid.*;

import java.io.*;

/**
 * The simplest data output interface to write to secondary file system in dual modes.
 */
public interface GridGgfsWriter extends Closeable {
    /**
     * Writes <code>data.length</code> bytes from the specified byte array to this output.
     * @param data The data.
     * @throws GridException In case of error.
     */
    void write(byte[] data) throws GridException;
}
