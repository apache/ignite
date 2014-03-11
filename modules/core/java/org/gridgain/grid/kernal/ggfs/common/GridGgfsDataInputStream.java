/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.common;

import java.io.*;

/**
 * Data input stream implementing object input but throwing exceptions on object methods.
 */
public class GridGgfsDataInputStream extends DataInputStream implements ObjectInput {
    /**
     * Creates a DataInputStream that uses the specified
     * underlying InputStream.
     *
     * @param  in The specified input stream
     */
    public GridGgfsDataInputStream(InputStream in) {
        super(in);
    }

    /** {@inheritDoc} */
    @Override public Object readObject() throws ClassNotFoundException, IOException {
        throw new IOException("This method must not be invoked on GGFS data input stream.");
    }
}
