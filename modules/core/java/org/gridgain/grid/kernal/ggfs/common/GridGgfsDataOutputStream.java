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
 * Data output stream implementing ObjectOutput but throwing exceptions on methods working with objects.
 */
public class GridGgfsDataOutputStream extends DataOutputStream implements ObjectOutput {
    /**
     * Creates a new data output stream to write data to the specified
     * underlying output stream. The counter <code>written</code> is
     * set to zero.
     *
     * @param   out   the underlying output stream, to be saved for later
     *                use.
     * @see     FilterOutputStream#out
     */
    public GridGgfsDataOutputStream(OutputStream out) {
        super(out);
    }

    /** {@inheritDoc} */
    @Override public void writeObject(Object obj) throws IOException {
        throw new IOException("This method must not be invoked on GGFS data output stream.");
    }
}
