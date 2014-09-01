/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;

import java.io.*;

/**
 * Adapter for use any {@link OutputStream} as simplest output interface {@link GridGgfsWriter}.
 */
public class GridGgfsOutputStreamWriter implements GridGgfsWriter {
    /** */
    private OutputStream outputStream;

    /**
     * Constructor.
     * @param outputStream Ouput stream to wrap.
     */
    public GridGgfsOutputStreamWriter(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    /** {@inheritDoc} */
    @Override public void write(byte[] data) throws GridException {
        try {
            outputStream.write(data);
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        outputStream.close();
    }
}
