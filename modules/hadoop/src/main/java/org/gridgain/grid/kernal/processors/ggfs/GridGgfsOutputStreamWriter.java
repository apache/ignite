package org.gridgain.grid.kernal.processors.ggfs;

import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;

import java.io.*;

public class GridGgfsOutputStreamWriter implements GridGgfsWriter {
    private OutputStream outputStream;

    public GridGgfsOutputStreamWriter(OutputStream outputStream) {

        this.outputStream = outputStream;
    }

    @Override public void write(byte[] data) throws GridException {
        try {
            outputStream.write(data);
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }

    @Override public void close() throws IOException {
        outputStream.close();
    }
}
