package org.gridgain.grid.ggfs;

import org.gridgain.grid.GridException;

import java.io.*;

public interface GridGgfsWriter extends Closeable {
    void write(byte[] data) throws GridException;
}
