package org.gridgain.grid.ggfs;

import java.util.Map;

public interface GridGgfsFileStatus {
    boolean isDir();

    int getBlockSize();

    long getLen();

    Map<String,String> properties();
}
