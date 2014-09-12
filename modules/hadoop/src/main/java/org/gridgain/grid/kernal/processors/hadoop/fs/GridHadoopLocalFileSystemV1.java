/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.fs;

import org.apache.hadoop.fs.*;

import java.io.*;

/**
 * Local file system replacement for Hadoop jobs.
 */
public class GridHadoopLocalFileSystemV1 extends LocalFileSystem {
    /**
     * Creates new local file system.
     */
    public GridHadoopLocalFileSystemV1() {
        super(new GridHadoopRawLocalFileSystem());
    }

    /** {@inheritDoc} */
    @Override public File pathToFile(Path path) {
        return ((GridHadoopRawLocalFileSystem)getRaw()).convert(path);
    }
}
