/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.fs;

import org.apache.hadoop.fs.*;
import org.gridgain.grid.ggfs.hadoop.v1.*;

/**
 * Utilities for configuring file systems to support the separate working directory per each thread.
 */
public class GridHadoopFileSystemsUtils {
    /** Name of the property for setting working directory on create new local FS instance. */
    public static String LOCAL_FS_WORK_DIR_PROPERTY = "fs." + FsConstants.LOCAL_FS_URI.getScheme() + ".workDir";

    /**
     * Set user name and default working directory for current thread if it's supported by file system.
     *
     * @param fs File system.
     * @param userName User name.
     */
    public static void setUser(FileSystem fs, String userName) {
        if (fs instanceof GridGgfsHadoopFileSystem)
            ((GridGgfsHadoopFileSystem)fs).setUser(userName);
    }
}
