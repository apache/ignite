/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gridgain.grid.kernal.processors.hadoop.fs;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.protocol.*;
import org.gridgain.grid.ggfs.hadoop.v1.*;

/**
 * Utilities for configuring file systems to support the separate working directory per each thread.
 */
public class GridHadoopFileSystemsUtils {
    /** Name of the property for setting working directory on create new local FS instance. */
    public static final String LOC_FS_WORK_DIR_PROP = "fs." + FsConstants.LOCAL_FS_URI.getScheme() + ".workDir";

    /**
     * Set user name and default working directory for current thread if it's supported by file system.
     *
     * @param fs File system.
     * @param userName User name.
     */
    public static void setUser(FileSystem fs, String userName) {
        if (fs instanceof GridGgfsHadoopFileSystem)
            ((GridGgfsHadoopFileSystem)fs).setUser(userName);
        else if (fs instanceof GridHadoopDistributedFileSystem)
            ((GridHadoopDistributedFileSystem)fs).setUser(userName);
    }

    /**
     * Setup wrappers of filesystems to support the separate working directory.
     *
     * @param cfg Config for setup.
     */
    public static void setupFileSystems(Configuration cfg) {
        cfg.set("fs." + FsConstants.LOCAL_FS_URI.getScheme() + ".impl", GridHadoopLocalFileSystemV1.class.getName());
        cfg.set("fs.AbstractFileSystem." + FsConstants.LOCAL_FS_URI.getScheme() + ".impl",
                GridHadoopLocalFileSystemV2.class.getName());

        cfg.set("fs." + HdfsConstants.HDFS_URI_SCHEME + ".impl", GridHadoopDistributedFileSystem.class.getName());
    }
}
