/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.hadoop.impl.fs;

import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;

/**
 * Local file system replacement for Hadoop jobs.
 */
public class HadoopLocalFileSystemV1 extends LocalFileSystem {
    /**
     * Creates new local file system.
     */
    public HadoopLocalFileSystemV1() {
        super(new HadoopRawLocalFileSystem());
    }

    /** {@inheritDoc} */
    @Override public File pathToFile(Path path) {
        return ((HadoopRawLocalFileSystem)getRaw()).convert(path);
    }
}