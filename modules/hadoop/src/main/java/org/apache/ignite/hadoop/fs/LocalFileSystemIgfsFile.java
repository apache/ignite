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

package org.apache.ignite.hadoop.fs;

import java.util.Map;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsPath;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of the IfgsFile interface for the local filesystem.
 */
public class LocalFileSystemIgfsFile implements IgfsFile {
    /** Path. */
    private final IgfsPath path;

    /** Is file. */
    private final boolean isFile;

    /** Is directory. */
    private final boolean isDirectory;

    /** Block size. */
    private final int blockSize;

    /** Modification time. */
    private final long modificationTime;

    /** Length. */
    private final long length;

    /** Properties. */
    private final Map<String, String> props;

    /**
     * @param path IGFS path.
     * @param isFile Path is a file.
     * @param isDirectory Path is a directory.
     * @param blockSize Block size in bytes.
     * @param modificationTime Modification time in millis.
     * @param length File length in bytes.
     * @param props Properties.
     */
    public LocalFileSystemIgfsFile(IgfsPath path, boolean isFile, boolean isDirectory, int blockSize,
        long modificationTime, long length, Map<String, String> props) {

        if (isDirectory)
            assert blockSize == 0 : "blockSize must be 0 for dirs. [blockSize=" + blockSize +']';

        if (isDirectory)
            assert length == 0 : "length must be 0 for dirs. [length=" + length +']';

        this.path = path;
        this.isFile = isFile;
        this.isDirectory = isDirectory;
        this.blockSize = blockSize;
        this.modificationTime = modificationTime;
        this.length = length;
        this.props = props;
    }

    /** {@inheritDoc} */
    @Override public IgfsPath path() {
        return path;
    }

    /** {@inheritDoc} */
    @Override public boolean isFile() {
        return isFile;
    }

    /** {@inheritDoc} */
    @Override public boolean isDirectory() {
        return isDirectory;
    }

    /** {@inheritDoc} */
    @Override public int blockSize() {
        return blockSize;
    }

    /** {@inheritDoc} */
    @Override public long groupBlockSize() {
        return blockSize();
    }

    /** {@inheritDoc} */
    @Override public long accessTime() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long modificationTime() {
        return modificationTime;
    }

    /** {@inheritDoc} */
    @Override public String property(String name) throws IllegalArgumentException {
        return props.get(name);
    }

    /** {@inheritDoc} */
    @Nullable @Override public String property(String name, @Nullable String dfltVal) {
        String res = props.get(name);
        if (res == null)
            return dfltVal;
        else
            return res;
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> properties() {
        return props;
    }

    /** {@inheritDoc} */
    @Override public long length() {
        return length;
    }
}
