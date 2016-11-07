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

package org.apache.ignite.internal.processors.igfs.secondary.local;

import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Map;

/**
 * Implementation of the IgfsFile interface for the local filesystem.
 */
public class LocalFileSystemIgfsFile implements IgfsFile {
    /** Path. */
    private final IgfsPath path;

    /** Flags. */
    private final byte flags;

    /** Block size. */
    private final int blockSize;

    /** Modification time. */
    private final long modTime;

    /** Length. */
    private final long len;

    /** Properties. */
    private final Map<String, String> props;

    /**
     * @param path IGFS path.
     * @param isFile Path is a file.
     * @param isDir Path is a directory.
     * @param blockSize Block size in bytes.
     * @param modTime Modification time in millis.
     * @param len File length in bytes.
     * @param props Properties.
     */
    public LocalFileSystemIgfsFile(IgfsPath path, boolean isFile, boolean isDir, int blockSize,
        long modTime, long len, Map<String, String> props) {

        assert !isDir || blockSize == 0 : "blockSize must be 0 for dirs. [blockSize=" + blockSize + ']';
        assert !isDir || len == 0 : "length must be 0 for dirs. [length=" + len + ']';

        this.path = path;
        this.flags = IgfsUtils.flags(isDir, isFile);
        this.blockSize = blockSize;
        this.modTime = modTime;
        this.len = len;
        this.props = props;
    }

    /** {@inheritDoc} */
    @Override public IgfsPath path() {
        return path;
    }

    /** {@inheritDoc} */
    @Override public boolean isFile() {
        return IgfsUtils.isFile(flags);
    }

    /** {@inheritDoc} */
    @Override public boolean isDirectory() {
        return IgfsUtils.isDirectory(flags);
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
        return modTime;
    }

    /** {@inheritDoc} */
    @Override public String property(String name) throws IllegalArgumentException {
        return property(name, null);
    }

    /** {@inheritDoc} */
    @Nullable @Override public String property(String name, @Nullable String dfltVal) {
        if (props != null) {
            String res = props.get(name);

            if (res != null)
                return res;
        }

        return dfltVal;
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> properties() {
        return props != null ? props : Collections.<String, String>emptyMap();
    }

    /** {@inheritDoc} */
    @Override public long length() {
        return len;
    }
}
