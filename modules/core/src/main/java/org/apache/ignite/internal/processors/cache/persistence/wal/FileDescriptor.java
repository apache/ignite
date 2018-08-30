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

package org.apache.ignite.internal.processors.cache.persistence.wal;

import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * WAL file descriptor.
 */
public class FileDescriptor implements Comparable<FileDescriptor>, AbstractWalRecordsIterator.AbstractFileDescriptor {

    /** file extension of WAL segment. */
    private static final String WAL_SEGMENT_FILE_EXT = ".wal";

    /** Length of WAL segment file name. */
    private static final int WAL_SEGMENT_FILE_NAME_LENGTH = 16;

    /** File represented by this class. */
    protected final File file;

    /** Absolute WAL segment file index. */
    protected final long idx;

    /**
     * Creates file descriptor. Index is restored from file name.
     *
     * @param file WAL segment file.
     */
    public FileDescriptor(@NotNull File file) {
        this(file, null);
    }

    /**
     * @param file WAL segment file.
     * @param idx Absolute WAL segment file index. For null value index is restored from file name.
     */
    public FileDescriptor(@NotNull File file, @Nullable Long idx) {
        this.file = file;

        String fileName = file.getName();

        assert fileName.contains(WAL_SEGMENT_FILE_EXT);

        this.idx = idx == null ? Long.parseLong(fileName.substring(0, WAL_SEGMENT_FILE_NAME_LENGTH)) : idx;
    }

    /**
     * @param segment Segment index.
     * @return Segment file name.
     */
    public static String fileName(long segment) {
        SB b = new SB();

        String segmentStr = Long.toString(segment);

        for (int i = segmentStr.length(); i < WAL_SEGMENT_FILE_NAME_LENGTH; i++)
            b.a('0');

        b.a(segmentStr).a(WAL_SEGMENT_FILE_EXT);

        return b.toString();
    }

    /** {@inheritDoc} */
    @Override public int compareTo(@NotNull FileDescriptor o) {
        return Long.compare(idx, o.idx);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof FileDescriptor))
            return false;

        FileDescriptor that = (FileDescriptor)o;

        return idx == that.idx;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return (int)(idx ^ (idx >>> 32));
    }

    /**
     * @return Absolute WAL segment file index
     */
    public long getIdx() {
        return idx;
    }

    /**
     * @return absolute pathname string of this file descriptor pathname.
     */
    public String getAbsolutePath() {
        return file.getAbsolutePath();
    }

    /** {@inheritDoc} */
    @Override public boolean isCompressed() {
        return file.getName().endsWith(FilePageStoreManager.ZIP_SUFFIX);
    }

    /** {@inheritDoc} */
    @Override public File file() {
        return file;
    }

    /** {@inheritDoc} */
    @Override public long idx() {
        return idx;
    }
}
