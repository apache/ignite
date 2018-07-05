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

import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 * File WAL pointer.
 */
public class FileWALPointer implements WALPointer, Comparable<FileWALPointer> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Pointer serialized size. */
    public static final int POINTER_SIZE = 16;

    /** Absolute WAL segment file index (incrementing counter) */
    private final long idx;

    /** */
    private final int fileOff;

    /** Written record length */
    private int len;

    /**
     * @param idx Absolute WAL segment file index (incremental counter).
     * @param fileOff Offset in file, from the beginning.
     * @param len Record length.
     */
    public FileWALPointer(long idx, int fileOff, int len) {
        this.idx = idx;
        this.fileOff = fileOff;
        this.len = len;
    }

    /**
     * @return Absolute WAL segment file index .
     */
    public long index() {
        return idx;
    }

    /**
     * @return File offset.
     */
    public int fileOffset() {
        return fileOff;
    }

    /**
     * @return Record length.
     */
    public int length() {
        return len;
    }

    /**
     * @param len Record length.
     */
    public void length(int len) {
        this.len = len;
    }

    /** {@inheritDoc} */
    @Override public WALPointer next() {
        if (len == 0)
            throw new IllegalStateException("Failed to calculate next WAL pointer " +
                "(this pointer is a terminal): " + this);

        // Return a terminal pointer.
        return new FileWALPointer(idx, fileOff + len, 0);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof FileWALPointer))
            return false;

        FileWALPointer that = (FileWALPointer)o;

        return idx == that.idx && fileOff == that.fileOff;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = (int)(idx ^ (idx >>> 32));

        res = 31 * res + fileOff;

        return res;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(@NotNull FileWALPointer o) {
        int res = Long.compare(idx, o.idx);

        return res == 0 ? Integer.compare(fileOff, o.fileOff) : res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(FileWALPointer.class, this);
    }
}
