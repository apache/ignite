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

package org.apache.ignite.internal.processors.query.h2.opt;

import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.ignite.internal.processors.query.h2.opt.GridLuceneOutputStream.BUFFER_SIZE;

/**
 * Lucene file.
 */
public class GridLuceneFile implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final AtomicInteger filesCnt = new AtomicInteger();

    /** */
    private LongArray buffers = new LongArray();

    /** */
    private long length;

    /** */
    private final GridLuceneDirectory dir;

    /** */
    private volatile long sizeInBytes;

    /**
     * File used as buffer, in no RAMDirectory
     *
     * @param dir Directory.
     */
    GridLuceneFile(GridLuceneDirectory dir) {
        this.dir = dir;

        filesCnt.incrementAndGet();
    }

    /**
     * For non-stream access from thread that might be concurrent with writing
     *
     * @return Length.
     */
    public synchronized long getLength() {
        return length;
    }

    /**
     * Sets length.
     *
     * @param length Length.
     */
    protected synchronized void setLength(long length) {
        this.length = length;
    }

    /**
     * @return New buffer address.
     */
    final long addBuffer() {
        long buf = newBuffer();

        synchronized (this) {
            buffers.add(buf);

            sizeInBytes += BUFFER_SIZE;
        }

        if (dir != null)
            dir.sizeInBytes.getAndAdd(BUFFER_SIZE);

        return buf;
    }

    /**
     * Gets address of buffer.
     *
     * @param idx Index.
     * @return Pointer.
     */
    protected final synchronized long getBuffer(int idx) {
        return buffers.get(idx);
    }

    /**
     * @return Number of buffers.
     */
    protected final synchronized int numBuffers() {
        return buffers.size();
    }

    /**
     * Expert: allocate a new buffer.
     * Subclasses can allocate differently.
     *
     * @return allocated buffer.
     */
    protected long newBuffer() {
        return dir.memory().allocate(BUFFER_SIZE);
    }

    /**
     * Deletes file and deallocates memory..
     */
    public synchronized void delete() {
        if (buffers == null)
            return;

        for (int i = 0; i < buffers.idx; i++)
            dir.memory().release(buffers.arr[i], BUFFER_SIZE);

        buffers = null;

        filesCnt.decrementAndGet();
    }

    /**
     * @return Size in bytes.
     */
    public long getSizeInBytes() {
        return sizeInBytes;
    }

    /**
     * @return Directory.
     */
    public GridLuceneDirectory getDirectory() {
        return dir;
    }

    /**
     * Simple expandable long[] wrapper.
     */
    private static class LongArray {
        /** */
        private long[] arr = new long[128];

        /** */
        private int idx;

        /**
         * @return Size.
         */
        int size() {
            return idx;
        }

        /**
         * Gets value by index.
         *
         * @param idx Index.
         * @return Value.
         */
        long get(int idx) {
            assert idx < this.idx;

            return arr[idx];
        }

        /**
         * Adds value.
         *
         * @param val Value.
         */
        void add(long val) {
            int len = arr.length;

            if (idx == len)
                arr = Arrays.copyOf(arr, Math.min(len * 2, len + 1024));

            arr[idx++] = val;
        }
    }
}