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

package org.apache.ignite.internal.processors.cache.database.freelist;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.database.CacheDataRow;

/**
 *
 */
public class FragmentContext {
    /** */
    private CacheDataRow row;

    /** */
    private ByteBuffer rowBuf;

    /** */
    private ByteBuffer pageBuf;

    /** */
    private CacheObjectContext coctx;

    /** */
    private int written;

    /** */
    private int totalEntrySize;

    /** */
    private int chunkSize;

    /** */
    private int chunks;

    /** */
    private long lastLink;

    /** */
    private int lastIdx;

    /** */
    private int pageDataOff;

    /** */
    private boolean lastFragment;

    /**
     *
     */
    public FragmentContext() {
    }

    /**
     * @param totalEntrySize Total entry size with overhead.
     * @param chunks Number of chunks.
     * @param chunkSize Chunk size.
     * @param row Cache data row.
     */
    public FragmentContext(
        final int totalEntrySize,
        final int chunks,
        final int chunkSize,
        final CacheDataRow row
    ) {
        this.totalEntrySize = totalEntrySize;
        this.chunks = chunks;
        this.chunkSize = chunkSize;
        this.row = row;
    }

    /**
     * @return Row.
     */
    public CacheDataRow dataRow() {
        return row;
    }

    /**
     * @param row Row.
     */
    public void dataRow(final CacheDataRow row) {
        this.row = row;
    }

    /**
     * @return Row buffer.
     */
    public ByteBuffer rowBuffer() {
        return rowBuf;
    }

    /**
     * @param rowBuf Row bufer.
     */
    public void rowBuffer(final ByteBuffer rowBuf) {
        this.rowBuf = rowBuf;
    }

    /**
     * @return Page buffer.
     */
    public ByteBuffer pageBuffer() {
        return pageBuf;
    }

    /**
     * @param buf Page buffer.
     */
    public void pageBuffer(final ByteBuffer buf) {
        this.pageBuf = buf;
    }

    /**
     * @return Cache object context.
     */
    public CacheObjectContext cacheObjectContext() {
        return coctx;
    }

    /**
     * @param coctx Cache object context.
     */
    public void cacheObjectContext(final CacheObjectContext coctx) {
        this.coctx = coctx;
    }

    /**
     * @return Written bytes.
     */
    public int written() {
        return written;
    }

    /**
     * @param written Written bytes.
     */
    public void written(final int written) {
        this.written = written;
    }

    /**
     * @return Total entry size in bytes.
     */
    public int totalEntrySize() {
        return totalEntrySize;
    }

    /**
     * @param totalEntrySize Total entry size in bytes.
     */
    public void totalEntrySize(final int totalEntrySize) {
        this.totalEntrySize = totalEntrySize;
    }

    /**
     * @return Chunk size in bytes.
     */
    public int chunkSize() {
        return chunkSize;
    }

    /**
     * @param chunkSize Chunk size in bytes.
     */
    public void chunkSize(final int chunkSize) {
        this.chunkSize = chunkSize;
    }

    /**
     * @return Number of chunks.
     */
    public int chunks() {
        return chunks;
    }

    /**
     * @param chunks Number of chunks.
     */
    public void chunks(final int chunks) {
        this.chunks = chunks;
    }

    /**
     * @return Last link.
     */
    public long lastLink() {
        return lastLink;
    }

    /**
     * @param lastLink Last link.
     */
    public void lastLink(final long lastLink) {
        this.lastLink = lastLink;
    }

    /**
     * @return Last index.
     */
    public int lastIndex() {
        return lastIdx;
    }

    /**
     * @param idx Last index.
     */
    public void lastIndex(final int idx) {
        this.lastIdx = idx;
    }

    /**
     * @return Page data offset.
     */
    public int pageDataOffset() {
        return pageDataOff;
    }

    /**
     * @param dataOff Page data offset.
     */
    public void pageDataOffset(final int dataOff) {
        this.pageDataOff = dataOff;
    }

    /**
     * @return Last fragment flag.
     */
    public boolean lastFragment() {
        return lastFragment;
    }

    /**
     * @param last Last fragment flag.
     */
    public void lastFragment(final boolean last) {
        this.lastFragment = last;
    }
}
