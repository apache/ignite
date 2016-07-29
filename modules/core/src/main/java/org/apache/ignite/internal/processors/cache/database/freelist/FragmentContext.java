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

import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.database.CacheDataRow;
import org.apache.ignite.internal.processors.cache.database.CacheEntryFragmentContext;

import java.nio.ByteBuffer;

/**
 *
 */
public class FragmentContext implements CacheEntryFragmentContext {
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

    /** {@inheritDoc} */
    @Override public CacheDataRow dataRow() {
        return row;
    }

    /** {@inheritDoc} */
    @Override public void dataRow(final CacheDataRow row) {
        this.row = row;
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer rowBuffer() {
        return rowBuf;
    }

    /** {@inheritDoc} */
    @Override public void rowBuffer(final ByteBuffer rowBuf) {
        this.rowBuf = rowBuf;
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer pageBuffer() {
        return pageBuf;
    }

    /** {@inheritDoc} */
    @Override public void pageBuffer(final ByteBuffer buf) {
        this.pageBuf = buf;
    }

    /** {@inheritDoc} */
    @Override public CacheObjectContext cacheObjectContext() {
        return coctx;
    }

    /** {@inheritDoc} */
    @Override public void cacheObjectContext(final CacheObjectContext coctx) {
        this.coctx = coctx;
    }

    /** {@inheritDoc} */
    @Override public int written() {
        return written;
    }

    /** {@inheritDoc} */
    @Override public void written(final int written) {
        this.written = written;
    }

    /** {@inheritDoc} */
    @Override public int totalEntrySize() {
        return totalEntrySize;
    }

    /** {@inheritDoc} */
    @Override public void totalEntrySize(final int totalEntrySize) {
        this.totalEntrySize = totalEntrySize;
    }

    /** {@inheritDoc} */
    @Override public int chunkSize() {
        return chunkSize;
    }

    /** {@inheritDoc} */
    @Override public void chunkSize(final int chunkSize) {
        this.chunkSize = chunkSize;
    }

    /** {@inheritDoc} */
    @Override public int chunks() {
        return chunks;
    }

    /** {@inheritDoc} */
    @Override public void chunks(final int chunks) {
        this.chunks = chunks;
    }

    /** {@inheritDoc} */
    @Override public long lastLink() {
        return lastLink;
    }

    /** {@inheritDoc} */
    @Override public void lastLink(final long lastLink) {
        this.lastLink = lastLink;
    }

    /** {@inheritDoc} */
    @Override public int lastIndex() {
        return lastIdx;
    }

    /** {@inheritDoc} */
    @Override public void lastIndex(final int idx) {
        this.lastIdx = idx;
    }

    /** {@inheritDoc} */
    @Override public int pageDataOffset() {
        return pageDataOff;
    }

    /** {@inheritDoc} */
    @Override public void pageDataOffset(final int dataOff) {
        this.pageDataOff = dataOff;
    }

    /** {@inheritDoc} */
    @Override public boolean lastFragment() {
        return lastFragment;
    }

    /** {@inheritDoc} */
    @Override public void lastFragment(final boolean last) {
        this.lastFragment = last;
    }
}
