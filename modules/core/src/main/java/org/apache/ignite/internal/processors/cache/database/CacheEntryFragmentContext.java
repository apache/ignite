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

package org.apache.ignite.internal.processors.cache.database;

import org.apache.ignite.internal.processors.cache.CacheObjectContext;

import java.nio.ByteBuffer;

/**
 * Keeps data required for writing fragmented cache entries.
 */
public interface CacheEntryFragmentContext {
    /**
     * @return Data row that keeps entry that should be written.
     */
    public CacheDataRow dataRow();

    /**
     * @param row Data row that keeps entry that should be written.
     */
    public void dataRow(CacheDataRow row);

    /**
     * @return Buffer with fragment data.
     */
    public ByteBuffer rowBuffer();

    /**
     * @param rowBuf Buffer with fragment data.
     */
    public void rowBuffer(ByteBuffer rowBuf);

    /**
     * @return Data page buffer.
     */
    public ByteBuffer pageBuffer();

    /**
     * @param buf Data page buffer.
     */
    public void pageBuffer(ByteBuffer buf);

    /**
     * @return Cache object context.
     */
    public CacheObjectContext cacheObjectContext();

    /**
     * @param coctx Cache object context.
     */
    public void cacheObjectContext(CacheObjectContext coctx);

    /**
     * @return Total written entry bytes from the entry end.
     */
    public int written();

    /**
     * @param written Total written entry bytes from the entry end.
     */
    public void written(int written);

    /**
     * @return Total entry size with overhead.
     */
    public int totalEntrySize();

    /**
     * @param totalEntrySize Total entry size with overhead.
     */
    public void totalEntrySize(int totalEntrySize);

    /**
     * @return Size of the each chunk with overhead, except last one,
     * which may be smaller.
     */
    public int chunkSize();

    /**
     * @param chunkSize Size of the each chunk with overhead, except last one,
     * which may be smaller.
     */
    public void chunkSize(int chunkSize);

    /**
     * @return Number of all chunks for the entry.
     */
    public int chunks();

    /**
     * @param chunks Number of all chunks for the entry.
     */
    public void chunks(int chunks);

    /**
     * @return Link to the last written fragment.
     */
    public long lastLink();

    /**
     * @param lastLink Link to the last written fragment.
     */
    public void lastLink(long lastLink);

    /**
     * @return Item index of the last written fragment.
     */
    public int lastIndex();

    /**
     * @param idx Item index of the last written fragment.
     */
    public void lastIndex(int idx);

    /**
     * @return Data offset that points to fragment actual data without overhead
     */
    public int pageDataOffset();

    /**
     * @param dataOff Data offset that points to fragment actual data without overhead
     */
    public void pageDataOffset(int dataOff);

    /**
     * @return {@code True} if last written fragment was the last one.
     */
    public boolean lastFragment();

    /**
     * @param last {@code True} if last written fragment was the last one.
     */
    public void lastFragment(boolean last);

}
