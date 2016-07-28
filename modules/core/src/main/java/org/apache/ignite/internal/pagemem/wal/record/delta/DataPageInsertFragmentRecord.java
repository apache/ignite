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

package org.apache.ignite.internal.pagemem.wal.record.delta;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPageIO;

import java.nio.ByteBuffer;

/**
 * Insert fragment to data page record.
 */
public class DataPageInsertFragmentRecord extends PageDeltaRecord {
    /** Total written entry bytes from the entry end. */
    private final int written;

    /** Link to the last entry fragment. */
    private final long lastLink;

    /** Actual fragment data. */
    private final byte[] fragmentData;

    /**
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param written Total written entry bytes from the entry end.
     * @param lastLink Link to the last entry fragment.
     * @param fragmentData Actual fragment data.
     */
    public DataPageInsertFragmentRecord(
        final int cacheId,
        final long pageId,
        final int written,
        final long lastLink,
        final byte[] fragmentData
        ) {
        super(cacheId, pageId);

        this.written = written;
        this.lastLink = lastLink;
        this.fragmentData = fragmentData;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(final GridCacheContext<?, ?> cctx,
        final ByteBuffer buf) throws IgniteCheckedException {
        final DataPageIO io = DataPageIO.VERSIONS.forPage(buf);

        io.addRowFragment(null, ByteBuffer.wrap(fragmentData), buf,
            cctx.cacheObjectContext(), written, 0, 0, 0, lastLink);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.DATA_PAGE_INSERT_FRAGMENT_RECORD;
    }

    /**
     * @return Fragment data size.
     */
    public int fragmentDataSize() {
        return fragmentData.length;
    }

    /**
     * @return Actual fragment data.
     */
    public byte[] fragmentData() {
        return fragmentData;
    }

    /**
     * @return Total written entry bytes from the entry end.
     */
    public int written() {
        return written;
    }

    /**
     * @return Link to the last entry fragment.
     */
    public long lastLink() {
        return lastLink;
    }
}
