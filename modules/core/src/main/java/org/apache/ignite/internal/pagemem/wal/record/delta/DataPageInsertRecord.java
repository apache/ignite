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

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.database.CacheDataRow;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 * Insert into data page.
 */
public class DataPageInsertRecord extends PageDeltaRecord implements CacheDataRow {
    /** */
    private KeyCacheObject key;

    /** */
    private CacheObject val;

    /** */
    private GridCacheVersion ver;

    /** */
    private int rowSize;

    /**
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param key Key.
     * @param val value.
     * @param ver version.
     * @param rowSize Row size.
     */
    public DataPageInsertRecord(
        int cacheId,
        long pageId,
        KeyCacheObject key,
        CacheObject val,
        GridCacheVersion ver,
        int rowSize
    ) {
        super(cacheId, pageId);

        this.key = key;
        this.val = val;
        this.ver = ver;
        this.rowSize = rowSize;
    }

    /**
     * @return Key.
     */
    @Override public KeyCacheObject key() {
        return key;
    }

    /**
     * @return Value.
     */
    @Override public CacheObject value() {
        return val;
    }

    /**
     * @return Version.
     */
    @Override public GridCacheVersion version() {
        return ver;
    }

    /**
     * @return Row size in bytes.
     */
    public int rowSize() {
        return rowSize;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(GridCacheContext<?,?> cctx, ByteBuffer buf)
        throws IgniteCheckedException {
        DataPageIO io = DataPageIO.VERSIONS.forPage(buf);

        CacheObjectContext coctx = cctx.cacheObjectContext();

        io.addRow(coctx, buf, this, rowSize);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.DATA_PAGE_INSERT_RECORD;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public long link() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void link(final long link) {
        // No-op.
    }
}
