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
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 * Insert into data page.
 */
public class DataPageInsertRecord extends PageDeltaRecord {
    /** */
    private CacheObject key;

    /** */
    private CacheObject val;

    /** */
    private GridCacheVersion ver;

    /** */
    private int itemId;

    /** */
    private int entrySize;

    /**
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param key Key.
     * @param val value.
     * @param ver version.
     * @param itemId Item ID.
     * @param entrySize Entry size.
     */
    public DataPageInsertRecord(
        int cacheId,
        long pageId,
        CacheObject key,
        CacheObject val,
        GridCacheVersion ver,
        int itemId,
        int entrySize
    ) {
        super(cacheId, pageId);

        this.key = key;
        this.val = val;
        this.ver = ver;
        this.itemId = itemId;
        this.entrySize = entrySize;
    }

    /**
     * @return Key.
     */
    public CacheObject key() {
        return key;
    }

    /**
     * @return Value.
     */
    public CacheObject value() {
        return val;
    }

    /**
     * @return Version.
     */
    public GridCacheVersion version() {
        return ver;
    }

    /**
     * @return Item ID.
     */
    public int itemId() {
        return itemId;
    }

    /**
     * @return Entry size.
     */
    public int entrySize() {
        return entrySize;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(GridCacheContext<?,?> cctx, ByteBuffer buf)
        throws IgniteCheckedException {
        DataPageIO io = DataPageIO.VERSIONS.forPage(buf);

        CacheObjectContext coctx = cctx.cacheObjectContext();

        int itemId = io.addRow(coctx, buf, key, val, ver, entrySize);

        if (itemId != this.itemId)
            throw new DeltaApplicationException("Unexpected itemId: " + itemId);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.DATA_PAGE_INSERT_RECORD;
    }
}
