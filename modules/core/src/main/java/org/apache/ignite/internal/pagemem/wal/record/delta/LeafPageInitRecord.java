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
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;

/**
 * Initialize new empty leaf page.
 */
public class LeafPageInitRecord extends PageDeltaRecord {
    /** */
    private int ioType;

    /** */
    private int ioVer;

    /** */
    private long newPageId;

    /**
     * @param cacheId Cache ID.
     * @param pageId  Page ID.
     * @param ioType IO Type.
     * @param ioVer IO Version.
     * @param newPageId New leaf page ID.
     */
    public LeafPageInitRecord(int cacheId, long pageId, int ioType, int ioVer, long newPageId) {
        super(cacheId, pageId);

        this.ioType = ioType;
        this.ioVer = ioVer;
        this.newPageId = newPageId;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(GridCacheContext<?,?> cctx, ByteBuffer buf) throws IgniteCheckedException {
        BPlusLeafIO<?> io = PageIO.getBPlusIO(ioType, ioVer);

        io.initNewPage(buf, newPageId);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.BTREE_PAGE_INIT_LEAF;
    }

    public int ioType() {
        return ioType;
    }

    public int ioVersion() {
        return ioVer;
    }

    public long newPageId() {
        return newPageId;
    }
}
