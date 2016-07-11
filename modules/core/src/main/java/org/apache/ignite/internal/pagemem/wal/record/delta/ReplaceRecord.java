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
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;

/**
 * Replace.
 */
public class ReplaceRecord<L> extends PageDeltaRecord {
    /** */
    private BPlusIO<L> io;

    /** */
    private L row;

    /** */
    private byte[] rowBytes;

    /** */
    private int idx;

    /**
     * @param cacheId Cache ID.
     * @param pageId  Page ID.
     * @param io IO.
     * @param row Row.
     * @param rowBytes Row bytes.
     * @param idx Index.
     */
    public ReplaceRecord(int cacheId, long pageId, BPlusIO<L> io, L row, byte[] rowBytes, int idx) {
        super(cacheId, pageId);

        this.io = io;
        this.row = row;
        this.rowBytes = rowBytes;
        this.idx = idx;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(GridCacheContext<?,?> cctx, ByteBuffer buf)
        throws IgniteCheckedException {
        if (io.getCount(buf) >= idx)
            throw new DeltaApplicationException("Index is greater than count: " + idx);

        io.store(buf, idx, row, rowBytes);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.BTREE_PAGE_REPLACE;
    }

    public BPlusIO<L> io() {
        return io;
    }

    public int index() {
        return idx;
    }

    public L row() {
        return row;
    }
}
