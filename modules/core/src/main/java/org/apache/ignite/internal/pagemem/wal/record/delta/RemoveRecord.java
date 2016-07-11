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
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;

/**
 * Remove.
 */
public class RemoveRecord extends PageDeltaRecord {
    /** */
    private int idx;

    /** */
    private int cnt;

    /** */
    private long rmvId;

    /**
     * @param cacheId Cache ID.
     * @param pageId  Page ID.
     * @param idx Index.
     * @param cnt Count.
     * @param rmvId Remove ID.
     */
    public RemoveRecord(int cacheId, long pageId, int idx, int cnt, long rmvId) {
        super(cacheId, pageId);

        this.idx = idx;
        this.cnt = cnt;
        this.rmvId = rmvId;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(GridCacheContext<?,?> cctx, ByteBuffer buf) throws IgniteCheckedException {
        BPlusIO<?> io = PageIO.getBPlusIO(buf);

        if (io.getCount(buf) != cnt)
            throw new DeltaApplicationException("Count in wrong: " + cnt);

        io.remove(buf, idx, cnt, rmvId);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.BTREE_PAGE_REMOVE;
    }

    public int index() {
        return idx;
    }

    public int count() {
        return cnt;
    }

    public long removeId() {
        return rmvId;
    }
}
