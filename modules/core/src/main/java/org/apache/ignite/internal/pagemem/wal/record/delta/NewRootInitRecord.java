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
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusInnerIO;

/**
 * Initialize new root page.
 */
public class NewRootInitRecord<L> extends PageDeltaRecord {
    /** */
    private long newRootId;

    /** */
    private BPlusInnerIO<L> io;

    /** */
    private long leftChildId;

    /** */
    private L row;

    /** */
    private byte[] rowBytes;

    /** */
    private long rightChildId;

    /**
     * @param cacheId Cache ID.
     * @param pageId  Page ID.
     * @param io IO.
     * @param leftChildId Left child ID.
     * @param rowBytes Row.
     * @param rightChildId Right child ID.
     */
    public NewRootInitRecord(
        int cacheId,
        long pageId,
        long newRootId,
        BPlusInnerIO<L> io,
        long leftChildId,
        L row,
        byte[] rowBytes,
        long rightChildId
    ) {
        super(cacheId, pageId);

        assert io != null;

        this.newRootId = newRootId;
        this.io = io;
        this.leftChildId = leftChildId;
        this.row = row;
        this.rowBytes = rowBytes;
        this.rightChildId = rightChildId;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(ByteBuffer buf) throws IgniteCheckedException {
        io.initNewRoot(buf, newRootId, leftChildId, row, rowBytes, rightChildId);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.BTREE_INIT_NEW_ROOT;
    }

    public BPlusInnerIO<L> io() {
        return io;
    }

    public long rootId() {
        return newRootId;
    }

    public long leftId() {
        return leftChildId;
    }

    public long rightId() {
        return rightChildId;
    }

    public L row() {
        return row;
    }
}
