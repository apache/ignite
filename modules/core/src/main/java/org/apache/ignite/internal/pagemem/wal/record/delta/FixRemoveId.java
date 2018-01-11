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
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Fix remove ID record.
 */
public class FixRemoveId extends PageDeltaRecord {
    /** */
    private long rmvId;

    /**
     * @param grpId Cache group ID.
     * @param pageId  Page ID.
     * @param rmvId Remove ID.
     */
    public FixRemoveId(int grpId, long pageId, long rmvId) {
        super(grpId, pageId);

        this.rmvId = rmvId;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr)
        throws IgniteCheckedException {
        BPlusIO<?> io = PageIO.getBPlusIO(pageAddr);

        io.setRemoveId(pageAddr, rmvId);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.BTREE_FIX_REMOVE_ID;
    }

    /**
     * @return Remove ID.
     */
    public long removeId() {
        return rmvId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(FixRemoveId.class, this, "super", super.toString());
    }
}
