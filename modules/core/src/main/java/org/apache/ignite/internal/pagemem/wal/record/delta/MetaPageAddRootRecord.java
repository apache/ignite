/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * New root in meta page.
 */
public class MetaPageAddRootRecord extends PageDeltaRecord {
    /** */
    private long rootId;

    /**
     * @param grpId Cache ID.
     * @param pageId  Page ID.
     * @param rootId Root ID.
     */
    public MetaPageAddRootRecord(int grpId, long pageId, long rootId) {
        super(grpId, pageId);

        this.rootId = rootId;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        BPlusMetaIO io = BPlusMetaIO.VERSIONS.forPage(pageAddr);

        io.addRoot(pageAddr, rootId, pageMem.realPageSize(groupId()));
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.BTREE_META_PAGE_ADD_ROOT;
    }

    /**
     *
     */
    public long rootId() {
        return rootId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MetaPageAddRootRecord.class, this, "super", super.toString());
    }
}
