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
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Initialize new meta page.
 */
public class MetaPageInitRootRecord extends PageDeltaRecord {
    /** */
    protected long rootId;

    /**
     * @param pageId Meta page ID.
     */
    public MetaPageInitRootRecord(int grpId, long pageId, long rootId) {
        super(grpId, pageId);

        assert pageId != rootId;

        this.rootId = rootId;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        BPlusMetaIO io = BPlusMetaIO.VERSIONS.forPage(pageAddr);

        io.initRoot(pageAddr, rootId, pageMem.realPageSize(groupId()));
        io.setInlineSize(pageAddr, 0);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.BTREE_META_PAGE_INIT_ROOT;
    }

    /**
     * @return Root ID.
     */
    public long rootId() {
        return rootId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MetaPageInitRootRecord.class, this, "super", super.toString());
    }
}
