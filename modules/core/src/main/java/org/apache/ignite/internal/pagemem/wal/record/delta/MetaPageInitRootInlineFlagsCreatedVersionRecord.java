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
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteProductVersion;

/**
 *
 */
public class MetaPageInitRootInlineFlagsCreatedVersionRecord extends MetaPageInitRootInlineRecord {
    /** Flags for the created meta page. */
    private final long flags;

    /** Version of the created meta page. */
    private final IgniteProductVersion createdVer;

    /**
     * @param grpId Cache group ID.
     * @param pageId Meta page ID.
     * @param rootId Root id.
     * @param inlineSize Inline size.
     */
    public MetaPageInitRootInlineFlagsCreatedVersionRecord(int grpId, long pageId, long rootId, int inlineSize) {
        super(grpId, pageId, rootId, inlineSize);

        createdVer = IgniteVersionUtils.VER;
        flags = BPlusMetaIO.DEFAULT_FLAGS;
    }

    /**
     * @param grpId Cache group ID.
     * @param pageId Meta page ID.
     * @param rootId Root id.
     * @param inlineSize Inline size.
     * @param flags Flags.
     * @param createdVer The version of ignite that creates this tree.
     */
    public MetaPageInitRootInlineFlagsCreatedVersionRecord(int grpId, long pageId, long rootId, int inlineSize,
        long flags, IgniteProductVersion createdVer) {
        super(grpId, pageId, rootId, inlineSize);

        this.flags = flags;
        this.createdVer = createdVer;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        super.applyDelta(pageMem, pageAddr);

        BPlusMetaIO io = BPlusMetaIO.VERSIONS.forPage(pageAddr);

        io.initFlagsAndVersion(pageAddr, flags, createdVer);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.BTREE_META_PAGE_INIT_ROOT_V3;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MetaPageInitRootInlineFlagsCreatedVersionRecord.class, this, "super", super.toString());
    }

    /**
     * @return Created version.
     */
    public IgniteProductVersion createdVersion() {
        return createdVer;
    }

    /**
     * @return Meta page flags.
     */
    public long flags() {
        return flags;
    }
}
