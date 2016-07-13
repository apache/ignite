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
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusMetaIO;

/**
 * Initialize new meta page.
 */
public class MetaPageInitNewRecord extends PageDeltaRecord {
    /** */
    private long metaId;

    /** */
    private long rootId;

    /** */
    private int ioVer;

    /**
     * @param cacheId Cache ID.
     * @param pageId Meta page ID.
     * @param rootId Root page ID.
     */
    public MetaPageInitNewRecord(int cacheId, long pageId, long metaId, long rootId, int ioVer) {
        super(cacheId, pageId);

        this.metaId = metaId;
        this.rootId = rootId;
        this.ioVer = ioVer;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(GridCacheContext<?,?> cctx, ByteBuffer buf)
        throws IgniteCheckedException {
        BPlusMetaIO.VERSIONS.forVersion(ioVer).initNewPage(buf, metaId, rootId);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.BTREE_META_PAGE_INIT_NEW;
    }

    public long metaId() {
        return metaId;
    }

    public long rootId() {
        return rootId;
    }

    public int ioVersion() {
        return ioVer;
    }
}
