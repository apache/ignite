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
import org.apache.ignite.internal.processors.cache.database.freelist.io.PagesListNodeIO;

/**
 *
 */
public class PagesListRemovePageRecord extends PageDeltaRecord {
    /** */
    private final long dataPageId;

    /**
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param dataPageId Data page ID to remove.
     */
    public PagesListRemovePageRecord(int cacheId, long pageId, long dataPageId) {
        super(cacheId, pageId);

        this.dataPageId = dataPageId;
    }

    /**
     * @return Data page ID.
     */
    public long dataPageId() {
        return dataPageId;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(GridCacheContext<?, ?> cctx, ByteBuffer buf) throws IgniteCheckedException {
        PagesListNodeIO io = PagesListNodeIO.VERSIONS.forPage(buf);

        boolean rmvd = io.removePage(buf, dataPageId);

        assert rmvd;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.PAGES_LIST_REMOVE_PAGE;
    }
}
