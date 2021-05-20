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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.pagemem.store.PageStoreCollection;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.MAX_PARTITION_ID;

/** */
public class PageReadWriteManagerImpl implements PageReadWriteManager {
    /** */
    private final GridKernalContext ctx;

    /** */
    @GridToStringExclude
    protected final PageStoreCollection pageStores;

    /** */
    @SuppressWarnings("unused")
    private final String name;

    /**
     * @param ctx Kernal context.
     * @param pageStores Page stores.
     */
    public PageReadWriteManagerImpl(
        GridKernalContext ctx,
        PageStoreCollection pageStores,
        String name
    ) {
        this.ctx = ctx;
        this.pageStores = pageStores;
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public void read(int grpId, long pageId, ByteBuffer pageBuf, boolean keepCrc) throws IgniteCheckedException {
        PageStore store = pageStores.getStore(grpId, PageIdUtils.partId(pageId));

        try {
            store.read(pageId, pageBuf, keepCrc);

            ctx.compress().decompressPage(pageBuf, store.getPageSize());
        }
        catch (StorageException e) {
            ctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public PageStore write(
        int grpId,
        long pageId,
        ByteBuffer pageBuf,
        int tag,
        boolean calculateCrc
    ) throws IgniteCheckedException {
        int partId = PageIdUtils.partId(pageId);

        PageStore store = pageStores.getStore(grpId, partId);

        try {
            int pageSize = store.getPageSize();
            int compressedPageSize = pageSize;

            GridCacheContext<?, ?> cctx0 = ctx.cache().context().cacheContext(grpId);

            if (cctx0 != null) {
                assert pageBuf.position() == 0 && pageBuf.limit() == pageSize : pageBuf;

                ByteBuffer compressedPageBuf = cctx0.compress().compressPage(pageBuf, store);

                if (compressedPageBuf != pageBuf) {
                    compressedPageSize = PageIO.getCompressedSize(compressedPageBuf);

                    if (!calculateCrc) {
                        calculateCrc = true;
                        PageIO.setCrc(compressedPageBuf, 0); // It will be recalculated over compressed data further.
                    }

                    PageIO.setCrc(pageBuf, 0); // It is expected to be reset to 0 after each write.
                    pageBuf = compressedPageBuf;
                }
            }

            store.write(pageId, pageBuf, tag, calculateCrc);

            if (pageSize > compressedPageSize)
                store.punchHole(pageId, compressedPageSize); // TODO maybe add async punch mode?
        }
        catch (StorageException e) {
            ctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw e;
        }

        return store;
    }

    /** {@inheritDoc} */
    @Override public long allocatePage(int grpId, int partId, byte flags) throws IgniteCheckedException {
        assert partId <= MAX_PARTITION_ID || partId == INDEX_PARTITION;

        PageStore store = pageStores.getStore(grpId, partId);

        try {
            long pageIdx = store.allocatePage();

            return PageIdUtils.pageId(partId, flags, (int)pageIdx);
        }
        catch (StorageException e) {
            ctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PageReadWriteManagerImpl.class, this);
    }
}
