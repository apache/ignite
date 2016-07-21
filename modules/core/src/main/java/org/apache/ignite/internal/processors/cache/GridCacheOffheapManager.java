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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageMemory;

import java.nio.ByteBuffer;

/**
 * Used when persistence enabled.
 */
public class GridCacheOffheapManager extends IgniteCacheOffheapManagerImpl {
    /** {@inheritDoc} */
    @Override protected Metas getOrAllocateMetas(final PageMemory pageMem,
        final int cacheId) throws IgniteCheckedException {
        long metastoreRoot = 0;

        long[] rootIds = null;

        boolean initNew = false;

        try (Page meta = pageMem.metaPage(cacheId)) {
            boolean initialized = false;

            final ByteBuffer buf = meta.getForWrite();

            try {
                final int pagesNum = buf.getInt();

                if (pagesNum > 0) {
                    initialized = true;

                    assert pagesNum > 2 : "Must be at least 2 pages for reuse list";

                    metastoreRoot = buf.getLong();

                    rootIds = new long[pagesNum - 1];

                    for (int i = 0; i < rootIds.length; i++) {
                        assert buf.remaining() >= 8 : "Meta page is corrupted";

                        rootIds[i] = buf.getLong();
                    }

                }

                if (!initialized) {
                    metastoreRoot = pageMem.allocatePage(cacheId, 0, PageMemory.FLAG_IDX);

                    rootIds = allocateMetas(pageMem, cacheId, true);

                    buf.rewind();

                    buf.putInt(rootIds.length + 1);
                    buf.putLong(metastoreRoot);

                    for (final long rootId : rootIds)
                        buf.putLong(rootId);

                    initNew = true;
                }
            }
            finally {
                meta.releaseWrite(!initialized);
            }
        }

        return new Metas(rootIds, metastoreRoot, initNew);
    }
}
