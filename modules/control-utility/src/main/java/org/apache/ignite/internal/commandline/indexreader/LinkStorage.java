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
package org.apache.ignite.internal.commandline.indexreader;

import org.apache.ignite.internal.util.collection.BitSetIntSet;
import org.apache.ignite.internal.util.collection.IntSet;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.ignite.internal.pagemem.PageIdUtils.*;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageIndex;

/**
 * Stores links to data pages.
 */
class LinkStorage implements ItemStorage<CacheAwareLink> {
    /** */
    private final Map<Integer, Map<Integer, Map<Byte, IntSet>>> store = new HashMap<>();

    /** */
    private long size;

    /** {@inheritDoc} */
    @Override public void add(CacheAwareLink cacheAwareLink) {
        long link = cacheAwareLink.link;

        long pageId = pageId(link);

        store.computeIfAbsent(cacheAwareLink.cacheId, k -> new HashMap<>())
                .computeIfAbsent(partId(pageId), k -> new HashMap<>())
                .computeIfAbsent((byte)itemId(link), k -> new BitSetIntSet())
                .add(pageIndex(pageId));

        size++;
    }

    /** {@inheritDoc} */
    @Override public boolean contains(CacheAwareLink cacheAwareLink) {
        long link = cacheAwareLink.link;

        long pageId = pageId(link);

        Map<Integer, Map<Byte, IntSet>> map = store.get(cacheAwareLink.cacheId);

        if (map != null) {
            Map<Byte, IntSet> innerMap = map.get(partId(pageId));

            if (innerMap != null) {
                IntSet set = innerMap.get((byte)itemId(link));

                if (set != null)
                    return set.contains(pageIndex(pageId));
            }
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public long size() {
        return size;
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Iterator<CacheAwareLink> iterator() {
        throw new UnsupportedOperationException("Item iteration is not supported by link storage.");
    }
}