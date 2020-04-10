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
package org.apache.ignite.development.utils.indexreader;

import org.apache.ignite.internal.util.BitSetIntSet;
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
    private final Map<Integer, Map<Integer, Map<Byte, BitSetIntSet>>> store = new HashMap<>();

    /** */
    private long size = 0;

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

        Map<Integer, Map<Byte, BitSetIntSet>> map = store.get(cacheAwareLink.cacheId);

        if (map != null) {
            Map<Byte, BitSetIntSet> innerMap = map.get(partId(pageId));

            if (innerMap != null) {
                BitSetIntSet set = innerMap.get((byte)itemId(link));

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