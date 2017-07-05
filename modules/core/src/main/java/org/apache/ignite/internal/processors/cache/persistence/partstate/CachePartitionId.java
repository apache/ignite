/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.partstate;

import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.jetbrains.annotations.NotNull;

/**
 * Pair of cache ID with partition ID.
 */
public class CachePartitionId implements Comparable<CachePartitionId> {
    /** Cache id. */
    private final int cacheId;
    /** Partition id. */
    private final int partId;

    public CachePartitionId(int cacheId, int partId) {
        this.cacheId = cacheId;
        this.partId = partId;
    }

    public int getCacheId() {
        return cacheId;
    }

    public int getPartId() {
        return partId;
    }

    /** Tmp method for compatibility with tuple */
    public int get1() {
        return getCacheId();
    }

    /** Tmp method for compatibility with tuple */
    public int get2() {
        return getPartId();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Key{" +
            "cacheId=" + cacheId +
            ", partId=" + partId +
            '}';
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        CachePartitionId key = (CachePartitionId)o;

        if (cacheId != key.cacheId)
            return false;
        return partId == key.partId;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = cacheId;
        result = 31 * result + partId;
        return result;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(@NotNull CachePartitionId o) {
        if (getCacheId() < o.getCacheId())
            return -1;

        if (getCacheId() > o.getCacheId())
            return 1;

        if (getPartId() < o.getPartId())
            return -1;

        if (getPartId() > o.getPartId())
            return 1;
        return 0;
    }

    /**
     * @param pageIdx Page Index, monotonically growing number within each partition
     * @return page ID (64 bits) constructed from partition ID and given index
     */
    public long createPageId(final int pageIdx) {
        return PageIdUtils.pageId(getPartId(), (byte)0, pageIdx);
    }

    /**
     * Returns Full page ID
     *
     * @param pageIdx Page Index, monotonically growing number within each partition
     * @return FullPageId consists of cache ID (32 bits) and page ID (64 bits).
     */
    @NotNull public FullPageId createFullPageId(final int pageIdx) {
        return new FullPageId(createPageId(pageIdx), getCacheId());
    }
}
