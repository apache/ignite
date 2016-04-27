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

package org.apache.ignite.internal.pagemem;

import org.apache.ignite.internal.util.typedef.internal.SB;

/**
 * Compound object used to address a page in the global page space.
 * <p>
 * There are three types of pages in PageMemory system:
 * <ul>
 *     <li>DATA pages</li>
 *     <li>INDEX pages</li>
 *     <li>META pages</li>
 * </ul>
 * Generally, a full page ID consists of a cache ID and page ID. A page ID consists of
 * file ID (22 bits) and page index (30 bits).
 * Higher 12 bits of page ID are reserved for use in index pages to address entries inside data pages.
 * File ID consists of 8 reserved bits, page type (2 bits) and partition ID (14 bits).
 * Note that partition ID is not used in full page ID comparison for non-data pages.
 * <p>
 * The structure of a page ID is shown in the next diagram:
 * <pre>
 * +------------+--------+--+--------------+------------------------------+
 * |   12 bits  | 8 bits |2b|   14 bits    |            30 bits           |
 * +------------+--------+--+--------------+------------------------------+
 * |   OFFSET   | RESRVD |FL| PARTITION ID |          PAGE INDEX          |
 * +------------+--------+--+--------------+------------------------------+
 *              |       FILE ID            |
 *              +--------------------------+
 * </pre>
 */
public class FullPageId {
    /** */
    private final long pageId;

    /** */
    private final long effectivePageId;

    /** */
    private final int cacheId;

    /**
     * @param pageId Page ID.
     * @param cacheId Cache ID.
     */
    public FullPageId(long pageId, int cacheId) {
        this.pageId = PageIdUtils.pageId(pageId);
        this.cacheId = cacheId;

        effectivePageId = PageIdUtils.effectivePageId(pageId);
    }

    /**
     * @return Page ID.
     */
    public long pageId() {
        return pageId;
    }

    /**
     * @return Cache ID.
     */
    public int cacheId() {
        return cacheId;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof FullPageId))
            return false;

        FullPageId that = (FullPageId)o;

        return effectivePageId == that.effectivePageId && cacheId == that.cacheId;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = (int)(effectivePageId ^ (effectivePageId >>> 32));

        result = 31 * result + cacheId;

        return result;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return new SB("FullPageId [pageId=").appendHex(pageId)
            .a(", effectivePageId=").appendHex(effectivePageId)
            .a(", cacheId=").a(cacheId).a(']').toString();
    }
}
