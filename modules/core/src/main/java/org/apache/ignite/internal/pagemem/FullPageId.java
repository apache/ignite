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
 * <h3>Page ID structure</h3>
 * <p>
 * Generally, a full page ID consists of a cache ID and page ID. A page ID consists of
 * page index (32 bits), partition ID (16 bits) and flags.
 * Higher 8 bits of page ID are unused and reserved to address entries inside data pages or page ID rotation.
 * <p>
 * Partition ID {@code 0xFFFF} is reserved for index pages.
 * <p>
 * The structure of a page ID is shown in the diagram below:
 * <pre>
 * +---------+-----------+------------+--------------------------+
 * | 8 bits  |   8 bits  |  16 bits   |         32 bits          |
 * +---------+-----------+------------+--------------------------+
 * |  OFFSET |   FLAGS   |PARTITION ID|       PAGE INDEX         |
 * +---------+-----------+------------+--------------------------+
 * <p>
 * <h3>Page ID rotation</h3>
 * There are scenarios when we reference one page (B) from within another page (A) by page ID. It is also
 * possible that this first page (B) is de-allocated and allocated again for a different purpose. In this
 * case we should have a mechanism to determine that page (B) cannot be used after reading it's ID in page (A).
 * This is ensured by page ID rotation - together with page's (B) ID we should write some value that is incremented
 * each time a page is de-allocated (page ID rotation). This ID should be verified after page read and a page
 * should be discarded if full ID is different.
 * <p>
 * Effective page ID is page ID with zeroed bits used for page ID rotation.
 */
public class FullPageId {
    /** */
    private final long pageId;

    /** */
    private final long effectivePageId;

    /** */
    private final int cacheId;

    /**
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @return Hash code.
     */
    public static int hashCode(int cacheId, long pageId) {
        long effectiveId = PageIdUtils.effectivePageId(pageId);

        return hashCode0(cacheId, effectiveId);
    }

    /**
     * Will not clear link bits.
     *
     * @param cacheId Cache ID.
     * @param effectivePageId Effective page ID.
     * @return Hash code.
     */
    private static int hashCode0(int cacheId, long effectivePageId) {
        int res = (int)(effectivePageId ^ (effectivePageId >>> 32));

        res = 31 * res + cacheId;

        return res;
    }

    /**
     * @param pageId Page ID.
     * @param cacheId Cache ID.
     */
    public FullPageId(long pageId, int cacheId) {
        this.pageId = pageId;
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
        return hashCode0(cacheId, effectivePageId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return new SB("FullPageId [pageId=").appendHex(pageId)
            .a(", effectivePageId=").appendHex(effectivePageId)
            .a(", cacheId=").a(cacheId).a(']').toString();
    }
}
