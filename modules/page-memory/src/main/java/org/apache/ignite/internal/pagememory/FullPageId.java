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

package org.apache.ignite.internal.pagememory;

import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteStringBuilder;

/**
 * Compound object used to address a page in the global page space.
 * <h3>Page ID structure</h3>
 *
 * <p>Generally, a full page ID consists of a group ID and a page ID. A page ID consists of a page index (32 bits), a partition ID (16 bits)
 * and flags (8 bits). Group ID is an integer identifier of a logical pages group, like a SQL table or metadata storage, for example.
 * Page index is a unique page identifier inside of a specific partition of a page group. Set of indexes in the partition represents
 * a continuous range that starts with 0. Higher 8 bits of a page ID are reserved for addressing entries inside data pages or for
 * page ID rotation.
 *
 * <p>Partition ID {@code 0xFFFF} is reserved for index pages.
 *
 * <p>The structure of a page ID is shown in the diagram below:
 * <pre>
 * +---------+-----------+------------+--------------------------+
 * | 8 bits  |   8 bits  |  16 bits   |         32 bits          |
 * +---------+-----------+------------+--------------------------+
 * |  OFFSET |   FLAGS   |PARTITION ID|       PAGE INDEX         |
 * +---------+-----------+------------+--------------------------+
 * </pre>
 *
 * <h3>Page ID rotation</h3>
 * There are scenarios when we reference one page (B) from within another page (A) by page ID. It is also
 * possible that this first page (B) is concurrently reused for a different purpose. In this
 * case we should have a mechanism to determine that the reference from page (A) to page (B) is no longer valid.
 * This is ensured by page ID rotation - together with the ID of page B we write a value that is incremented
 * each time a page is reused. This value should be verified after every page read and the page
 * should be discarded if the full ID is different.
 *
 * <p>Effective page ID is a page ID with zeroed bits used for page ID rotation.
 */
public final class FullPageId {
    /** Null page ID. */
    public static final FullPageId NULL_PAGE = new FullPageId(-1, -1);

    /** Page ID. */
    private final long pageId;

    /** Group ID. */
    private final int groupId;

    /**
     * Constructor.
     *
     * @param pageId  Page ID.
     * @param groupId Group ID.
     */
    public FullPageId(long pageId, int groupId) {
        this.pageId = pageId;
        this.groupId = groupId;
    }

    /**
     * Returns a page ID.
     */
    public long pageId() {
        return pageId;
    }

    /**
     * Returns an effective page ID: pageId with only pageIdx and partitionId.
     */
    public long effectivePageId() {
        return PageIdUtils.effectivePageId(pageId);
    }

    /**
     * Returns a group ID.
     */
    public int groupId() {
        return groupId;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof FullPageId)) {
            return false;
        }

        FullPageId that = (FullPageId) o;

        return effectivePageId() == that.effectivePageId() && groupId == that.groupId;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return hashCode0(groupId, PageIdUtils.effectivePageId(pageId));
    }

    /**
     * Returns hash code of the ID.
     *
     * @param groupId Group ID.
     * @param pageId  Page ID.
     * @return Hash code.
     */
    public static int hashCode(int groupId, long pageId) {
        long effectiveId = PageIdUtils.effectivePageId(pageId);

        return IgniteUtils.hash(hashCode0(groupId, effectiveId));
    }

    /**
     * Will not clear link bits.
     *
     * @param groupId         Group ID.
     * @param effectivePageId Effective page ID.
     * @return Hash code.
     */
    private static int hashCode0(int groupId, long effectivePageId) {
        return (int) (mix64(effectivePageId) ^ mix32(groupId));
    }

    /**
     * MH3's plain finalization step.
     */
    private static int mix32(int k) {
        k = (k ^ (k >>> 16)) * 0x85ebca6b;
        k = (k ^ (k >>> 13)) * 0xc2b2ae35;

        return k ^ (k >>> 16);
    }

    /**
     * Computes David Stafford variant 9 of 64bit mix function (MH3 finalization step, with different shifts and constants).
     *
     * <p>Variant 9 is picked because it contains two 32-bit shifts which could be possibly optimized into better machine code.
     *
     * @see "http://zimbry.blogspot.com/2011/09/better-bit-mixing-improving-on.html"
     */
    private static long mix64(long z) {
        z = (z ^ (z >>> 32)) * 0x4cd6944c5cc20b6dL;
        z = (z ^ (z >>> 29)) * 0xfc12c5b19d3259e9L;

        return z ^ (z >>> 32);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return new IgniteStringBuilder("FullPageId [pageId=").appendHex(pageId)
                .app(", effectivePageId=").appendHex(effectivePageId())
                .app(", groupId=").app(groupId).app(']').toString();
    }
}
