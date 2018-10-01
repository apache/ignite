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
 *
 */

package org.apache.ignite.internal.stat;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.typedef.T2;

/**
 * Enumeration fine grain IO types.
 */
public enum PageType {
    /** */
    T_DATA(PageIO.T_DATA),
    /** */
    T_BPLUS_META(PageIO.T_BPLUS_META),
    /** */
    T_H2_REF_LEAF(PageIO.T_H2_REF_LEAF),
    /** */
    T_H2_REF_INNER(PageIO.T_H2_REF_INNER),
    /** */
    T_DATA_REF_INNER(PageIO.T_DATA_REF_INNER),
    /** */
    T_DATA_REF_LEAF(PageIO.T_DATA_REF_LEAF),
    /** */
    T_METASTORE_INNER(PageIO.T_METASTORE_INNER),
    /** */
    T_METASTORE_LEAF(PageIO.T_METASTORE_LEAF),
    /** */
    T_PENDING_REF_INNER(PageIO.T_PENDING_REF_INNER),
    /** */
    T_PENDING_REF_LEAF(PageIO.T_PENDING_REF_LEAF),
    /** */
    T_META(PageIO.T_META),
    /** */
    T_PAGE_LIST_META(PageIO.T_PAGE_LIST_META),
    /** */
    T_PAGE_LIST_NODE(PageIO.T_PAGE_LIST_NODE),
    /** */
    T_PART_META(PageIO.T_PART_META),
    /** */
    T_PAGE_UPDATE_TRACKING(PageIO.T_PAGE_UPDATE_TRACKING),
    /** */
    T_CACHE_ID_AWARE_DATA_REF_INNER(PageIO.T_CACHE_ID_AWARE_DATA_REF_INNER),
    /** */
    T_CACHE_ID_AWARE_DATA_REF_LEAF(PageIO.T_CACHE_ID_AWARE_DATA_REF_LEAF),
    /** */
    T_CACHE_ID_AWARE_PENDING_REF_INNER(PageIO.T_CACHE_ID_AWARE_PENDING_REF_INNER),
    /** */
    T_CACHE_ID_AWARE_PENDING_REF_LEAF(PageIO.T_CACHE_ID_AWARE_PENDING_REF_LEAF),
    /** */
    T_PART_CNTRS(PageIO.T_PART_CNTRS),
    /** */
    T_DATA_METASTORAGE(PageIO.T_DATA_METASTORAGE),
    /** */
    T_DATA_REF_METASTORAGE_INNER(PageIO.T_DATA_REF_METASTORAGE_INNER),
    /** */
    T_DATA_REF_METASTORAGE_LEAF(PageIO.T_DATA_REF_METASTORAGE_LEAF),
    /** */
    T_DATA_REF_MVCC_INNER(PageIO.T_DATA_REF_MVCC_INNER),
    /** */
    T_DATA_REF_MVCC_LEAF(PageIO.T_DATA_REF_MVCC_LEAF),
    /** */
    T_CACHE_ID_DATA_REF_MVCC_INNER(PageIO.T_CACHE_ID_DATA_REF_MVCC_INNER),
    /** */
    T_CACHE_ID_DATA_REF_MVCC_LEAF(PageIO.T_CACHE_ID_DATA_REF_MVCC_LEAF),
    /** */
    T_H2_MVCC_REF_LEAF(PageIO.T_H2_MVCC_REF_LEAF),
    /** */
    T_H2_MVCC_REF_INNER(PageIO.T_H2_MVCC_REF_INNER),
    /** */
    T_TX_LOG_LEAF(PageIO.T_TX_LOG_LEAF),
    /** */
    T_TX_LOG_INNER(PageIO.T_TX_LOG_INNER),
    /** */
    T_H2_EX_REF_LEAF(PageIO.T_H2_EX_REF_LEAF_START, PageIO.T_H2_EX_REF_LEAF_END),
    /** */
    T_H2_EX_REF_INNER(PageIO.T_H2_EX_REF_INNER_START, PageIO.T_H2_EX_REF_INNER_END),
    /** */
    T_H2_EX_REF_MVCC_LEAF(PageIO.T_H2_EX_REF_MVCC_LEAF_START, PageIO.T_H2_EX_REF_MVCC_LEAF_END),
    /** */
    T_H2_EX_REF_MVCC_INNER(PageIO.T_H2_EX_REF_MVCC_INNER_START, PageIO.T_H2_EX_REF_MVCC_INNER_END);

    /** Related to {@code pageType} pageIo types. */
    private final T2<Integer, Integer> pageIoTypeRange;

    /**
     * @param pageIoTypeId Page IO type related to constructing of page type.
     */
    PageType(int pageIoTypeId) {
        pageIoTypeRange = new T2<>(pageIoTypeId, pageIoTypeId);
    }

    /**
     * @param pageIoTypeIdStart Left bound of range of page IO type.
     * @param pageIoTypeEnd Right bound of range of page IO type.
     */
    PageType(int pageIoTypeIdStart, int pageIoTypeEnd) {
        pageIoTypeRange = new T2<>(pageIoTypeIdStart, pageIoTypeEnd);
    }

    /**
     * Convert page io type to {@code PageType}.
     *
     * @param pageIoType Page io type.
     * @return Derived PageType.
     */
    static PageType derivePageType(int pageIoType) {
        for (PageType t : values()) {
            if (t.pageIoTypeRange.get1() <= pageIoType && pageIoType <= t.pageIoTypeRange.get2())
                return t;
        }

        throw new IgniteException("pageIoType " + pageIoType + "doesn't support");
    }

}
