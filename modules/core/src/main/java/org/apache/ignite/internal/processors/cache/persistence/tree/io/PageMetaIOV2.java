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

package org.apache.ignite.internal.processors.cache.persistence.tree.io;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.util.GridStringBuilder;

/**
 *
 */
public class PageMetaIOV2 extends PageMetaIO {
    /** */
    private static final int INDEX_REBUILD_MARKERS_TREE_ROOT_OFF = END_OF_PAGE_META;

    /**
     * @param ver Page format version.
     */
    public PageMetaIOV2(int ver) {
        super(ver);
    }

    /** {@inheritDoc} */
    @Override public void initNewPage(long pageAddr, long pageId, int pageSize) {
        super.initNewPage(pageAddr, pageId, pageSize);

        setIndexRebuildMarkersTreeRoot(pageAddr, 0);
    }

    /** {@inheritDoc} */
    @Override public long getIndexRebuildMarkersTreeRoot(long pageAddr) {
        return PageUtils.getLong(pageAddr, INDEX_REBUILD_MARKERS_TREE_ROOT_OFF);
    }

    /** {@inheritDoc} */
    @Override public void setIndexRebuildMarkersTreeRoot(long pageAddr, long treeRoot) {
        PageUtils.putLong(pageAddr, INDEX_REBUILD_MARKERS_TREE_ROOT_OFF, treeRoot);
    }

    /** {@inheritDoc} */
    @Override protected void printPage(long addr, int pageSize, GridStringBuilder sb) throws IgniteCheckedException {
        sb.a("PageMeta[\n\ttreeRoot=").a(getReuseListRoot(addr))
            .a(",\n\tlastAllocatedPageCount=").a(getLastAllocatedPageCount(addr))
            .a(",\n\tcandidatePageCount=").a(getCandidatePageCount(addr))
            .a(",\n\trebuildStateTreeRoot=").a(getIndexRebuildMarkersTreeRoot(addr))
            .a("\n]");
    }

    /**
     * Upgrade page to PageMetaIOV2
     *
     * @param pageAddr Page address.
     */
    public void upgradePage(long pageAddr) {
        assert PageMetaIO.getType(pageAddr) == getType();
        assert PageMetaIO.getVersion(pageAddr) < 2;

        PageMetaIO.setVersion(pageAddr, getVersion());

        setIndexRebuildMarkersTreeRoot(pageAddr, 0);
    }
}
