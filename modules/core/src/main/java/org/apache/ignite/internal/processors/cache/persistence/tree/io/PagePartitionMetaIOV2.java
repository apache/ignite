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

package org.apache.ignite.internal.processors.cache.persistence.tree.io;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.util.GridStringBuilder;

/**
 * IO for partition metadata pages.
 * Persistent partition contains it's own PendingTree.
 */
public class PagePartitionMetaIOV2 extends PagePartitionMetaIO {
    /** */
    private static final int PENDING_TREE_ROOT_OFF = PagePartitionMetaIO.END_OF_PARTITION_PAGE_META;

    /**
     * @param ver Version.
     */
    public PagePartitionMetaIOV2(int ver) {
        super(ver);
    }

    /** {@inheritDoc} */
    @Override public void initNewPage(long pageAddr, long pageId, int pageSize) {
        super.initNewPage(pageAddr, pageId, pageSize);

        setPendingTreeRoot(pageAddr, 0L);
    }

    /** {@inheritDoc} */
    @Override public long getPendingTreeRoot(long pageAddr) {
        return PageUtils.getLong(pageAddr, PENDING_TREE_ROOT_OFF);
    }

    /** {@inheritDoc} */
    @Override public void setPendingTreeRoot(long pageAddr, long treeRoot) {
        PageUtils.putLong(pageAddr, PENDING_TREE_ROOT_OFF, treeRoot);
    }

    /** {@inheritDoc} */
    @Override protected void printPage(long pageAddr, int pageSize, GridStringBuilder sb) throws IgniteCheckedException {
        byte state = getPartitionState(pageAddr);

        sb.a("PagePartitionMeta[\n\ttreeRoot=").a(getReuseListRoot(pageAddr));
        sb.a(",\n\tpendingTreeRoot=").a(getLastSuccessfulFullSnapshotId(pageAddr));
        sb.a(",\n\tlastSuccessfulFullSnapshotId=").a(getLastSuccessfulFullSnapshotId(pageAddr));
        sb.a(",\n\tlastSuccessfulSnapshotId=").a(getLastSuccessfulSnapshotId(pageAddr));
        sb.a(",\n\tnextSnapshotTag=").a(getNextSnapshotTag(pageAddr));
        sb.a(",\n\tlastSuccessfulSnapshotTag=").a(getLastSuccessfulSnapshotTag(pageAddr));
        sb.a(",\n\tlastAllocatedPageCount=").a(getLastAllocatedPageCount(pageAddr));
        sb.a(",\n\tcandidatePageCount=").a(getCandidatePageCount(pageAddr));
        sb.a(",\n\tsize=").a(getSize(pageAddr));
        sb.a(",\n\tupdateCounter=").a(getUpdateCounter(pageAddr));
        sb.a(",\n\tglobalRemoveId=").a(getGlobalRemoveId(pageAddr));
        sb.a(",\n\tpartitionState=").a(state).a("(").a(GridDhtPartitionState.fromOrdinal(state)).a(")");
        sb.a(",\n\tcountersPageId=").a(getCountersPageId(pageAddr));
        sb.a("\n]");
    }

    /**
     * Upgrade page to PagePartitionMetaIOV2
     *
     * @param pageAddr Page address.
     */
    public void upgradePage(long pageAddr) {
        assert PageIO.getType(pageAddr) == getType();
        assert PageIO.getVersion(pageAddr) < 2;

        PageIO.setVersion(pageAddr, getVersion());
        setPendingTreeRoot(pageAddr, 0);
    }
}
