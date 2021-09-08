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

package org.apache.ignite.internal.processors.cache.persistence.defragmentation;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.GridUnsafe;

/** */
public class TreeIterator {
    /** Direct memory buffer with a size of one page. */
    private final ByteBuffer pageBuf;

    /** Offheap page size. */
    private final int pageSize;

    /** */
    public TreeIterator(int size) {
        pageSize = size;

        pageBuf = ByteBuffer.allocateDirect(pageSize);
    }

    /** */
    public <L, T extends L> void iterate(
        BPlusTree<L, T> tree,
        PageMemoryEx pageMemory,
        BPlusTree.TreeRowClosure<L, T> c
    ) throws IgniteCheckedException {
        int grpId = tree.groupId();

        long leafId = findFirstLeafId(grpId, tree.getMetaPageId(), pageMemory);

        long bufAddr = GridUnsafe.bufferAddress(pageBuf);

        while (leafId != 0L) {
            long leafPage = pageMemory.acquirePage(grpId, leafId);

            BPlusIO<L> io;

            try {
                long leafPageAddr = pageMemory.readLock(grpId, leafId, leafPage);

                try {
                    io = PageIO.getBPlusIO(leafPageAddr);

                    assert io instanceof BPlusLeafIO : io;

                    GridUnsafe.copyMemory(leafPageAddr, bufAddr, pageSize);
                }
                finally {
                    pageMemory.readUnlock(grpId, leafId, leafPage);
                }
            }
            finally {
                pageMemory.releasePage(grpId, leafId, leafPage);
            }

            int cnt = io.getCount(bufAddr);

            for (int idx = 0; idx < cnt; idx++)
                c.apply(tree, io, bufAddr, idx);

            leafId = io.getForward(bufAddr);
        }
    }

    /** */
    private long findFirstLeafId(int grpId, long metaPageId, PageMemoryEx partPageMemory) throws IgniteCheckedException {
        long metaPage = partPageMemory.acquirePage(grpId, metaPageId);

        try {
            long metaPageAddr = partPageMemory.readLock(grpId, metaPageId, metaPage);

            try {
                BPlusMetaIO metaIO = PageIO.getPageIO(metaPageAddr);

                return metaIO.getFirstPageId(metaPageAddr, 0);
            }
            finally {
                partPageMemory.readUnlock(grpId, metaPageId, metaPage);
            }
        }
        finally {
            partPageMemory.releasePage(grpId, metaPageId, metaPage);
        }
    }
}
