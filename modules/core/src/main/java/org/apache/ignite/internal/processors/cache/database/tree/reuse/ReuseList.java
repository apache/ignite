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

package org.apache.ignite.internal.processors.cache.database.tree.reuse;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Reuse list for index pages.
 */
public final class ReuseList {
    /** */
    private final ReuseTree[] trees;

    /**
     * @param cacheId Cache ID.
     * @param pageMem Page memory.
     * @param wal Write ahead log manager.
     * @param rooIds Root IDs.
     * @param initNew Init new flag.
     * @throws IgniteCheckedException If failed.
     */
    public ReuseList(int cacheId, PageMemory pageMem, IgniteWriteAheadLogManager wal, long[] rooIds,
        boolean initNew) throws IgniteCheckedException {
        A.ensure(rooIds.length > 1, "Segments must be greater than 1.");

        trees = new ReuseTree[rooIds.length];

        for (int i = 0; i < rooIds.length; i++) {
            String idxName = BPlusTree.treeName("s" + i, "Reuse");

            trees[i] = new ReuseTree(idxName, this, cacheId, pageMem, wal, rooIds[i], initNew);
        }
    }

    /**
     * @return Size.
     * @throws IgniteCheckedException If failed.
     */
    public long size() throws IgniteCheckedException {
        long size = 0;

        for (ReuseTree tree : trees)
            size += tree.size();

        return size;
    }

    /**
     * @param client Client.
     * @return Reuse tree.
     */
    private ReuseTree tree(BPlusTree<?, ?> client) {
        int treeIdx = BPlusTree.randomInt(trees.length);

        ReuseTree tree = trees[treeIdx];

        assert tree != null;

        // Avoid recursion on the same tree to avoid dead lock.
        if (tree == client) {
            treeIdx++; // Go forward and take the next tree.

            if (treeIdx == trees.length)
                treeIdx = 0;

            tree = trees[treeIdx];
        }

        return tree;
    }

    /**
     * @param client Client tree.
     * @param bag Reuse bag.
     * @return Page ID or {@code 0} if none available.
     * @throws IgniteCheckedException If failed.
     */
    public long take(BPlusTree<?, ?> client, ReuseBag bag) throws IgniteCheckedException {
        // Remove and return page at min possible position.
        Long pageId = tree(client).removeCeil(0L, bag);

        return pageId != null ? pageId : 0;
    }

    /**
     * @param bag Reuse bag.
     * @throws IgniteCheckedException If failed.
     */
    public void add(ReuseBag bag) throws IgniteCheckedException {
        assert bag != null;

        for (int i = BPlusTree.randomInt(trees.length);;) {
            long pageId = bag.pollFreePage();

            if (pageId == 0)
                break;

            trees[i].put(pageId, bag);

            if (++i == trees.length)
                i = 0;
        }
    }

    /**
     * @return Pages contained in this Reuse List.
     * @throws IgniteCheckedException If failed.
     */
    public Collection<Long> pages() throws IgniteCheckedException {
        Collection<Long> result = new ArrayList<>();

        for (ReuseTree tree : trees) {
            GridCursor<Long> cursor = tree.find(null, null);

            while (cursor.next())
                result.add(cursor.get());
        }

        return result;
    }

    /**
     * Destroys this Reuse List.
     * @throws IgniteCheckedException If failed.
     */
    public void destroy() throws IgniteCheckedException {
        for (int i = 0; i < trees.length; i++)
            trees[i].destroy();
    }
}
