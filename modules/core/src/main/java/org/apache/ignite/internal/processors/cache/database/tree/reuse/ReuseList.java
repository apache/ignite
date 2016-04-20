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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.database.MetaStore;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.internal.processors.cache.database.tree.BPlusTree.randomInt;

/**
 * Reuse list for index pages.
 */
public class ReuseList {
    /** */
    private final ReuseTree[] trees;

    /** */
    private boolean ready;

    /**
     * @param cacheId Cache ID.
     * @param pageMem Page memory.
     * @param segments Segments.
     * @param metaStore Meta store.
     * @throws IgniteCheckedException If failed.
     */
    public ReuseList(int cacheId, PageMemory pageMem, int segments, MetaStore metaStore) throws IgniteCheckedException {
        A.ensure(segments > 1, "Segments must be greater than 1.");

        trees = new ReuseTree[segments];

        for (int i = 0; i < trees.length; i++) {
            String idxName = i + "##" + cacheId + "_reuse";

            IgniteBiTuple<FullPageId,Boolean> t = metaStore.getOrAllocateForIndex(cacheId, idxName);

            trees[i] = new ReuseTree(this, cacheId, pageMem, t.get1(), t.get2());
        }

        ready = true;
    }

    /**
     * @param client Client.
     * @return Reuse tree.
     */
    private ReuseTree tree(BPlusTree<?,?> client) {
        assert trees.length > 1;

        int treeIdx = randomInt(trees.length);

        ReuseTree tree = trees[treeIdx];

        assert tree != null;

        // Avoid dead locks.
        if (tree == client) {
            treeIdx++; // Go forward.

            if (treeIdx == trees.length)
                treeIdx = 0;

            tree = trees[treeIdx];
        }

        return tree;
    }

    /**
     * @param client Client tree.
     * @return Page ID.
     * @throws IgniteCheckedException If failed.
     */
    public FullPageId take(BPlusTree<?,?> client) throws IgniteCheckedException {
        return ready ? tree(client).removeFirst() : null;
    }

    /**
     * @param fullPageId Page ID.
     * @throws IgniteCheckedException If failed.
     */
    public void put(BPlusTree<?,?> client, FullPageId fullPageId) throws IgniteCheckedException {
        assert fullPageId != null;
        assert ready;

        tree(client).put(fullPageId);
    }
}
