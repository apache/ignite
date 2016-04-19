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
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.internal.processors.cache.database.tree.BPlusTree.randomInt;

/**
 * Reuse list for index pages.
 */
public class ReuseList {
    /** */
    private final ReuseTree[] trees;

    /**
     * @param cacheId Cache ID.
     * @param pageMem Page memory.
     * @param metaStore Meta store.
     * @throws IgniteCheckedException If failed.
     */
    public ReuseList(int cacheId, PageMemory pageMem, int stripes, MetaStore metaStore) throws IgniteCheckedException {
        trees = new ReuseTree[stripes];

        for (int i = 0; i < trees.length; i++) {
            String idxName = i + "##" + cacheId + "_reuse";

            IgniteBiTuple<FullPageId,Boolean> t = metaStore.getOrAllocateForIndex(cacheId, idxName);

            trees[i] = new ReuseTree(cacheId, pageMem, t.get1(), t.get2());
        }
    }

    /**
     * @return Page ID.
     * @throws IgniteCheckedException If failed.
     */
    public FullPageId take() throws IgniteCheckedException {
        return trees.length == 0 ? null : trees[randomInt(trees.length)].removeFirst();
    }

    /**
     * @param fullPageId Page ID.
     * @throws IgniteCheckedException If failed.
     */
    public void put(FullPageId fullPageId) throws IgniteCheckedException {
        assert fullPageId != null;

        if (trees.length != 0)
            trees[randomInt(trees.length)].put(fullPageId);
    }
}
