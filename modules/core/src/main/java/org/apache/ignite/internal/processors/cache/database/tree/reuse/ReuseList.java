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

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.database.MetadataStorage;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Reuse list for index pages.
 */
public class ReuseList {
    /** */
    private final ReuseTree[] trees = new ReuseTree[16];

    /** */
    private final GridCacheContext<?,?> cctx;

    /**
     * @param cctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    public ReuseList(GridCacheContext<?,?> cctx) throws IgniteCheckedException {
        this.cctx = cctx;

        PageMemory pageMem = cctx.shared().database().pageMemory();

        MetadataStorage metaStore = cctx.shared().database().meta();

        for (int i = 0; i < trees.length; i++) {
            String idxName = i + "##" + cctx.cacheId() + "_reuse";

            IgniteBiTuple<FullPageId,Boolean> t = metaStore.getOrAllocateForIndex(cctx.cacheId(), idxName);

            trees[i] = new ReuseTree(cctx.cacheId(), pageMem, t.get1(), t.get2());
        }
    }

    /**
     * @return Page ID.
     * @throws IgniteCheckedException If failed.
     */
    public FullPageId take() throws IgniteCheckedException {
        return trees[ThreadLocalRandom.current().nextInt(trees.length)].removeFirst();
    }

    /**
     * @param fullPageId Page ID.
     * @throws IgniteCheckedException If failed.
     */
    public void put(FullPageId fullPageId) throws IgniteCheckedException {
        trees[ThreadLocalRandom.current().nextInt(trees.length)].put(fullPageId);
    }
}
