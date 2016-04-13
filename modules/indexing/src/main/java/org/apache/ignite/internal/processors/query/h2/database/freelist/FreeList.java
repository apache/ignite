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

package org.apache.ignite.internal.processors.query.h2.database.freelist;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jsr166.ConcurrentHashMap8;

/**
 * Free data page list.
 */
public class FreeList {
    /** */
    private final GridCacheContext<?,?> cctx;

    /** */
    private final PageMemory pageMem;

    /** */
    private final ConcurrentHashMap8<Integer,GridFutureAdapter<FreeTree>> trees = new ConcurrentHashMap8<>();

    /**
     * @param cctx Cache context.
     */
    public FreeList(GridCacheContext<?,?> cctx) {
        assert cctx != null;

        this.cctx = cctx;

        pageMem = cctx.shared().database().pageMemory();

        assert pageMem != null;
    }

    /**
     * @param part Partition.
     * @param neededSpace Needed free space.
     * @return Page ID or {@code null} if it was impossible to find one.
     * @throws IgniteCheckedException If failed.
     */
    public FullPageId take(int part, short neededSpace) throws IgniteCheckedException {
        assert part >= 0: part;
        assert neededSpace > 0 && neededSpace < Short.MAX_VALUE: neededSpace;

        FreeTree tree = tree(part);

        assert tree != null;

        FreeItem res = tree.removeCeil(new FreeItem(neededSpace, dispersion(), 0, 0));

        assert res == null || (res.pageId() != 0 && res.cacheId() == cctx.cacheId()): res;

        return res;
    }

    /**
     * @return Random dispersion value.
     */
    private static short dispersion() {
        return (short)ThreadLocalRandom.current().nextInt(Short.MIN_VALUE, Short.MAX_VALUE);
    }

    /**
     * @param part Partition.
     * @return Tree.
     * @throws IgniteCheckedException If failed.
     */
    private FreeTree tree(Integer part) throws IgniteCheckedException {
        GridFutureAdapter<FreeTree> fut = trees.get(part);

        if (fut != null) {
            fut = new GridFutureAdapter<>();

            if (trees.putIfAbsent(part, fut) != null)
                fut = trees.get(part);
            else {
                // Index name will be the same across restarts.
                String idxName = part + "$$" + cctx.cacheId() + "_free";

                IgniteBiTuple<FullPageId,Boolean> t = cctx.shared().database().meta()
                    .getOrAllocateForIndex(cctx.cacheId(), idxName);

                fut.onDone(new FreeTree(pageMem, cctx.cacheId(), part, t.get1(), t.get2()));
            }
        }

        return fut.get();
    }
}
