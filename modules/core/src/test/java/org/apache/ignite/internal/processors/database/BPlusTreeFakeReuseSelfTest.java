/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.database;

import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseBag;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;

/**
 *
 */
public class BPlusTreeFakeReuseSelfTest extends BPlusTreeSelfTest {
    /** {@inheritDoc} */
    @Override protected ReuseList createReuseList(int cacheId, PageMemory pageMem, long rootId, boolean initNew)
        throws IgniteCheckedException {
        return new FakeReuseList();
    }

    /**
     * Fake reuse list.
     */
    private static class FakeReuseList implements ReuseList {
        /** */
        private final ConcurrentLinkedDeque<Long> deque = new ConcurrentLinkedDeque<>();

        /** {@inheritDoc} */
        @Override public void addForRecycle(ReuseBag bag) throws IgniteCheckedException {
            long pageId;

            while ((pageId = bag.pollFreePage()) != 0L)
                deque.addFirst(pageId);
        }

        /** {@inheritDoc} */
        @Override public long takeRecycledPage() throws IgniteCheckedException {
            Long pageId = deque.pollFirst();

            return pageId == null ? 0L : pageId;
        }

        /** {@inheritDoc} */
        @Override public long recycledPagesCount() throws IgniteCheckedException {
            return deque.size();
        }
    }
}
