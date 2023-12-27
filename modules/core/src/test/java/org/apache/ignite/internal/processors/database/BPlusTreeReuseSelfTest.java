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

package org.apache.ignite.internal.processors.database;

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManager;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseListImpl;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.testframework.junits.GridTestKernalContext;

import static org.apache.ignite.internal.pagemem.PageIdUtils.effectivePageId;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test with reuse list.
 */
public class BPlusTreeReuseSelfTest extends BPlusTreeSelfTest {
    /** {@inheritDoc} */
    @Override protected ReuseList createReuseList(
        int cacheId,
        PageMemory pageMem,
        long rootId,
        boolean initNew
    ) throws IgniteCheckedException {
        PageLockTrackerManager pageLockTrackerMgr = mock(PageLockTrackerManager.class);

        when(pageLockTrackerMgr.createPageLockTracker(anyString())).thenReturn(new TestPageLockListener());

        return new TestReuseList(
            cacheId,
            "test",
            pageMem,
            rootId,
            pageLockTrackerMgr,
            new GridTestKernalContext(log),
            initNew
        );
    }

    /** {@inheritDoc} */
    @Override protected void assertNoLocks() {
        super.assertNoLocks();

        assertTrue(TestReuseList.checkNoLocks());
    }

    /**
     *
     */
    private static class TestReuseList extends ReuseListImpl {
        /**
         * @param cacheId    Cache ID.
         * @param name       Name (for debug purpose).
         * @param pageMem    Page memory.
         * @param metaPageId Metadata page ID.
         * @param initNew    {@code True} if new metadata should be initialized.
         * @throws IgniteCheckedException If failed.
         */
        public TestReuseList(
            int cacheId,
            String name,
            PageMemory pageMem,
            long metaPageId,
            PageLockTrackerManager pageLockTrackerManager,
            GridKernalContext ctx,
            boolean initNew
        ) throws IgniteCheckedException {
            super(cacheId, name, pageMem, null, metaPageId, initNew, pageLockTrackerManager, ctx, null, PageIdAllocator.FLAG_IDX);
        }

        /**
         *
         */
        static boolean checkNoLocks() {
            return TestPageLockListener.readLocks.get().isEmpty() && TestPageLockListener.writeLocks.get().isEmpty();
        }
    }

    /** */
    private static class TestPageLockListener implements PageLockListener {

        /** */
        private static final ThreadLocal<Set<Long>> readLocks = ThreadLocal.withInitial(HashSet::new);

        /** */
        private static final ThreadLocal<Set<Long>> writeLocks = ThreadLocal.withInitial(HashSet::new);

        /** {@inheritDoc} */
        @Override public void onBeforeReadLock(int cacheId, long pageId, long page) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onReadLock(int cacheId, long pageId, long page, long pageAddr) {
            checkPageId(pageId, pageAddr);

            assertTrue(readLocks.get().add(pageId));
        }

        /** {@inheritDoc} */
        @Override public void onReadUnlock(int cacheId, long pageId, long page, long pageAddr) {
            checkPageId(pageId, pageAddr);

            assertTrue(readLocks.get().remove(pageId));
        }

        /** {@inheritDoc} */
        @Override public void onBeforeWriteLock(int cacheId, long pageId, long page) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onWriteLock(int cacheId, long pageId, long page, long pageAddr) {
            if (pageAddr == 0L)
                return; // Failed to lock.

            checkPageId(pageId, pageAddr);

            assertTrue(writeLocks.get().add(pageId));
        }

        /** {@inheritDoc} */
        @Override public void onWriteUnlock(int cacheId, long pageId, long page, long pageAddr) {
            assertEquals(effectivePageId(pageId), effectivePageId(PageIO.getPageId(pageAddr)));

            assertTrue(writeLocks.get().remove(pageId));
        }

        /** {@inheritDoc} */
        @Override public void close() {
            // No-op.
        }
    }
}
