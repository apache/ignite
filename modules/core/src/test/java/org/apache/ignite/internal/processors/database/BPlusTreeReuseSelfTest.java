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
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseListImpl;

import static org.apache.ignite.internal.pagemem.PageIdUtils.effectivePageId;

/**
 * Test with reuse list.
 */
public class BPlusTreeReuseSelfTest extends BPlusTreeSelfTest {
    /** {@inheritDoc} */
    @Override protected ReuseList createReuseList(int cacheId, PageMemory pageMem, long rootId, boolean initNew)
        throws IgniteCheckedException {
        return new TestReuseList(cacheId, "test", pageMem, null, rootId, initNew);
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
        /** */
        private static ThreadLocal<Set<Long>> readLocks = new ThreadLocal<Set<Long>>() {
            @Override protected Set<Long> initialValue() {
                return new HashSet<>();
            }
        };

        /** */
        private static ThreadLocal<Set<Long>> writeLocks = new ThreadLocal<Set<Long>>() {
            @Override protected Set<Long> initialValue() {
                return new HashSet<>();
            }
        };

        /**
         * @param cacheId    Cache ID.
         * @param name       Name (for debug purpose).
         * @param pageMem    Page memory.
         * @param wal        Write ahead log manager.
         * @param metaPageId Metadata page ID.
         * @param initNew    {@code True} if new metadata should be initialized.
         * @throws IgniteCheckedException If failed.
         */
        public TestReuseList(
            int cacheId,
            String name,
            PageMemory pageMem,
            IgniteWriteAheadLogManager wal,
            long metaPageId,
            boolean initNew
        ) throws IgniteCheckedException {
            super(cacheId, name, pageMem, wal, metaPageId, initNew);
        }

        /** {@inheritDoc} */
        @Override public void onBeforeReadLock(Page page) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onReadLock(Page page, long pageAddr) {
            checkPageId(page, pageAddr);

            assertTrue(readLocks.get().add(page.id()));
        }

        /** {@inheritDoc} */
        @Override public void onReadUnlock(Page page, long pageAddr) {
            checkPageId(page, pageAddr);

            assertTrue(readLocks.get().remove(page.id()));
        }

        /** {@inheritDoc} */
        @Override public void onBeforeWriteLock(Page page) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onWriteLock(Page page, long pageAddr) {
            if (pageAddr == 0L)
                return; // Failed to lock.

            checkPageId(page, pageAddr);

            assertTrue(writeLocks.get().add(page.id()));
        }

        /** {@inheritDoc} */
        @Override public void onWriteUnlock(Page page, long pageAddr) {
            assertEquals(effectivePageId(page.id()), effectivePageId(PageIO.getPageId(pageAddr)));

            assertTrue(writeLocks.get().remove(page.id()));
        }

        static boolean checkNoLocks() {
            return readLocks.get().isEmpty() && writeLocks.get().isEmpty();
        }
    }
}