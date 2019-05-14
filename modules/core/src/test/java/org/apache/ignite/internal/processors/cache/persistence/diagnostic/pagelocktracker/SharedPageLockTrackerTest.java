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

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LockTrackerFactory.HEAP_LOG;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LockTrackerFactory.HEAP_STACK;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LockTrackerFactory.OFF_HEAP_LOG;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LockTrackerFactory.OFF_HEAP_STACK;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.dumpprocessors.ToStringDumpProcessor.toStringDump;
import static org.junit.Assert.assertEquals;

/**
 *
 */
public class SharedPageLockTrackerTest extends AbstractPageLockTest {
    /** */
    @Test
    public void testTakeDumpByCount() throws Exception {
        int[] trackerTypes = new int[] {HEAP_STACK, HEAP_LOG, OFF_HEAP_STACK, OFF_HEAP_LOG};

        LockTrackerFactory.DEFAULT_CAPACITY = 512;

        for (int i = 0; i < trackerTypes.length; i++) {
            LockTrackerFactory.DEFAULT_TYPE = trackerTypes[i];

            doTestTakeDumpByCount(5, 1, 10, 1);

            doTestTakeDumpByCount(5, 2, 10, 2);

            doTestTakeDumpByCount(10, 3, 20, 4);

            doTestTakeDumpByCount(20, 6, 40, 8);
        }
    }

    /** */
    @Test
    public void testTakeDumpByTime() throws Exception {
        int[] trackerTypes = new int[] {HEAP_STACK, HEAP_LOG, OFF_HEAP_STACK, OFF_HEAP_LOG};

        LockTrackerFactory.DEFAULT_CAPACITY = 512;

        for (int i = 0; i < trackerTypes.length; i++) {
            LockTrackerFactory.DEFAULT_TYPE = trackerTypes[i];

            doTestTakeDumpByTime(5, 1, 40_000, 1);

            doTestTakeDumpByTime(5, 2, 20_000, 2);

            doTestTakeDumpByTime(10, 3, 10_000, 4);

            doTestTakeDumpByTime(20, 6, 10_000, 8);
        }
    }

    /**
     *
     */
    private void doTestTakeDumpByCount(
        int pagesCnt,
        int structuresCnt,
        int dumpCnt,
        int threads
    ) throws IgniteCheckedException, InterruptedException {
        SharedPageLockTracker sharedPageLockTracker = new SharedPageLockTracker();

        List<PageMeta> pageMetas = new ArrayList<>(pagesCnt);

        int id = 1;

        for (int i = 0; i < pagesCnt; i++)
            pageMetas.add(new PageMeta((id++) % structuresCnt, id++, id++, id++));

        List<PageLockListener> pageLsnrs = new ArrayList<>();

        for (int i = 0; i < structuresCnt; i++)
            pageLsnrs.add(sharedPageLockTracker.registrateStructure(String.valueOf(i)));

        AtomicBoolean stop = new AtomicBoolean();

        CountDownLatch awaitThreadStartLatch = new CountDownLatch(threads);

        IgniteInternalFuture f = GridTestUtils.runMultiThreadedAsync(() -> {
            awaitThreadStartLatch.countDown();

            List<PageLockListener> locks = new ArrayList<>(pageLsnrs);
            List<PageMeta> pages = new ArrayList<>(pageMetas);

            while (!stop.get()) {
                Collections.shuffle(locks);
                Collections.shuffle(pages);

                for (PageLockListener lsnr : locks) {
                    for (PageMeta pageMeta : pages) {
                        awaitRandom(50);

                        lsnr.onBeforeReadLock(pageMeta.structureId, pageMeta.pageId, pageMeta.page);

                        awaitRandom(50);

                        lsnr.onReadLock(pageMeta.structureId, pageMeta.pageId, pageMeta.page, pageMeta.pageAddr);
                    }
                }

                awaitRandom(10);

                Collections.reverse(locks);
                Collections.reverse(pages);

                for (PageLockListener lsnr : locks) {
                    for (PageMeta pageMeta : pages) {
                        awaitRandom(50);

                        lsnr.onReadUnlock(pageMeta.structureId, pageMeta.pageId, pageMeta.page, pageMeta.pageAddr);
                    }
                }
            }
        }, threads, "PageLocker");

        awaitThreadStartLatch.await();

        for (int i = 0; i < dumpCnt; i++) {
            awaitRandom(1000);

            ThreadPageLocksDumpLock dump = sharedPageLockTracker.dump();

            System.out.println(toStringDump(dump));

            assertEquals(threads, dump.threadStates.size());
            assertEquals(0, dump.threadStates.stream().filter(e -> e.invalidContext != null).count());
        }

        stop.set(true);

        f.get();
    }

    /**
     *
     */
    private void doTestTakeDumpByTime(
        int pagesCnt,
        int structuresCnt,
        int dumpTime,
        int threads
    ) throws IgniteCheckedException, InterruptedException {
        SharedPageLockTracker sharedPageLockTracker = new SharedPageLockTracker();

        List<PageMeta> pageMetas = new ArrayList<>(pagesCnt);

        int id = 1;

        for (int i = 0; i < pagesCnt; i++)
            pageMetas.add(new PageMeta((id++) % structuresCnt, id++, id++, id++));

        List<PageLockListener> pageLsnrs = new ArrayList<>();

        for (int i = 0; i < structuresCnt; i++)
            pageLsnrs.add(sharedPageLockTracker.registrateStructure(String.valueOf(i)));

        AtomicBoolean stop = new AtomicBoolean();

        CountDownLatch awaitThreadStartLatch = new CountDownLatch(threads);

        IgniteInternalFuture f = GridTestUtils.runMultiThreadedAsync(() -> {
            awaitThreadStartLatch.countDown();

            List<PageLockListener> locks = new ArrayList<>(pageLsnrs);
            List<PageMeta> pages = new ArrayList<>(pageMetas);

            while (!stop.get()) {
                Collections.shuffle(locks);
                Collections.shuffle(pages);

                for (PageLockListener lsnr : locks) {
                    for (PageMeta pageMeta : pages) {
                        //awaitRandom(5);

                        lsnr.onBeforeReadLock(pageMeta.structureId, pageMeta.pageId, pageMeta.page);

                        //awaitRandom(5);

                        lsnr.onReadLock(pageMeta.structureId, pageMeta.pageId, pageMeta.page, pageMeta.pageAddr);
                    }
                }

                Collections.reverse(locks);
                Collections.reverse(pages);

                for (PageLockListener lsnr : locks) {
                    for (PageMeta pageMeta : pages) {
                        // awaitRandom(5);

                        lsnr.onReadUnlock(pageMeta.structureId, pageMeta.pageId, pageMeta.page, pageMeta.pageAddr);
                    }
                }
            }
        }, threads, "PageLocker");

        IgniteInternalFuture dumpF = GridTestUtils.runAsync(() -> {
            try {
                awaitThreadStartLatch.await();
            }
            catch (InterruptedException e) {
                // Ignore.
                return;
            }

            while (!stop.get()) {
                awaitRandom(20);

                ThreadPageLocksDumpLock dump = sharedPageLockTracker.dump();

                System.out.println(toStringDump(dump));

                assertEquals(threads, dump.threadStates.size());
                assertEquals(0, dump.threadStates.stream().filter(e -> e.invalidContext != null).count());
            }
        });

        Thread.sleep(dumpTime);

        stop.set(true);

        f.get();
    }

    /** */
    private static class PageMeta {
        /** */
        final int structureId;
        /** */
        final long pageId;
        /** */
        final long page;
        /** */
        final long pageAddr;

        /** */
        private PageMeta(
            int structureId,
            long pageId,
            long page,
            long pageAddr
        ) {
            this.structureId = structureId;
            this.pageId = pageId;
            this.page = page;
            this.pageAddr = pageAddr;
        }
    }
}