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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CyclicBarrier;
import javax.cache.event.EventType;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class CacheContinuousQueryEventBufferTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBuffer1() throws Exception {
        testBuffer(1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBuffer2() throws Exception {
        for (int i = 0; i < 10; i++) {
            log.info("Iteration: " + i);

            testBuffer(10);
        }
    }

    /**
     * @param threads Threads number.
     * @throws Exception If failed.
     */
    private void testBuffer(int threads) throws Exception {
        long seed = System.nanoTime();

        Random rnd = new Random(seed);

        log.info("Start test, seed: " + seed);

        for (int i = 0; i < 10; i++) {
            int cnt = rnd.nextInt(10_000) + 1;

            testBuffer(rnd, new CacheContinuousQueryEventBuffer(0), cnt, 1, 0.5f, threads);
            testBuffer(rnd, new CacheContinuousQueryEventBuffer(0), cnt, 1, 0.9f, threads);
            testBuffer(rnd, new CacheContinuousQueryEventBuffer(0), cnt, 1, 0.99f, threads);
            testBuffer(rnd, new CacheContinuousQueryEventBuffer(0), cnt, 1, 0.01f, threads);
            testBuffer(rnd, new CacheContinuousQueryEventBuffer(0), cnt, 1, 0.f, threads);
        }

        CacheContinuousQueryEventBuffer b = new CacheContinuousQueryEventBuffer(0);

        long cntr = 1;

        for (int i = 0; i < 10; i++) {
            int cnt = rnd.nextInt(10_000) + 1;
            float ratio = rnd.nextFloat();

            testBuffer(rnd, b, cnt, cntr, ratio, threads);

            cntr += cnt;
        }
    }

    /**
     * @param rnd Random.
     * @param b Buffer.
     * @param cnt Entries count.
     * @param cntr Current counter.
     * @param filterRatio Filtered events ratio.
     * @param threads Threads number.
     * @throws Exception If failed.
     */
    private void testBuffer(Random rnd,
        final CacheContinuousQueryEventBuffer b,
        int cnt,
        long cntr,
        float filterRatio,
        int threads)
        throws Exception
    {
        List<CacheContinuousQueryEntry> expEntries = new ArrayList<>();

        List<CacheContinuousQueryEntry> entries = new ArrayList<>();

        long filtered = b.currentFiltered();

        for (int i = 0; i < cnt; i++) {
            CacheContinuousQueryEntry entry = new CacheContinuousQueryEntry(
                0,
                EventType.CREATED,
                null,
                null,
                null,
                false,
                0,
                cntr,
                null,
                (byte)0);

            entries.add(entry);

            if (rnd.nextFloat() < filterRatio) {
                entry.markFiltered();

                filtered++;
            }
            else {
                CacheContinuousQueryEntry expEntry = new CacheContinuousQueryEntry(
                    0,
                    EventType.CREATED,
                    null,
                    null,
                    null,
                    false,
                    0,
                    cntr,
                    null,
                    (byte)0);

                expEntry.filteredCount(filtered);

                expEntries.add(expEntry);

                filtered = 0;
            }

            cntr++;
        }

        Collections.shuffle(entries, rnd);

        List<CacheContinuousQueryEntry> actualEntries = new ArrayList<>(expEntries.size());

        if (threads == 1) {
            for (int i = 0; i < entries.size(); i++) {
                Object o = entries.get(i);

                Object res = b.processEntry((CacheContinuousQueryEntry)o, false);

                if (res != null) {
                    if (res instanceof CacheContinuousQueryEntry)
                        actualEntries.add((CacheContinuousQueryEntry)res);
                    else
                        actualEntries.addAll((List<CacheContinuousQueryEntry>)res);
                }
            }
        }
        else {
            final CyclicBarrier barrier = new CyclicBarrier(threads);

            final ConcurrentLinkedQueue<CacheContinuousQueryEntry> q = new ConcurrentLinkedQueue<>(entries);

            final ConcurrentSkipListMap<Long, CacheContinuousQueryEntry> act0 = new ConcurrentSkipListMap<>();

            GridTestUtils.runMultiThreaded(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    barrier.await();

                    Object o;

                    while ((o = q.poll()) != null) {
                        Object res = b.processEntry((CacheContinuousQueryEntry)o, false);

                        if (res != null) {
                            if (res instanceof CacheContinuousQueryEntry)
                                act0.put(((CacheContinuousQueryEntry)res).updateCounter(), (CacheContinuousQueryEntry)res);
                            else {
                                for (CacheContinuousQueryEntry e : ((List<CacheContinuousQueryEntry>)res))
                                    act0.put(e.updateCounter(), e);
                            }
                        }
                    }

                    return null;
                }
            }, threads, "test");

            actualEntries.addAll(act0.values());
        }

        assertEquals(expEntries.size(), actualEntries.size());

        for (int i = 0; i < expEntries.size(); i++) {
            CacheContinuousQueryEntry expEvt = expEntries.get(i);
            CacheContinuousQueryEntry actualEvt = actualEntries.get(i);

            assertEquals(expEvt.updateCounter(), actualEvt.updateCounter());
            assertEquals(expEvt.filteredCount(), actualEvt.filteredCount());
        }
    }
}
