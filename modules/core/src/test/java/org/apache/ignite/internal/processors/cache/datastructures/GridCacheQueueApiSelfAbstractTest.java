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

package org.apache.ignite.internal.processors.cache.datastructures;

import org.apache.ignite.*;
import org.apache.ignite.cache.datastructures.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.testframework.junits.common.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Queue basic tests.
 */
public abstract class GridCacheQueueApiSelfAbstractTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int QUEUE_CAPACITY = 3;

    /** */
    private static final int THREAD_NUM = 2;

    /** */
    private static AtomicInteger cntr = new AtomicInteger();

    /**
     * Default constructor.
     *
     */
    protected GridCacheQueueApiSelfAbstractTest() {
        super(true /** Start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        cfg.setLocalHost("127.0.0.1");

        return cfg;
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPrepareQueue() throws Exception {
        // Random sequence names.
        String queueName1 = UUID.randomUUID().toString();
        String queueName2 = UUID.randomUUID().toString();

        IgniteQueue queue1 = grid().cache(null).dataStructures().queue(queueName1, 0, false, true);
        IgniteQueue queue2 = grid().cache(null).dataStructures().queue(queueName2, 0, false, true);
        IgniteQueue queue3 = grid().cache(null).dataStructures().queue(queueName1, 0, false, true);

        assertNotNull(queue1);
        assertNotNull(queue2);
        assertNotNull(queue3);
        assert queue1.equals(queue3);
        assert queue3.equals(queue1);
        assert !queue3.equals(queue2);

        assert grid().cache(null).dataStructures().removeQueue(queueName1);
        assert grid().cache(null).dataStructures().removeQueue(queueName2);
        assert !grid().cache(null).dataStructures().removeQueue(queueName1);
        assert !grid().cache(null).dataStructures().removeQueue(queueName2);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testAddUnbounded() throws Exception {
        // Random queue name.
        String queueName = UUID.randomUUID().toString();

        String val = UUID.randomUUID().toString();

        IgniteQueue<String> queue = grid().cache(null).dataStructures().queue(queueName, 0, false, true);

        assert queue.add(val);

        assert val.equals(queue.poll());
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testAddDeleteUnbounded() throws Exception {
        // Random queue name.
        String queueName = UUID.randomUUID().toString();

        String val = UUID.randomUUID().toString();

        IgniteQueue<String> queue = grid().cache(null).dataStructures().queue(queueName, 0, false, true);

        assert queue.add(val);

        assert queue.remove(val);

        assert queue.isEmpty();
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testCollectionMethods() throws Exception {
        // Random queue name.
        String queueName = UUID.randomUUID().toString();

        // TODO was LIFO
        IgniteQueue<SameHashItem> queue = grid().cache(null).dataStructures().queue(queueName, 0, false, true);

        int retries = 100;

        // Initialize queue.
        for (int i = 0; i < retries; i++)
            queue.addAll(Arrays.asList(new SameHashItem(Integer.toString(i)), new SameHashItem(Integer.toString(i))));

        // Get arrays from queue.
        assertEquals(retries * 2, queue.toArray().length);

        SameHashItem[] arr2 = new SameHashItem[retries * 3];

        Object[] arr3 = queue.toArray(arr2);

        assertEquals(arr2, arr3);
        assertEquals(arr3[0], new SameHashItem("0"));

        // Check queue items.
        assertEquals(retries * 2, queue.size());

        assertTrue(queue.contains(new SameHashItem(Integer.toString(14))));

        assertFalse(queue.contains(new SameHashItem(Integer.toString(144))));

        Collection<SameHashItem> col1 = Arrays.asList(new SameHashItem(Integer.toString(14)),
            new SameHashItem(Integer.toString(14)), new SameHashItem(Integer.toString(18)));

        assertTrue(queue.containsAll(col1));

        Collection<SameHashItem> col2 = Arrays.asList(new SameHashItem(Integer.toString(245)),
            new SameHashItem(Integer.toString(14)), new SameHashItem(Integer.toString(18)));

        assertFalse(queue.containsAll(col2));

        // Try to remove item.
        assertTrue(queue.remove(new SameHashItem(Integer.toString(14))));

        assertEquals((retries * 2) - 1, queue.size());

        assertTrue(queue.contains(new SameHashItem(Integer.toString(14))));

        assertTrue(queue.remove(new SameHashItem(Integer.toString(14))));

        assertEquals((retries - 1) * 2, queue.size());

        assertFalse(queue.remove(new SameHashItem(Integer.toString(14))));

        // Try to remove some items.
        assertTrue(queue.contains(new SameHashItem(Integer.toString(33))));

        assertTrue(queue.removeAll(Arrays.asList(new SameHashItem(Integer.toString(15)),
            new SameHashItem(Integer.toString(14)), new SameHashItem(Integer.toString(33)),
            new SameHashItem(Integer.toString(1)))));

        assertFalse(queue.contains(new SameHashItem(Integer.toString(33))));

        // Try to retain all items.
        assertTrue(queue.retainAll(Arrays.asList(new SameHashItem(Integer.toString(15)),
            new SameHashItem(Integer.toString(14)), new SameHashItem(Integer.toString(33)),
            new SameHashItem(Integer.toString(1)))));

        assertFalse(queue.contains(new SameHashItem(Integer.toString(2))));

        assert queue.isEmpty();
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testAddPollUnbounded() throws Exception {
        // Random queue name.
        String queueName = UUID.randomUUID().toString();

        IgniteQueue<String> queue = grid().cache(null).dataStructures().queue(queueName, 0, false, true);

        assert queue.add("1");

        assert queue.add("2");

        assert queue.add("3");

        assertEquals("1", queue.poll());
        assertEquals("2", queue.poll());
        assertEquals("3", queue.poll());
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testAddPeekUnbounded() throws Exception {
        // Random queue name.
        String queueName = UUID.randomUUID().toString();

        IgniteQueue<String> queue = grid().cache(null).dataStructures().queue(queueName, 0, true, true);

        String item1 = "1";
        assert queue.add(item1);

        String item2 = "2";
        assert queue.add(item2);

        String item3 = "3";
        assert queue.add(item3);

        assert item1.equals(queue.peek());
        assert item1.equals(queue.peek());
        assert !item2.equals(queue.peek());
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testIterator() throws Exception {
        // Random queue name.
        String queueName = UUID.randomUUID().toString();

        IgniteQueue<String> queue = grid().cache(null).dataStructures().queue(queueName, 0, false, true);

        for (int i = 0; i < 100; i++)
            assert queue.add(Integer.toString(i));

        Iterator<String> iter1 = queue.iterator();

        int cnt = 0;
        for (int i = 0; i < 100; i++) {
            assertNotNull(iter1.next());
            cnt++;
        }

        assertEquals(100, queue.size());
        assertEquals(100, cnt);

        assertNotNull(queue.take());
        assertNotNull(queue.take());
        assertTrue(queue.remove("33"));
        assertTrue(queue.remove("77"));

        assertEquals(96, queue.size());

        Iterator<String> iter2 = queue.iterator();

        try {
            iter2.remove();
        }
        catch (IllegalStateException e) {
            info("Caught expected exception: " + e);
        }

        iter2.next();
        iter2.remove();

        cnt = 0;
        while (iter2.hasNext()) {
            assertNotNull(iter2.next());
            cnt++;
        }

        assertEquals(95, cnt);
        assertEquals(95, queue.size());

        iter2.remove();
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPutGetUnbounded() throws Exception {
        // Random queue name.
        String queueName = UUID.randomUUID().toString();

        IgniteQueue<String> queue = grid().cache(null).dataStructures()
            .queue(queueName, QUEUE_CAPACITY, false, true);

        String thName = Thread.currentThread().getName();

        for (int i = 0; i < 5; i++) {
            queue.put(thName);
            queue.peek();
            queue.take();
        }

        assert queue.isEmpty() : queue.size();
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPutGetMultithreadUnbounded() throws Exception {
        // Random queue name.
        String queueName = UUID.randomUUID().toString();

        final IgniteQueue<String> queue = grid().cache(null).dataStructures()
            .queue(queueName, QUEUE_CAPACITY, false, true);

        multithreaded(new Callable<String>() {
                @Override public String call() throws Exception {
                    String thName = Thread.currentThread().getName();

                    for (int i = 0; i < 5; i++) {
                        queue.put(thName);
                        queue.peek();
                        queue.take();
                    }

                    return "";
                }
            }, THREAD_NUM);

        assert queue.isEmpty() : queue.size();
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPutGetMultithreadBounded() throws Exception {
        // Random queue name.
        String queueName = UUID.randomUUID().toString();

        final IgniteQueue<String> queue = grid().cache(null).dataStructures()
            .queue(queueName, QUEUE_CAPACITY, false, true);

        multithreaded(new Callable<String>() {
                @Override public String call() throws Exception {
                    String thName = Thread.currentThread().getName();

                    for (int i = 0; i < QUEUE_CAPACITY * 5; i++) {
                        queue.put(thName);
                        queue.peek();
                        queue.take();
                    }
                    return "";
                }
            }, THREAD_NUM);

        assert queue.isEmpty() : queue.size();
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testQueueRemoveMultithreadBounded() throws Exception {
        // Random queue name.
        final String queueName = UUID.randomUUID().toString();

        final AtomicInteger rmvNum = new AtomicInteger(0);

        final IgniteQueue<String> queue = grid().cache(null).dataStructures()
            .queue(queueName, QUEUE_CAPACITY, false, true);

        final CountDownLatch putLatch = new CountDownLatch(THREAD_NUM);

        final CountDownLatch clearLatch = new CountDownLatch(THREAD_NUM);

        for (int t = 0; t < THREAD_NUM; t++) {
            Thread th = new Thread(new Runnable() {
                @Override public void run() {
                    if (log.isDebugEnabled())
                        log.debug("Thread has been started." + Thread.currentThread().getName());

                    try {
                        // Thread must be blocked on put operation.
                        for (int i = 0; i < (QUEUE_CAPACITY * THREAD_NUM); i++)
                            queue.offer("anything", 3, TimeUnit.MINUTES);

                        fail("Queue failed");
                    }
                    catch (IgniteException e) {
                        putLatch.countDown();

                        assert e.getMessage().contains("removed");

                        assert queue.removed();
                    }

                    if (log.isDebugEnabled())
                        log.debug("Thread has been stopped." + Thread.currentThread().getName());

                }
            });
            th.start();
        }

        for (int t = 0; t < THREAD_NUM; t++) {
            Thread th = new Thread(new Runnable() {
                @Override public void run() {
                    try {
                        if (grid().cache(null).dataStructures().removeQueue(queueName, 0)) {
                            rmvNum.incrementAndGet();

                            if (log.isDebugEnabled())
                                log.debug("Queue removed [queue " + queue + ']');
                        }
                    }
                    catch (IgniteCheckedException e) {
                        info("Caught expected exception: " + e.getMessage());

                        assert queue.removed();
                    }
                    finally {
                        clearLatch.countDown();
                    }
                }
            });
            th.start();
        }

        assert putLatch.await(3, TimeUnit.MINUTES);

        assert clearLatch.await(3, TimeUnit.MINUTES);

        assert rmvNum.get() == 1 : "Expected 1 but was " + rmvNum.get();

        try {
            assert queue.isEmpty() : queue.size();
            fail("Queue must be removed.");
        }
        catch (IgniteException e) {
            assert e.getMessage().contains("removed");

            assert queue.removed();
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testQueueRemoveMultithreadUnbounded() throws Exception {
        // Random queue name.
        final String queueName = UUID.randomUUID().toString();

        final AtomicInteger rmvNum = new AtomicInteger(0);

        final IgniteQueue<String> queue = grid().cache(null).dataStructures()
            .queue(queueName, QUEUE_CAPACITY, false, true);

        final CountDownLatch takeLatch = new CountDownLatch(THREAD_NUM);

        final CountDownLatch clearLatch = new CountDownLatch(THREAD_NUM);

        for (int t = 0; t < THREAD_NUM; t++) {
            Thread th = new Thread(new Runnable() {
                @Override public void run() {
                    if (log.isDebugEnabled())
                        log.debug("Thread has been started." + Thread.currentThread().getName());

                    try {
                        // Thread must be blocked on take operation.
                        for (int i = 0; i < (QUEUE_CAPACITY * THREAD_NUM); i++) {
                            queue.poll(3, TimeUnit.MINUTES);

                            fail("Queue failed");
                        }
                    }
                    catch (IgniteException e) {
                        takeLatch.countDown();

                        assert e.getMessage().contains("removed");

                        assert queue.removed();

                        if (log.isDebugEnabled())
                            log.debug("Thread has been stopped." + Thread.currentThread().getName());
                    }
                }
            });
            th.start();
        }

        for (int t = 0; t < THREAD_NUM; t++) {
            Thread th = new Thread(new Runnable() {
                @Override public void run() {
                    try {
                        if (grid().cache(null).dataStructures().removeQueue(queueName, 0)) {
                            rmvNum.incrementAndGet();

                            if (log.isDebugEnabled())
                                log.debug("Queue has been removed." + queue);
                        }
                    }
                    catch (IgniteCheckedException e) {
                        info("Caught expected exception: " + e.getMessage());

                        assert queue.removed();
                    }
                    finally {
                        clearLatch.countDown();
                    }
                }
            });
            th.start();
        }

        assert takeLatch.await(3, TimeUnit.MINUTES);

        assert clearLatch.await(3, TimeUnit.MINUTES);

        assert rmvNum.get() == 1 : "Expected 1 but was " + rmvNum.get();

        try {
            assert queue.isEmpty() : queue.size();

            fail("Queue must be removed.");
        }
        catch (IgniteException e) {
            assert e.getMessage().contains("removed");

            assert queue.removed();
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPutRemoveAll() throws Exception {
        final Collection<Integer> keys = new LinkedList<>();

        for (int j = 0; j < QUEUE_CAPACITY; j++) {
            for (int i = 0; i < QUEUE_CAPACITY; i++) {
                int key = cntr.getAndIncrement();

                keys.add(key);

                grid().cache(null).put(key, "123");
            }

            multithreaded(
                new Callable<String>() {
                    @Override public String call() throws Exception {
                        info("Removing all keys: " + keys);

                        grid().cache(null).removeAll(keys);

                        info("Thread finished for keys: " + keys);

                        return "";
                    }
                },
                THREAD_NUM
            );
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPutRemoveUnbounded() throws Exception {
        // Random queue name.
        String queueName = UUID.randomUUID().toString();

        IgniteQueue<String> queue = grid().cache(null).dataStructures()
            .queue(queueName, 0, false, true);

        String thread = Thread.currentThread().getName();

        for (int i = 0; i < QUEUE_CAPACITY; i++)
            queue.put(thread);

        info("Finished loop 1: " + thread);

        queue.clear();

        info("Cleared queue 1: " + thread);

        assert queue.isEmpty() : "Queue must be empty. " + queue.size();
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPutRemoveMultiThreadedUnbounded() throws Exception {
        // Random queue name.
        String queueName = UUID.randomUUID().toString();

        final IgniteQueue<String> queue = grid().cache(null).dataStructures()
            .queue(queueName, 0, false, true);

        multithreaded(
            new Callable<String>() {
                @Override public String call() throws Exception {
                    String thread = Thread.currentThread().getName();

                    for (int i = 0; i < QUEUE_CAPACITY; i++)
                        queue.put(thread);

                    info("Finished loop 1: " + thread);

                    queue.clear();

                    info("Cleared queue 1: " + thread);

                    return "";
                }
            },
            THREAD_NUM
        );

        assert queue.isEmpty() : "Queue must be empty. " + queue.size();
    }

    /**
     *  Test class with the same hash code.
     */
    private static class SameHashItem implements Serializable {
        /** Data field*/
        private final String s;

        /**
         * @param s Item data.
         */
        private SameHashItem(String s) {
            this.s = s;
        }

        /**
         * @return Priority.
         */
        String data() {
            return s;
        }

        @Override public int hashCode() {
            return 0;
        }

        @Override public boolean equals(Object o) {
            {
                if (this == o)
                    return true;
                if (o instanceof SameHashItem) {
                    SameHashItem i = (SameHashItem)o;
                    return s.equals(i.s);
                }

                return false;
            }
        }

        @Override public String toString() {
            return S.toString(SameHashItem.class, this);
        }
    }
}
