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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Queue basic tests.
 */
public abstract class GridCacheQueueApiSelfAbstractTest extends IgniteCollectionAbstractTest {
    /** */
    private static final int QUEUE_CAPACITY = 3;

    /** */
    private static final int THREAD_NUM = 2;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
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

        CollectionConfiguration colCfg = config(false);

        IgniteQueue queue1 = grid(0).queue(queueName1, 0, colCfg);
        IgniteQueue queue2 = grid(0).queue(queueName2, 0, colCfg);
        IgniteQueue queue3 = grid(0).queue(queueName1, 0, colCfg);

        assertNotNull(queue1);
        assertNotNull(queue2);
        assertNotNull(queue3);
        assert queue1.equals(queue3);
        assert queue3.equals(queue1);
        assert !queue3.equals(queue2);

        queue1.close();
        queue2.close();
        queue3.close();

        assertNull(grid(0).queue(queueName1, 0, null));
        assertNull(grid(0).queue(queueName2, 0, null));
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

        IgniteQueue<String> queue = grid(0).queue(queueName, 0, config(false));

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

        IgniteQueue<String> queue = grid(0).queue(queueName, 0, config(false));

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

        IgniteQueue<SameHashItem> queue = grid(0).queue(queueName, 0, config(false));

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

        IgniteQueue<String> queue = grid(0).queue(queueName, 0, config(false));

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

        IgniteQueue<String> queue = grid(0).queue(queueName, 0, config(false));

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
     * @throws Exception If failed.
     */
    public void testIterator() throws Exception {
        checkIterator(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIteratorCollocated() throws Exception {
        checkIterator(true);
    }

    /**
     * @param collocated Collocated flag.
     */
    private void checkIterator(boolean collocated) {
        // Random queue name.
        String queueName = UUID.randomUUID().toString();

        IgniteQueue<String> queue = grid(0).queue(queueName, 0, config(collocated));

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

        IgniteQueue<String> queue = grid(0).queue(queueName, QUEUE_CAPACITY, config(false));

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

        final IgniteQueue<String> queue = grid(0).queue(queueName, QUEUE_CAPACITY, config(false));

        multithreaded(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    String thName = Thread.currentThread().getName();

                    for (int i = 0; i < 5; i++) {
                        queue.put(thName);
                        queue.peek();
                        queue.take();
                    }

                    return null;
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

        final IgniteQueue<String> queue = grid(0).queue(queueName, QUEUE_CAPACITY, config(false));

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

        final IgniteQueue<String> queue = grid(0).queue(queueName, QUEUE_CAPACITY, config(false));

        final CountDownLatch putLatch = new CountDownLatch(THREAD_NUM);

        final CountDownLatch clearLatch = new CountDownLatch(THREAD_NUM);

        IgniteInternalFuture<?> offerFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                if (log.isDebugEnabled())
                    log.debug("Thread has been started." + Thread.currentThread().getName());

                try {
                    // Thread must be blocked on put operation.
                    for (int i = 0; i < (QUEUE_CAPACITY * THREAD_NUM); i++)
                        queue.offer("anything", 3, TimeUnit.MINUTES);

                    fail("Queue failed");
                }
                catch (IgniteException | IllegalStateException e) {
                    putLatch.countDown();

                    assert e.getMessage().contains("removed");

                    assert queue.removed();
                }

                if (log.isDebugEnabled())
                    log.debug("Thread has been stopped." + Thread.currentThread().getName());

                return null;
            }
        }, THREAD_NUM, "offer-thread");

        IgniteInternalFuture<?> closeFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                try {
                    IgniteQueue<String> queue = grid(0).queue(queueName, 0, null);

                    if (queue != null)
                        queue.close();
                }
                catch (Exception e) {
                    fail("Unexpected exception: " + e);
                }
                finally {
                    clearLatch.countDown();
                }

                return null;
            }
        }, THREAD_NUM, "close-thread");

        assert putLatch.await(3, TimeUnit.MINUTES);

        assert clearLatch.await(3, TimeUnit.MINUTES);

        offerFut.get();
        closeFut.get();

        try {
            assert queue.isEmpty() : queue.size();

            fail("Queue must be removed.");
        }
        catch (IgniteException | IllegalStateException e) {
            assert e.getMessage().contains("removed");

            assert queue.removed();
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

        IgniteQueue<String> queue = grid(0).queue(queueName, 0, config(false));

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

        final IgniteQueue<String> queue = grid(0).queue(queueName, 0, config(false));

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
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPutRemovePeekPollUnbounded() throws Exception {
        // Random queue name.
        String queueName = UUID.randomUUID().toString();

        IgniteQueue<String> queue = grid(0).queue(queueName, 0, config(false));

        for (int i = 0; i < QUEUE_CAPACITY; i++)
            queue.put("Item-" + i);

        assertEquals(QUEUE_CAPACITY, queue.size());

        queue.remove("Item-1");

        assertEquals(QUEUE_CAPACITY - 1, queue.size());

        assertEquals("Item-0", queue.peek());
        assertEquals("Item-0", queue.poll());
        assertEquals("Item-2", queue.poll());

        assertEquals(0, queue.size());

        queue.clear();

        assertTrue(queue.isEmpty());
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testRemovePeek() throws Exception {
        // Random queue name.
        String queueName = UUID.randomUUID().toString();

        IgniteQueue<String> queue = grid(0).queue(queueName, 0, config(false));

        for (int i = 0; i < 5; i++)
            queue.put("Item-" + i);

        queue.remove("Item-1");

        assertEquals("Item-0", queue.peek());

        queue.remove("Item-2");

        assertEquals("Item-0", queue.peek());

        queue.remove("Item-0");

        assertEquals("Item-3", queue.peek());

        queue.remove("Item-4");

        assertEquals("Item-3", queue.peek());

        queue.remove("Item-3");

        assertNull(queue.peek());
    }

    /**
     * @throws Exception If failed.
     */
    public void testReuseCache() throws Exception {
        CollectionConfiguration colCfg = collectionConfiguration();

        IgniteQueue queue1 = grid(0).queue("Queue1", 0, colCfg);

        IgniteQueue queue2 = grid(0).queue("Queue2", 0, colCfg);

        assertEquals(getQueueCache(queue1), getQueueCache(queue2));
    }

    /**
     * @throws Exception If failed.
     */
    public void testNotReuseCache() throws Exception {
        CollectionConfiguration colCfg1 = collectionConfiguration();

        CollectionConfiguration colCfg2 = collectionConfiguration();

        if (colCfg2.getAtomicityMode() == ATOMIC)
            colCfg2.setAtomicityMode(TRANSACTIONAL);
        else
            colCfg2.setAtomicityMode(ATOMIC);

        IgniteQueue queue1 = grid(0).queue("Queue1", 0, colCfg1);

        IgniteQueue queue2 = grid(0).queue("Queue2", 0, colCfg2);

        assertNotSame(getQueueCache(queue1), getQueueCache(queue2));
    }

    /**
     * @throws Exception If failed.
     */
    public void testFilterNode() throws Exception {
        CollectionConfiguration colCfg1 = collectionConfiguration();

        CollectionConfiguration colCfg2 = collectionConfiguration();

        colCfg2.setNodeFilter(CacheConfiguration.ALL_NODES);

        IgniteQueue queue1 = grid(0).queue("Queue1", 0, colCfg1);

        IgniteQueue queue2 = grid(0).queue("Queue2", 0, colCfg2);

        assertNotSame(getQueueCache(queue1), getQueueCache(queue2));

        colCfg1.setNodeFilter(CacheConfiguration.ALL_NODES);

        IgniteQueue queue3 = grid(0).queue("Queue3", 0, colCfg1);

        assertEquals(getQueueCache(queue2), getQueueCache(queue3));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSystemCache() throws Exception {
        CollectionConfiguration colCfg = collectionConfiguration();

        IgniteQueue queue = grid(0).queue("Queue1", 0, colCfg);

        final CacheConfiguration ccfg = getQueueCache(queue);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                grid(0).cache(ccfg.getName());
                return null;
            }
        }, IllegalStateException.class, "Failed to get cache because it is a system cache");

        assertNotNull(((IgniteKernal)grid(0)).internalCache(ccfg.getName()));
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityRun() throws Exception {
        final CollectionConfiguration colCfg = collectionConfiguration();

        colCfg.setCollocated(false);
        colCfg.setCacheMode(CacheMode.PARTITIONED);

        try (final IgniteQueue<Integer> queue1 = grid(0).queue("Queue1", 0, colCfg)) {
            GridTestUtils.assertThrows(
                log,
                new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        queue1.affinityRun(new IgniteRunnable() {
                            @Override public void run() {
                                // No-op.
                            }
                        });

                        return null;
                    }
                },
                IgniteException.class,
                "Failed to execute affinityRun() for non-collocated queue: " + queue1.name() +
                    ". This operation is supported only for collocated queues.");
        }

        colCfg.setCollocated(true);

        try (final IgniteQueue<Integer> queue2 = grid(0).queue("Queue2", 0, colCfg)) {
            queue2.add(100);

            queue2.affinityRun(new IgniteRunnable() {
                @IgniteInstanceResource
                private IgniteEx ignite;

                @Override public void run() {
                    assertTrue(ignite.cachex("datastructures_0").affinity().isPrimaryOrBackup(
                        ignite.cluster().localNode(), "Queue2"));

                    assertEquals(100, queue2.take().intValue());
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityCall() throws Exception {
        final CollectionConfiguration colCfg = collectionConfiguration();

        colCfg.setCollocated(false);
        colCfg.setCacheMode(CacheMode.PARTITIONED);

        try (final IgniteQueue<Integer> queue1 = grid(0).queue("Queue1", 0, colCfg)) {
            GridTestUtils.assertThrows(
                log,
                new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        queue1.affinityCall(new IgniteCallable<Object>() {
                            @Override public Object call() {
                                return null;
                            }
                        });

                        return null;
                    }
                },
                IgniteException.class,
                "Failed to execute affinityCall() for non-collocated queue: " + queue1.name() +
                    ". This operation is supported only for collocated queues.");
        }

        colCfg.setCollocated(true);

        try (final IgniteQueue<Integer> queue2 = grid(0).queue("Queue2", 0, colCfg)) {
            queue2.add(100);

            Integer res = queue2.affinityCall(new IgniteCallable<Integer>() {
                @IgniteInstanceResource
                private IgniteEx ignite;

                @Override public Integer call() {
                    assertTrue(ignite.cachex("datastructures_0").affinity().isPrimaryOrBackup(
                        ignite.cluster().localNode(), "Queue2"));

                    return queue2.take();
                }
            });

            assertEquals(100, res.intValue());
        }
    }

    /**
     * Implementation of ignite data structures internally uses special system caches, need make sure
     * that transaction on these system caches do not intersect with transactions started by user.
     *
     * @throws Exception If failed.
     */
    public void testIsolation() throws Exception {
        Ignite ignite = grid(0);

        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setName("myCache");
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(cfg);

        try {
            String queueName = UUID.randomUUID().toString();

            IgniteQueue<String> queue = grid(0).queue(queueName, 0, config(false));

            try (Transaction tx = ignite.transactions().txStart()) {
                cache.put(1, 1);

                for (int i = 0; i < QUEUE_CAPACITY; i++)
                    queue.put("Item-" + i);

                tx.rollback();
            }

            assertEquals(0, cache.size());

            assertEquals(QUEUE_CAPACITY, queue.size());

            queue.remove("Item-1");

            assertEquals(QUEUE_CAPACITY - 1, queue.size());

            assertEquals("Item-0", queue.peek());
            assertEquals("Item-0", queue.poll());
            assertEquals("Item-2", queue.poll());

            assertEquals(0, queue.size());

            queue.clear();

            assertTrue(queue.isEmpty());
        }
        finally {
            ignite.destroyCache(cfg.getName());
        }
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

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o instanceof SameHashItem) {
                SameHashItem i = (SameHashItem)o;

                return s.equals(i.s);
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SameHashItem.class, this);
        }
    }

}
