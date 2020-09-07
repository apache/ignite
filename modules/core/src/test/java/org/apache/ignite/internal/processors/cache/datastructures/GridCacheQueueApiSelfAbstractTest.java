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
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Queue basic tests.
 */
public abstract class GridCacheQueueApiSelfAbstractTest extends IgniteCollectionAbstractTest {
    /** To be used as a boolean system property. If true then run binary tests. */
    public static final String BINARY_QUEUE = "BINARY_QUEUE";

    /** Binary queue mode. */
    private static final boolean BINARY_QUEUE_MODE = IgniteSystemProperties.getBoolean(BINARY_QUEUE, false);

    /** */
    private static final int QUEUE_CAPACITY = 3;

    /** */
    private static final int THREAD_NUM = 2;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /**
     * Intialize Ignite queue
     */
    protected <T> IgniteQueue<T> initQueue(int idx, String name, int cap,
        @Nullable CollectionConfiguration cfg) {
        IgniteQueue<T> queue = grid(idx).queue(name, cap, cfg);

        if (queue != null && BINARY_QUEUE_MODE)
            return queue.withKeepBinary();

        return queue;
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPrepareQueue() throws Exception {
        // Random sequence names.
        String queueName1 = UUID.randomUUID().toString();
        String queueName2 = UUID.randomUUID().toString();

        CollectionConfiguration colCfg = config(false);

        IgniteQueue queue1 = initQueue(0, queueName1, 0, colCfg);
        IgniteQueue queue2 = initQueue(0, queueName2, 0, colCfg);
        IgniteQueue queue3 = initQueue(0, queueName1, 0, colCfg);

        assertNotNull(queue1);
        assertNotNull(queue2);
        assertNotNull(queue3);
        assert queue1.equals(queue3);
        assert queue3.equals(queue1);
        assert !queue3.equals(queue2);

        queue1.close();
        queue2.close();
        queue3.close();

        assertNull(initQueue(0, queueName1, 0, null));
        assertNull(initQueue(0, queueName2, 0, null));
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAddUnbounded() throws Exception {
        // Random queue name.
        String queueName = UUID.randomUUID().toString();

        String val = UUID.randomUUID().toString();

        IgniteQueue<String> queue = initQueue(0, queueName, 0, config(false));

        assert queue.add(val);

        assert val.equals(queue.poll());
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAddDeleteUnbounded() throws Exception {
        // Random queue name.
        String queueName = UUID.randomUUID().toString();

        String val = UUID.randomUUID().toString();

        IgniteQueue<String> queue = initQueue(0, queueName, 0, config(false));

        assert queue.add(val);

        assert queue.remove(val);

        assert queue.isEmpty();
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @Test
    public <T> void testCollectionMethods() throws Exception {
        // Random queue name.
        String queueName = UUID.randomUUID().toString();

        IgniteQueue<T> queue = initQueue(0, queueName, 0, config(false));

        SameHashInstanceFactory<T> factory = new SameHashInstanceFactory<T>(grid(0));

        int retries = 100;

        // Initialize queue.
        for (int i = 0; i < retries; i++)
            queue.addAll(Arrays.asList(factory.instance(i), factory.instance(i)));

        // Get arrays from queue.
        assertEquals(retries * 2, queue.toArray().length);

        T[] arr2 = factory.array(retries * 3);

        Object[] arr3 = queue.toArray(arr2);

        assertEquals(arr2, arr3);
        assertEquals(arr3[0], factory.instance(0));

        // Check queue items.
        assertEquals(retries * 2, queue.size());

        assertTrue(queue.contains(factory.instance(14)));

        assertFalse(queue.contains(factory.instance(144)));

        Collection<T> col1 = Arrays.asList(factory.instance(14), factory.instance(14), factory.instance(18));

        assertTrue(queue.containsAll(col1));

        Collection<T> col2 = Arrays.asList(factory.instance(245), factory.instance(14), factory.instance(18));

        assertFalse(queue.containsAll(col2));

        // Try to remove item.
        assertTrue(queue.remove(factory.instance(14)));

        assertEquals((retries * 2) - 1, queue.size());

        assertTrue(queue.contains(factory.instance(14)));

        assertTrue(queue.remove(factory.instance(14)));

        assertEquals((retries - 1) * 2, queue.size());

        assertFalse(queue.remove(factory.instance(14)));

        // Try to remove some items.
        assertTrue(queue.contains(factory.instance(33)));

        assertTrue(queue.removeAll(Arrays.asList(factory.instance(15), factory.instance(14), factory.instance(33),
            factory.instance(1))));

        assertFalse(queue.contains(factory.instance(33)));

        // Try to retain all items.
        assertTrue(queue.retainAll(Arrays.asList(factory.instance(15), factory.instance(14), factory.instance(33),
            factory.instance(1))));

        assertFalse(queue.contains(factory.instance(2)));

        assert queue.isEmpty();
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAddPollUnbounded() throws Exception {
        // Random queue name.
        String queueName = UUID.randomUUID().toString();

        IgniteQueue<String> queue = initQueue(0, queueName, 0, config(false));

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
    @Test
    public void testAddPeekUnbounded() throws Exception {
        // Random queue name.
        String queueName = UUID.randomUUID().toString();

        IgniteQueue<String> queue = initQueue(0, queueName, 0, config(false));

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
    @Test
    public void testIterator() throws Exception {
        checkIterator(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIteratorCollocated() throws Exception {
        checkIterator(true);
    }

    /**
     * @param collocated Collocated flag.
     */
    private void checkIterator(boolean collocated) {
        // Random queue name.
        String queueName = UUID.randomUUID().toString();

        IgniteQueue<String> queue = initQueue(0, queueName, 0, config(collocated));

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
    @Test
    public void testPutGetUnbounded() throws Exception {
        // Random queue name.
        String queueName = UUID.randomUUID().toString();

        IgniteQueue<String> queue = initQueue(0, queueName, QUEUE_CAPACITY, config(false));

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
    @Test
    public void testPutGetMultithreadUnbounded() throws Exception {
        // Random queue name.
        String queueName = UUID.randomUUID().toString();

        final IgniteQueue<String> queue = initQueue(0, queueName, QUEUE_CAPACITY, config(false));

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
    @Test
    public void testPutGetMultithreadBounded() throws Exception {
        // Random queue name.
        String queueName = UUID.randomUUID().toString();

        final IgniteQueue<String> queue = initQueue(0, queueName, QUEUE_CAPACITY, config(false));

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
    @Test
    public void testQueueRemoveMultithreadBounded() throws Exception {
        // Random queue name.
        final String queueName = UUID.randomUUID().toString();

        final IgniteQueue<String> queue = initQueue(0, queueName, QUEUE_CAPACITY, config(false));

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
                    IgniteQueue<String> queue = initQueue(0, queueName, 0, null);

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
    @Test
    public void testPutRemoveUnbounded() throws Exception {
        // Random queue name.
        String queueName = UUID.randomUUID().toString();

        IgniteQueue<String> queue = initQueue(0, queueName, 0, config(false));

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
    @Test
    public void testPutRemoveMultiThreadedUnbounded() throws Exception {
        // Random queue name.
        String queueName = UUID.randomUUID().toString();

        final IgniteQueue<String> queue = initQueue(0, queueName, 0, config(false));

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
    @Test
    public void testPutRemovePeekPollUnbounded() throws Exception {
        // Random queue name.
        String queueName = UUID.randomUUID().toString();

        IgniteQueue<String> queue = initQueue(0, queueName, 0, config(false));

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
    @Test
    public void testRemovePeek() throws Exception {
        // Random queue name.
        String queueName = UUID.randomUUID().toString();

        IgniteQueue<String> queue = initQueue(0, queueName, 0, config(false));

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
    @Test
    public void testReuseCache() throws Exception {
        CollectionConfiguration colCfg = collectionConfiguration();

        IgniteQueue queue1 = initQueue(0, "Queue1", 0, colCfg);

        IgniteQueue queue2 = initQueue(0, "Queue2", 0, colCfg);

        assertEquals(getQueueCache(queue1), getQueueCache(queue2));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNotReuseCache() throws Exception {
        CollectionConfiguration colCfg1 = collectionConfiguration();

        CollectionConfiguration colCfg2 = collectionConfiguration();

        if (colCfg2.getAtomicityMode() == ATOMIC)
            colCfg2.setAtomicityMode(TRANSACTIONAL);
        else
            colCfg2.setAtomicityMode(ATOMIC);

        IgniteQueue queue1 = initQueue(0, "Queue1", 0, colCfg1);

        IgniteQueue queue2 = initQueue(0, "Queue2", 0, colCfg2);

        assertNotSame(getQueueCache(queue1), getQueueCache(queue2));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFilterNode() throws Exception {
        CollectionConfiguration colCfg1 = collectionConfiguration();

        CollectionConfiguration colCfg2 = collectionConfiguration();

        colCfg2.setNodeFilter(new IgnitePredicate<ClusterNode>() {
            @Override public boolean apply(ClusterNode node) {
                return true;
            }
        });

        initQueue(0, "Queue1", 0, colCfg1);

        try {
            initQueue(0, "Queue2", 0, colCfg2);

            fail("Exception was expected.");
        }
        catch (Exception ex) {
            // Expected
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSystemCache() throws Exception {
        CollectionConfiguration colCfg = collectionConfiguration();

        IgniteQueue queue = initQueue(0, "Queue1", 0, colCfg);

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
    @Test
    public void testAffinityRun() throws Exception {
        final CollectionConfiguration colCfg = collectionConfiguration();

        colCfg.setCollocated(false);
        colCfg.setGroupName("testGroup");
        colCfg.setCacheMode(CacheMode.PARTITIONED);

        try (final IgniteQueue<Integer> queue1 = initQueue(0, "Queue1", 0, colCfg)) {
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

        try (final IgniteQueue<Integer> queue2 = initQueue(0, "Queue2", 0, colCfg)) {
            queue2.add(100);

            queue2.affinityRun(new IgniteRunnable() {
                @IgniteInstanceResource
                private IgniteEx ignite;

                @Override public void run() {
                    assertTrue(ignite.cachex(cctx(queue2).cache().name()).affinity().isPrimaryOrBackup(
                        ignite.cluster().localNode(), "Queue2"));

                    assertEquals(100, queue2.take().intValue());
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAffinityCall() throws Exception {
        final CollectionConfiguration colCfg = collectionConfiguration();

        colCfg.setCollocated(false);
        colCfg.setGroupName("testGroup");
        colCfg.setCacheMode(CacheMode.PARTITIONED);

        try (final IgniteQueue<Integer> queue1 = initQueue(0, "Queue1", 0, colCfg)) {
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

        try (final IgniteQueue<Integer> queue2 = initQueue(0, "Queue2", 0, colCfg)) {
            queue2.add(100);

            Integer res = queue2.affinityCall(new IgniteCallable<Integer>() {
                @IgniteInstanceResource
                private IgniteEx ignite;

                @Override public Integer call() {
                    assertTrue(ignite.cachex(cctx(queue2).cache().name()).affinity().isPrimaryOrBackup(
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
    @Test
    public void testIsolation() throws Exception {
        Ignite ignite = grid(0);

        CacheConfiguration cfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cfg.setName("myCache");
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(cfg);

        try {
            String queueName = UUID.randomUUID().toString();

            IgniteQueue<String> queue = initQueue(0, queueName, 0, config(false));

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
     * Test that queues within the same group and compatible configurations are stored in the same cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheReuse() throws Exception {
        CollectionConfiguration colCfg = collectionConfiguration();

        colCfg.setAtomicityMode(ATOMIC);
        colCfg.setGroupName("grp1");

        IgniteQueue queue1 = initQueue(0, "queue1", 100, colCfg);
        IgniteQueue queue2 = initQueue(0, "queue2", 100, colCfg);

        assert cctx(queue1).cacheId() == cctx(queue2).cacheId();

        colCfg.setAtomicityMode(TRANSACTIONAL);

        IgniteQueue queue3 = initQueue(0, "queue3", 100, colCfg);
        IgniteQueue queue4 = initQueue(0, "queue4", 100, colCfg);

        assert cctx(queue3).cacheId() == cctx(queue4).cacheId();
        assert cctx(queue1).cacheId() != cctx(queue3).cacheId();
        assert cctx(queue1).groupId() == cctx(queue3).groupId();

        colCfg.setGroupName("gtp2");

        IgniteQueue queue5 = initQueue(0, "queue5", 100, colCfg);
        IgniteQueue queue6 = initQueue(0, "queue6", 100, colCfg);

        assert cctx(queue5).cacheId() == cctx(queue6).cacheId();
        assert cctx(queue1).groupId() != cctx(queue5).groupId();
    }

    /**
     * Tests that basic API works correctly when there are multiple structures in multiple groups.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleStructuresInDifferentGroups() throws Exception {
        CollectionConfiguration cfg1 = collectionConfiguration();
        CollectionConfiguration cfg2 = collectionConfiguration().setGroupName("grp2");

        IgniteQueue<String> queue1 = initQueue(0, "queue1", 100, cfg1);
        IgniteQueue<String> queue2 = initQueue(0, "queue2", 100, cfg1);
        IgniteQueue<String> queue3 = initQueue(0, "queue3", 100, cfg2);
        IgniteQueue<String> queue4 = initQueue(0, "queue4", 100, cfg2);

        assertTrue(queue1.offer("a"));
        assertTrue(queue2.offer("b"));
        assertTrue(queue3.offer("c"));
        assertTrue(queue4.offer("d"));

        assertEquals("a", queue1.peek());
        assertEquals("b", queue2.peek());
        assertEquals("c", queue3.peek());
        assertEquals("d", queue4.peek());

        assertTrue(queue1.add("A"));
        assertTrue(queue2.add("B"));
        assertTrue(queue3.add("C"));
        assertTrue(queue4.add("D"));

        assertEquals(2, queue1.size());
        assertEquals(2, queue2.size());
        assertEquals(2, queue3.size());
        assertEquals(2, queue4.size());

        assertEquals("a", queue1.poll());
        assertEquals("b", queue2.poll());
        assertEquals("c", queue3.poll());
        assertEquals("d", queue4.poll());

        assertEquals("A", queue1.peek());
        assertEquals("B", queue2.peek());
        assertEquals("C", queue3.peek());
        assertEquals("D", queue4.peek());

        assertEquals(1, queue1.size());
        assertEquals(1, queue2.size());
        assertEquals(1, queue3.size());
        assertEquals(1, queue4.size());

        queue2.close();
        queue4.close();

        assertTrue(queue2.removed());
        assertTrue(queue4.removed());

        assertFalse(queue1.removed());
        assertFalse(queue3.removed());

        assertNotNull(initQueue(0, "queue1", 100, null));
        assertNull(initQueue(0, "queue2", 100, null));

        queue1.close();
        queue3.close();
    }

    /**
     *  Test class with the same hash code.
     */
    protected static class SameHashItem implements Serializable {
        /** Data field */
        private final String s;

        /**
         * @param s Item data.
         */
        protected SameHashItem(String s) {
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

    /**
     * Class to generate {@link SameHashItem} or {@link BinaryObject} objects based on wheter the queue mode is
     * binary.
     */
    private static class SameHashInstanceFactory<T> {
        /** Ignite instance. */
        private Ignite ignite;

        /**
         * @param ignite Ignite instance.
         */
        SameHashInstanceFactory(Ignite ignite) {
            this.ignite = ignite;
        }

        /**
         * @param val value to be used for creating the instance.
         */
        public T instance(int val) {
            return BINARY_QUEUE_MODE
                ? (T)ignite.binary().toBinary(new SameHashItem(Integer.toString(val)))
                : (T)new SameHashItem(Integer.toString(val));
        }

        /**
         * @param size Size of array.
         */
        public T[] array(int size) {
            return BINARY_QUEUE_MODE
                ? (T[])new BinaryObject[size]
                : (T[])new SameHashItem[size];
        }
    }
}
