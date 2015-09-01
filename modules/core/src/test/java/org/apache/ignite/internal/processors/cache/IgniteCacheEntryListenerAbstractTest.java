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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListener;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.EventType;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ModifiedExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.continuous.GridContinuousProcessor;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static javax.cache.event.EventType.CREATED;
import static javax.cache.event.EventType.EXPIRED;
import static javax.cache.event.EventType.REMOVED;
import static javax.cache.event.EventType.UPDATED;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 *
 */
public abstract class IgniteCacheEntryListenerAbstractTest extends IgniteCacheAbstractTest {
    /** */
    private static volatile List<CacheEntryEvent<? extends Integer, ? extends Integer>> evts;

    /** */
    private static volatile CountDownLatch evtsLatch;

    /** */
    private static volatile CountDownLatch syncEvtLatch;

    /** */
    private Integer lastKey = 0;

    /** */
    private CacheEntryListenerConfiguration<Integer, Integer> lsnrCfg;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        if (lsnrCfg != null)
            cfg.addCacheEntryListenerConfiguration(lsnrCfg);

        cfg.setEagerTtl(eagerTtl());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        for (int i = 0; i < gridCount(); i++) {
            GridContinuousProcessor proc = grid(i).context().continuous();

            ConcurrentMap<?, ?> syncMsgFuts = GridTestUtils.getFieldValue(proc, "syncMsgFuts");

            assertEquals(0, syncMsgFuts.size());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testExceptionIgnored() throws Exception {
        CacheEntryListenerConfiguration<Integer, Integer> lsnrCfg = new MutableCacheEntryListenerConfiguration<>(
            new Factory<CacheEntryListener<Integer, Integer>>() {
                @Override public CacheEntryListener<Integer, Integer> create() {
                    return new ExceptionListener();
                }
            },
            null,
            false,
            false
        );

        IgniteCache<Integer, Integer> cache = jcache();

        cache.registerCacheEntryListener(lsnrCfg);

        try {
            for (Integer key : keys()) {
                log.info("Check listener exceptions are ignored [key=" + key + ']');

                cache.put(key, key);

                cache.remove(key);
            }
        }
        finally {
            cache.deregisterCacheEntryListener(lsnrCfg);
        }

        lsnrCfg = new MutableCacheEntryListenerConfiguration<>(
            new Factory<CacheEntryListener<Integer, Integer>>() {
                @Override public CacheEntryListener<Integer, Integer> create() {
                    return new CreateUpdateRemoveExpireListener();
                }
            },
            new Factory<CacheEntryEventSerializableFilter<? super Integer, ? super Integer>>() {
                @Override public CacheEntryEventSerializableFilter<? super Integer, ? super Integer> create() {
                    return new ExceptionFilter();
                }
            },
            false,
            false
        );

        cache.registerCacheEntryListener(lsnrCfg);

        try {
            for (Integer key : keys()) {
                log.info("Check filter exceptions are ignored [key=" + key + ']');

                cache.put(key, key);

                cache.remove(key);
            }
        }
        finally {
            cache.deregisterCacheEntryListener(lsnrCfg);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoOldValue() throws Exception {
        CacheEntryListenerConfiguration<Integer, Integer> lsnrCfg = new MutableCacheEntryListenerConfiguration<>(
            new Factory<CacheEntryListener<Integer, Integer>>() {
                @Override public CacheEntryListener<Integer, Integer> create() {
                    return new CreateUpdateRemoveExpireListener();
                }
            },
            null,
            false,
            true
        );

        IgniteCache<Integer, Integer> cache = jcache();

        try {
            for (Integer key : keys()) {
                log.info("Check create/update/remove/expire events, no old value [key=" + key + ']');

                cache.registerCacheEntryListener(lsnrCfg);

                checkEvents(cache, lsnrCfg, key, true, true, true, true, false);
            }
        }
        finally {
            cache.deregisterCacheEntryListener(lsnrCfg);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSynchronousEvents() throws Exception {
        final CacheEntryCreatedListener<Integer, Integer> lsnr = new CreateUpdateRemoveExpireListener() {
            @Override public void onRemoved(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                super.onRemoved(evts);

                awaitLatch();
            }

            @Override public void onCreated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                super.onCreated(evts);

                awaitLatch();
            }

            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                super.onUpdated(evts);

                awaitLatch();
            }

            private void awaitLatch() {
                try {
                    assertTrue(syncEvtLatch.await(5000, MILLISECONDS));
                }
                catch (InterruptedException e) {
                    fail("Unexpected exception: " + e);
                }
            }
        };

        CacheEntryListenerConfiguration<Integer, Integer> lsnrCfg = new MutableCacheEntryListenerConfiguration<>(
            new Factory<CacheEntryListener<Integer, Integer>>() {
                @Override public CacheEntryListener<Integer, Integer> create() {
                    return lsnr;
                }
            },
            null,
            true,
            true
        );

        IgniteCache<Integer, Integer> cache = jcache();

        cache.registerCacheEntryListener(lsnrCfg);

        try {
            for (Integer key : keys()) {
                log.info("Check synchronous create event [key=" + key + ']');

                syncEvent(key, 1, cache, 1);

                checkEvent(evts.iterator(), key, CREATED, 1, null);

                log.info("Check synchronous update event [key=" + key + ']');

                syncEvent(key, 2, cache, 1);

                checkEvent(evts.iterator(), key, UPDATED, 2, 1);

                log.info("Check synchronous remove event [key=" + key + ']');

                syncEvent(key, null, cache, 1);

                checkEvent(evts.iterator(), key, REMOVED, null, 2);

                log.info("Check synchronous expire event [key=" + key + ']');

                syncEvent(key,
                    3,
                    cache.withExpiryPolicy(new ModifiedExpiryPolicy(new Duration(MILLISECONDS, 1000))),
                    eagerTtl() ? 2 : 1);

                checkEvent(evts.iterator(), key, CREATED, 3, null);

                if (!eagerTtl()) {
                    U.sleep(1100);

                    assertNull(primaryCache(key, cache.getName()).get(key));

                    evtsLatch.await(5000, MILLISECONDS);

                    assertEquals(1, evts.size());
                }

                checkEvent(evts.iterator(), key, EXPIRED, null, 3);

                assertEquals(0, evts.size());
            }
        }
        finally {
            cache.deregisterCacheEntryListener(lsnrCfg);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSynchronousEventsListenerNodeFailed() throws Exception {
        if (cacheMode() != PARTITIONED)
            return;

        lsnrCfg = new MutableCacheEntryListenerConfiguration<>(
            new NoOpCreateUpdateListenerFactory(),
            null,
            true,
            true
        );

        final Ignite grid = startGrid(gridCount());

        try {
            awaitPartitionMapExchange();

            IgniteCache<Integer, Integer> cache = jcache(0);

            Map<Integer, Integer> vals = new HashMap<>();

            for (Integer key : nearKeys(grid.cache(null), 100, 1_000_000))
                vals.put(key, 1);

            final AtomicBoolean done = new AtomicBoolean();

            IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    U.sleep(500);

                    stopGrid(grid.name());

                    done.set(true);

                    return null;
                }
            });

            while (!done.get())
                cache.putAll(vals);

            fut.get();

            log.info("Update one more time.");

            cache.putAll(vals);
        }
        finally {
            stopGrid(gridCount());
        }
    }

    /**
     * @param key Key.
     * @param val Value.
     * @param cache Cache.
     * @param expEvts Expected events number.
     * @throws Exception If failed.
     */
    private void syncEvent(Integer key, Integer val, IgniteCache<Integer, Integer> cache, int expEvts)
        throws Exception {
        evts = Collections.synchronizedList(new ArrayList<CacheEntryEvent<? extends Integer, ? extends Integer>>());

        evtsLatch = new CountDownLatch(expEvts);

        syncEvtLatch = new CountDownLatch(1);

        final AtomicBoolean done = new AtomicBoolean();

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                assertFalse(done.get());

                U.sleep(500);

                assertFalse(done.get());

                syncEvtLatch.countDown();

                return null;
            }
        });

        if (val != null)
            cache.put(key, val);
        else
            cache.remove(key);

        done.set(true);

        fut.get();

        evtsLatch.await(5000, MILLISECONDS);

        assertEquals(expEvts, evts.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testEvents() throws Exception {
        IgniteCache<Integer, Integer> cache = jcache();

        Map<Integer, Integer> vals = new HashMap<>();

        for (int i = 0; i < 100; i++)
            vals.put(i + 2_000_000, i);

        cache.putAll(vals); // Put some data in cache to make sure events are not generated for existing entries.

        for (Integer key : keys()) {
            log.info("Check create event [key=" + key + ']');

            checkEvents(cache, new CreateListenerFactory(), key, true, false, false, false);

            log.info("Check update event [key=" + key + ']');

            checkEvents(cache, new UpdateListenerFactory(), key, false, true, false, false);

            log.info("Check remove event [key=" + key + ']');

            checkEvents(cache, new RemoveListenerFactory(), key, false, false, true, false);

            log.info("Check expire event [key=" + key + ']');

            checkEvents(cache, new ExpireListenerFactory(), key, false, false, false, true);

            log.info("Check create/update events [key=" + key + ']');

            checkEvents(cache, new CreateUpdateListenerFactory(), key, true, true, false, false);

            log.info("Check create/update/remove/expire events [key=" + key + ']');

            checkEvents(cache, new CreateUpdateRemoveExpireListenerFactory(), key, true, true, true, true);
        }

        CacheEntryListenerConfiguration<Integer, Integer> lsnrCfg = new MutableCacheEntryListenerConfiguration<>(
            new CreateUpdateRemoveExpireListenerFactory(),
            new TestFilterFactory(),
            true,
            false
        );

        cache.registerCacheEntryListener(lsnrCfg);

        log.info("Check filter.");

        try {
            checkFilter(cache, vals);
        }
        finally {
            cache.deregisterCacheEntryListener(lsnrCfg);
        }

        cache.putAll(vals);

        try {
            checkListenerOnStart(vals);
        }
        finally {
            cache.removeAll(vals.keySet());
        }
    }

    /**
     * @param vals Values in cache.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void checkListenerOnStart(Map<Integer, Integer> vals) throws Exception {
        lsnrCfg = new MutableCacheEntryListenerConfiguration<>(
            new CreateUpdateRemoveExpireListenerFactory(),
            null,
            true,
            false
        );

        Ignite grid = startGrid(gridCount());

        try {
            awaitPartitionMapExchange();

            IgniteCache<Integer, Integer> cache = grid.cache(null);

            Integer key = Integer.MAX_VALUE;

            log.info("Check create/update/remove events for listener in configuration [key=" + key + ']');

            checkEvents(cache, lsnrCfg, key, true, true, true, true, true);
        }
        finally {
            stopGrid(gridCount());
        }

        lsnrCfg = new MutableCacheEntryListenerConfiguration<>(
            new CreateUpdateRemoveExpireListenerFactory(),
            new TestFilterFactory(),
            true,
            false
        );

        grid = startGrid(gridCount());

        try {
            awaitPartitionMapExchange();

            IgniteCache<Integer, Integer> cache = grid.cache(null);

            log.info("Check filter for listener in configuration.");

            if (cacheMode() == LOCAL)
                cache.putAll(vals);

            checkFilter(cache, vals);
        }
        finally {
            stopGrid(gridCount());
        }
    }

    /**
     * @param cache Cache.
     * @param lsnrFactory Listener factory.
     * @param key Key.
     * @param create {@code True} if listens for create events.
     * @param update {@code True} if listens for update events.
     * @param rmv {@code True} if listens for remove events.
     * @param expire {@code True} if listens for expire events.
     * @throws Exception If failed.
     */
    private void checkEvents(
        final IgniteCache<Integer, Integer> cache,
        final Factory<CacheEntryListener<Integer, Integer>> lsnrFactory,
        Integer key,
        boolean create,
        boolean update,
        boolean rmv,
        boolean expire) throws Exception {
        CacheEntryListenerConfiguration<Integer, Integer> lsnrCfg = new MutableCacheEntryListenerConfiguration<>(
            lsnrFactory,
            null,
            true,
            false
        );

        cache.registerCacheEntryListener(lsnrCfg);

        try {
            checkEvents(cache, lsnrCfg, key, create, update, rmv, expire, true);
        }
        finally {
            cache.deregisterCacheEntryListener(lsnrCfg);
        }
    }

    /**
     * @param cache Cache.
     * @param vals Values in cache.
     * @throws Exception If failed.
     */
    private void checkFilter(final IgniteCache<Integer, Integer> cache, Map<Integer, Integer> vals) throws Exception {
        evts = Collections.synchronizedList(new ArrayList<CacheEntryEvent<? extends Integer, ? extends Integer>>());

        final int expEvts = (vals.size() / 2) * 4; // Remove, create, update and expire for half of modified entries.

        evtsLatch = new CountDownLatch(expEvts);

        cache.removeAll(vals.keySet());

        cache.putAll(vals);

        final Map<Integer, Integer> newVals = new HashMap<>();

        for (Integer key : vals.keySet())
            newVals.put(key, -1);

        cache.withExpiryPolicy(new ModifiedExpiryPolicy(new Duration(MILLISECONDS, 500))).putAll(newVals);

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (Integer key : newVals.keySet()) {
                    if (primaryCache(key, cache.getName()).get(key) != null)
                        return false;
                }

                return true;
            }
        }, 5000);

        evtsLatch.await(5000, MILLISECONDS);

        assertEquals(expEvts, evts.size());

        Set<Integer> rmvd = new HashSet<>();
        Set<Integer> created = new HashSet<>();
        Set<Integer> updated = new HashSet<>();
        Set<Integer> expired = new HashSet<>();

        for (CacheEntryEvent<? extends Integer, ? extends Integer> evt : evts) {
            assertTrue(evt.getKey() % 2 == 0);

            assertTrue(vals.keySet().contains(evt.getKey()));

            switch (evt.getEventType()) {
                case REMOVED:
                    assertNull(evt.getValue());

                    assertEquals(vals.get(evt.getKey()), evt.getOldValue());

                    assertTrue(rmvd.add(evt.getKey()));

                    break;

                case CREATED:
                    assertEquals(vals.get(evt.getKey()), evt.getValue());

                    assertNull(evt.getOldValue());

                    assertTrue(rmvd.contains(evt.getKey()));

                    assertTrue(created.add(evt.getKey()));

                    break;

                case UPDATED:
                    assertEquals(-1, (int)evt.getValue());

                    assertEquals(vals.get(evt.getKey()), evt.getOldValue());

                    assertTrue(rmvd.contains(evt.getKey()));

                    assertTrue(created.contains(evt.getKey()));

                    assertTrue(updated.add(evt.getKey()));

                    break;

                case EXPIRED:
                    assertNull(evt.getValue());

                    assertEquals(-1, (int)evt.getOldValue());

                    assertTrue(rmvd.contains(evt.getKey()));

                    assertTrue(created.contains(evt.getKey()));

                    assertTrue(updated.contains(evt.getKey()));

                    assertTrue(expired.add(evt.getKey()));

                    break;

                default:
                    fail("Unexpected type: " + evt.getEventType());
            }
        }

        assertEquals(vals.size() / 2, rmvd.size());
        assertEquals(vals.size() / 2, created.size());
        assertEquals(vals.size() / 2, updated.size());
        assertEquals(vals.size() / 2, expired.size());
    }

    /**
     * @param cache Cache.
     * @param lsnrCfg Listener configuration.
     * @param key Key.
     * @param create {@code True} if listens for create events.
     * @param update {@code True} if listens for update events.
     * @param rmv {@code True} if listens for remove events.
     * @param expire {@code True} if listens for expire events.
     * @param oldVal {@code True} if old value should be provided for event.
     * @throws Exception If failed.
     */
    private void checkEvents(
        final IgniteCache<Integer, Integer> cache,
        final CacheEntryListenerConfiguration<Integer, Integer> lsnrCfg,
        Integer key,
        boolean create,
        boolean update,
        boolean rmv,
        boolean expire,
        boolean oldVal) throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.registerCacheEntryListener(lsnrCfg);

                return null;
            }
        }, IllegalArgumentException.class, null);

        final int UPDATES = 10;

        int expEvts = 0;

        if (create)
            expEvts += 4;

        if (update)
            expEvts += (UPDATES + 1);

        if (rmv)
            expEvts += 2;

        if (expire)
            expEvts += 2;

        evts = Collections.synchronizedList(new ArrayList<CacheEntryEvent<? extends Integer, ? extends Integer>>());

        evtsLatch = new CountDownLatch(expEvts);

        cache.put(key, 0);

        for (int i = 0; i < UPDATES; i++) {
            if (i % 2 == 0)
                cache.put(key, i + 1);
            else
                cache.invoke(key, new EntrySetValueProcessor(i + 1));
        }

        // Invoke processor does not update value, should not trigger event.
        assertEquals(String.valueOf(UPDATES), cache.invoke(key, new EntryToStringProcessor()));

        assertFalse(cache.putIfAbsent(key, -1));

        assertFalse(cache.remove(key, -1));

        assertTrue(cache.remove(key));

        IgniteCache<Integer, Integer> expirePlcCache =
            cache.withExpiryPolicy(new CreatedExpiryPolicy(new Duration(MILLISECONDS, 100)));

        expirePlcCache.put(key, 10);

        U.sleep(700);

        if (!eagerTtl())
            assertNull(primaryCache(key, cache.getName()).get(key)); // Provoke expire event if eager ttl is disabled.

        IgniteCache<Integer, Integer> cache1 = cache;

        if (gridCount() > 1)
            cache1 = jcache(1); // Do updates from another node.

        cache1.put(key, 1);

        cache1.put(key, 2);

        assertTrue(cache1.remove(key));

        IgniteCache<Integer, Integer> expirePlcCache1 =
            cache1.withExpiryPolicy(new CreatedExpiryPolicy(new Duration(MILLISECONDS, 100)));

        expirePlcCache1.put(key, 20);

        U.sleep(200);

        if (!eagerTtl())
            assertNull(primaryCache(key, cache.getName()).get(key)); // Provoke expire event if eager ttl is disabled.

        evtsLatch.await(5000, MILLISECONDS);

        assertEquals(expEvts, evts.size());

        Iterator<CacheEntryEvent<? extends Integer, ? extends Integer>> iter = evts.iterator();

        if (create)
            checkEvent(iter, key, CREATED, 0, null);

        if (update) {
            for (int i = 0; i < UPDATES; i++)
                checkEvent(iter, key, UPDATED, i + 1, oldVal ? i : null);
        }

        if (rmv)
            checkEvent(iter, key, REMOVED, null, oldVal ? UPDATES : null);

        if (create)
            checkEvent(iter, key, CREATED, 10, null);

        if (expire)
            checkEvent(iter, key, EXPIRED, null, oldVal ? 10 : null);

        if (create)
            checkEvent(iter, key, CREATED, 1, null);

        if (update)
            checkEvent(iter, key, UPDATED, 2, oldVal ? 1 : null);

        if (rmv)
            checkEvent(iter, key, REMOVED, null, oldVal ? 2 : null);

        if (create)
            checkEvent(iter, key, CREATED, 20, null);

        if (expire)
            checkEvent(iter, key, EXPIRED, null, oldVal ? 20 : null);

        assertEquals(0, evts.size());

        log.info("Remove listener.");

        cache.deregisterCacheEntryListener(lsnrCfg);

        cache.put(key, 1);

        cache.put(key, 2);

        assertTrue(cache.remove(key));

        U.sleep(500); // Sleep some time to ensure listener was really removed.

        assertEquals(0, evts.size());

        cache.registerCacheEntryListener(lsnrCfg);

        cache.deregisterCacheEntryListener(lsnrCfg);
    }

    /**
     * @param iter Received events iterator.
     * @param expKey Expected key.
     * @param expType Expected type.
     * @param expVal Expected value.
     * @param expOld Expected old value.
     */
    private void checkEvent(Iterator<CacheEntryEvent<? extends Integer, ? extends Integer>> iter,
        Integer expKey,
        EventType expType,
        @Nullable Integer expVal,
        @Nullable Integer expOld) {
        assertTrue(iter.hasNext());

        CacheEntryEvent<? extends Integer, ? extends Integer> evt = iter.next();

        iter.remove();

        assertTrue(evt.getSource() instanceof IgniteCacheProxy);

        assertEquals(expKey, evt.getKey());

        assertEquals(expType, evt.getEventType());

        assertEquals(expVal, evt.getValue());

        assertEquals(expOld, evt.getOldValue());

        if (expOld == null)
            assertFalse(evt.isOldValueAvailable());
        else
            assertTrue(evt.isOldValueAvailable());
    }

    /**
     * @return Test keys.
     * @throws Exception If failed.
     */
    protected Collection<Integer> keys() throws Exception {
        IgniteCache<Integer, Object> cache = jcache(0);

        ArrayList<Integer> keys = new ArrayList<>();

        keys.add(primaryKeys(cache, 1, lastKey).get(0));

        if (gridCount() > 1) {
            keys.add(backupKeys(cache, 1, lastKey).get(0));

            if (cache.getConfiguration(CacheConfiguration.class).getCacheMode() != REPLICATED)
                keys.add(nearKeys(cache, 1, lastKey).get(0));
        }

        lastKey = Collections.max(keys) + 1;

        return keys;
    }

    /**
     * @return Value for configuration property {@link CacheConfiguration#isEagerTtl()}.
     */
    protected boolean eagerTtl() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        evts = null;

        evtsLatch = null;
    }

    /**
     * @param evt Event.
     */
    private static void onEvent(CacheEntryEvent<? extends Integer, ? extends Integer> evt) {
        // System.out.println("Received event [evt=" + evt + ", thread=" + Thread.currentThread().getName() + ']');

        assertNotNull(evt);
        assertNotNull(evt.getSource());
        assertNotNull(evt.getEventType());
        assertNotNull(evt.getKey());

        evts.add(evt);

        evtsLatch.countDown();
    }

    /**
     *
     */
    private static class CreateUpdateRemoveExpireListenerFactory implements Factory<CacheEntryListener<Integer, Integer>> {
        @Override public CacheEntryListener<Integer, Integer> create() {
            return new CreateUpdateRemoveExpireListener();
        }
    }

    /**
     *
     */
    private static class NoOpCreateUpdateListenerFactory implements Factory<CacheEntryListener<Integer, Integer>> {
        @Override public CacheEntryListener<Integer, Integer> create() {
            return new NoOpCreateUpdateListener();
        }
    }

    /**
     *
     */
    private static class CreateUpdateListenerFactory implements Factory<CacheEntryListener<Integer, Integer>> {
        @Override public CacheEntryListener<Integer, Integer> create() {
            return new CreateUpdateListener();
        }
    }

    /**
     *
     */
    private static class CreateListenerFactory implements Factory<CacheEntryListener<Integer, Integer>> {
        @Override public CacheEntryListener<Integer, Integer> create() {
            return new CreateListener();
        }
    }

    /**
     *
     */
    private static class RemoveListenerFactory implements Factory<CacheEntryListener<Integer, Integer>> {
        @Override public CacheEntryListener<Integer, Integer> create() {
            return new RemoveListener();
        }
    }

    /**
     *
     */
    private static class UpdateListenerFactory implements Factory<CacheEntryListener<Integer, Integer>> {
        @Override public CacheEntryListener<Integer, Integer> create() {
            return new UpdateListener();
        }
    }

    /**
     *
     */
    private static class ExpireListenerFactory implements Factory<CacheEntryListener<Integer, Integer>> {
        @Override public CacheEntryListener<Integer, Integer> create() {
            return new ExpireListener();
        }
    }

    /**
     *
     */
    private static class TestFilterFactory implements Factory<CacheEntryEventSerializableFilter<Integer, Integer>> {
        /** {@inheritDoc} */
        @Override public CacheEntryEventSerializableFilter<Integer, Integer> create() {
            return new TestFilter();
        }
    }

    /**
     *
     */
    private static class CreateListener implements CacheEntryCreatedListener<Integer, Integer> {
        @Override public void onCreated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
            for (CacheEntryEvent<? extends Integer, ? extends Integer> evt : evts)
                onEvent(evt);
        }
    }

    /**
     *
     */
    private static class UpdateListener implements CacheEntryUpdatedListener<Integer, Integer> {
        @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
            for (CacheEntryEvent<? extends Integer, ? extends Integer> evt : evts)
                onEvent(evt);
        }
    }

    /**
     *
     */
    private static class RemoveListener implements CacheEntryRemovedListener<Integer, Integer> {
        @Override public void onRemoved(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
            for (CacheEntryEvent<? extends Integer, ? extends Integer> evt : evts)
                onEvent(evt);
        }
    }

    /**
     *
     */
    private static class ExpireListener implements CacheEntryExpiredListener<Integer, Integer> {
        @Override public void onExpired(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
            for (CacheEntryEvent<? extends Integer, ? extends Integer> evt : evts)
                onEvent(evt);
        }
    }

    /**
     *
     */
    private static class TestFilter implements CacheEntryEventSerializableFilter<Integer, Integer> {
        /** {@inheritDoc} */
        @Override public boolean evaluate(CacheEntryEvent<? extends Integer, ? extends Integer> evt) {
            assert evt != null;
            assert evt.getSource() != null : evt;
            assert evt.getEventType() != null : evt;
            assert evt.getKey() != null : evt;

            return evt.getKey() % 2 == 0;
        }
    }

    /**
     *
     */
    private static class CreateUpdateListener implements CacheEntryCreatedListener<Integer, Integer>,
        CacheEntryUpdatedListener<Integer, Integer> {
        /** {@inheritDoc} */
        @Override public void onCreated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
            for (CacheEntryEvent<? extends Integer, ? extends Integer> evt : evts)
                onEvent(evt);
        }

        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
            for (CacheEntryEvent<? extends Integer, ? extends Integer> evt : evts)
                onEvent(evt);
        }
    }

    /**
     *
     */
    private static class NoOpCreateUpdateListener implements CacheEntryCreatedListener<Integer, Integer>,
        CacheEntryUpdatedListener<Integer, Integer> {
        /** {@inheritDoc} */
        @Override public void onCreated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
            for (CacheEntryEvent<? extends Integer, ? extends Integer> evt : evts) {
                assertNotNull(evt);
                assertNotNull(evt.getSource());
                assertNotNull(evt.getEventType());
                assertNotNull(evt.getKey());
            }
        }

        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
            for (CacheEntryEvent<? extends Integer, ? extends Integer> evt : evts) {
                assertNotNull(evt);
                assertNotNull(evt.getSource());
                assertNotNull(evt.getEventType());
                assertNotNull(evt.getKey());
            }
        }
    }

    /**
     *
     */
    private static class CreateUpdateRemoveExpireListener extends CreateUpdateListener
        implements CacheEntryRemovedListener<Integer, Integer>, CacheEntryExpiredListener<Integer, Integer> {
        /** {@inheritDoc} */
        @Override public void onRemoved(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
            for (CacheEntryEvent<? extends Integer, ? extends Integer> evt : evts)
                onEvent(evt);
        }

        /** {@inheritDoc} */
        @Override public void onExpired(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
            for (CacheEntryEvent<? extends Integer, ? extends Integer> evt : evts)
                onEvent(evt);
        }
    }

    /**
     *
     */
    private static class ExceptionFilter implements CacheEntryEventSerializableFilter<Integer, Integer> {
        /** {@inheritDoc} */
        @Override public boolean evaluate(CacheEntryEvent<? extends Integer, ? extends Integer> evt) {
            throw new RuntimeException("Test filter error.");
        }
    }

    /**
     *
     */
    private static class ExceptionListener extends CreateUpdateListener
        implements CacheEntryRemovedListener<Integer, Integer>, CacheEntryExpiredListener<Integer, Integer> {
        /** {@inheritDoc} */
        @Override public void onCreated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
            error();
        }

        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
            error();
        }

        /** {@inheritDoc} */
        @Override public void onExpired(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
            error();
        }

        /** {@inheritDoc} */
        @Override public void onRemoved(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
            error();
        }

        /**
         * Throws exception.
         */
        private void error() {
            throw new RuntimeException("Test listener error.");
        }
    }

    /**
     *
     */
    protected static class EntryToStringProcessor implements EntryProcessor<Integer, Integer, String> {
        /** {@inheritDoc} */
        @Override public String process(MutableEntry<Integer, Integer> e, Object... arguments)
            throws EntryProcessorException {
            return String.valueOf(e.getValue());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(EntryToStringProcessor.class, this);
        }
    }

    /**
     *
     */
    protected static class EntrySetValueProcessor implements EntryProcessor<Integer, Integer, String> {
        /** */
        private Integer val;

        /**
         * @param val Value to set.
         */
        public EntrySetValueProcessor(Integer val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public String process(MutableEntry<Integer, Integer> e, Object... arguments)
            throws EntryProcessorException {
            e.setValue(val);

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(EntrySetValueProcessor.class, this);
        }
    }

}