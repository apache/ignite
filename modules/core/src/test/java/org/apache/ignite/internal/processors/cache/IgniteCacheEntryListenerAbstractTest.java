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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
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
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListener;
import javax.cache.event.CacheEntryListenerException;
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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.continuous.GridContinuousProcessor;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.eventstorage.memory.MemoryEventStorageSpi;
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
    private static volatile List<CacheEntryEvent<?, ?>> evts;

    /** */
    private static volatile CountDownLatch evtsLatch;

    /** */
    private static volatile CountDownLatch syncEvtLatch;

    /** */
    private Integer lastKey = 0;

    /** */
    private CacheEntryListenerConfiguration<Object, Object> lsnrCfg;

    /** */
    private boolean useObjects;

    /** */
    private static AtomicBoolean serialized = new AtomicBoolean(false);

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(igniteInstanceName);

        if (lsnrCfg != null)
            cfg.addCacheEntryListenerConfiguration(lsnrCfg);

        cfg.setEagerTtl(eagerTtl());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        MemoryEventStorageSpi eventSpi = new MemoryEventStorageSpi();
        eventSpi.setExpireCount(50);

        cfg.setEventStorageSpi(eventSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        for (int i = 0; i < gridCount(); i++) {
            GridContinuousProcessor proc = grid(i).context().continuous();

            final ConcurrentMap<?, ?> syncMsgFuts = GridTestUtils.getFieldValue(proc, "syncMsgFuts");

            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return syncMsgFuts.isEmpty();
                }
            }, 5000);

            assertEquals(0, syncMsgFuts.size());
        }

        serialized.set(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testExceptionIgnored() throws Exception {
        CacheEntryListenerConfiguration<Object, Object> lsnrCfg = new MutableCacheEntryListenerConfiguration<>(
            new Factory<CacheEntryListener<Object, Object>>() {
                @Override public CacheEntryListener<Object, Object> create() {
                    return new ExceptionListener();
                }
            },
            null,
            false,
            false
        );

        IgniteCache<Object, Object> cache = jcache();

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
            new Factory<CacheEntryListener<Object, Object>>() {
                @Override public CacheEntryListener<Object, Object> create() {
                    return new CreateUpdateRemoveExpireListener();
                }
            },
            new ExceptionFilterFactory(),
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
        CacheEntryListenerConfiguration<Object, Object> lsnrCfg = new MutableCacheEntryListenerConfiguration<>(
            new Factory<CacheEntryListener<Object, Object>>() {
                @Override public CacheEntryListener<Object, Object> create() {
                    return new CreateUpdateRemoveExpireListener();
                }
            },
            null,
            false,
            true
        );

        IgniteCache<Object, Object> cache = jcache();

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
    public void testSynchronousEventsObjectKeyValue() throws Exception {
        useObjects = true;

        testSynchronousEvents();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSynchronousEvents() throws Exception {
        final CacheEntryCreatedListener<Object, Object> lsnr = new CreateUpdateRemoveExpireListener() {
            @Override public void onRemoved(Iterable<CacheEntryEvent<?, ?>> evts) {
                super.onRemoved(evts);

                awaitLatch();
            }

            @Override public void onCreated(Iterable<CacheEntryEvent<?, ?>> evts) {
                super.onCreated(evts);

                awaitLatch();
            }

            @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> evts) {
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

        CacheEntryListenerConfiguration<Object, Object> lsnrCfg = new MutableCacheEntryListenerConfiguration<>(
            new Factory<CacheEntryListener<Object, Object>>() {
                @Override public CacheEntryListener<Object, Object> create() {
                    return lsnr;
                }
            },
            null,
            true,
            true
        );

        IgniteCache<Object, Object> cache = jcache();

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

                checkEvent(evts.iterator(), key, REMOVED, 2, 2);

                log.info("Check synchronous expire event [key=" + key + ']');

                syncEvent(key,
                    3,
                    cache.withExpiryPolicy(new ModifiedExpiryPolicy(new Duration(MILLISECONDS, 1000))),
                    eagerTtl() ? 2 : 1);

                checkEvent(evts.iterator(), key, CREATED, 3, null);

                if (!eagerTtl()) {
                    U.sleep(1100);

                    assertNull(primaryCache(key(key), cache.getName()).get(key(key)));

                    evtsLatch.await(5000, MILLISECONDS);

                    assertEquals(1, evts.size());
                }

                checkEvent(evts.iterator(), key, EXPIRED, 3, 3);

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

            for (Integer key : nearKeys(grid.cache(DEFAULT_CACHE_NAME), 100, 1_000_000))
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
     * @throws Exception If failed.
     */
    public void testConcurrentRegisterDeregister() throws Exception {
        final int THREADS = 10;

        final CyclicBarrier barrier = new CyclicBarrier(THREADS);

        final IgniteCache<Object, Object> cache = jcache(0);

        GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                CacheEntryListenerConfiguration<Object, Object> cfg = new MutableCacheEntryListenerConfiguration<>(
                    new Factory<CacheEntryListener<Object, Object>>() {
                        @Override public CacheEntryListener<Object, Object> create() {
                            return new CreateUpdateRemoveExpireListener();
                        }
                    },
                    null,
                    true,
                    false
                );

                barrier.await();

                for (int i = 0; i < 100; i++) {
                    cache.registerCacheEntryListener(cfg);

                    cache.deregisterCacheEntryListener(cfg);
                }

                return null;
            }
        }, THREADS, "register-thread").get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSerialization() throws Exception {
        if (cacheMode() == LOCAL)
            return;

        AtomicBoolean serialized = new AtomicBoolean();

        NonSerializableListener lsnr = new NonSerializableListener(serialized);

        jcache(0).registerCacheEntryListener(new MutableCacheEntryListenerConfiguration<>(
            FactoryBuilder.factoryOf(lsnr),
            new SerializableFactory(),
            true,
            false
        ));

        try {
            startGrid(gridCount());

            jcache(0).put(1, 1);
        }
        finally {
            stopGrid(gridCount());
        }

        jcache(0).put(2, 2);

        assertFalse(IgniteCacheEntryListenerAbstractTest.serialized.get());
        assertFalse(serialized.get());
    }

    /**
     * @param key Key.
     * @param val Value.
     * @param cache Cache.
     * @param expEvts Expected events number.
     * @throws Exception If failed.
     */
    private void syncEvent(
        Integer key,
        Integer val,
        IgniteCache<Object, Object> cache,
        int expEvts)
        throws Exception {
        evts = Collections.synchronizedList(new ArrayList<CacheEntryEvent<?, ?>>());

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
            cache.put(key(key), value(val));
        else
            cache.remove(key(key));

        done.set(true);

        fut.get();

        evtsLatch.await(5000, MILLISECONDS);

        assertEquals(expEvts, evts.size());
    }

    /**
     * @param key Integer key.
     * @return Key instance.
     */
    private Object key(Integer key) {
        assert key != null;

        return useObjects ? new ListenerTestKey(key) : key;
    }

    /**
     * @param val Integer value.
     * @return Value instance.
     */
    private Object value(Integer val) {
        if (val == null)
            return null;

        return useObjects ? new ListenerTestValue(val) : val;
    }

    /**
     * @throws Exception If failed.
     */
    public void testEventsObjectKeyValue() throws Exception {
        useObjects = true;

        testEvents();
    }

    /**
     * @throws Exception If failed.
     */
    public void testEvents() throws Exception {
        IgniteCache<Object, Object> cache = jcache();

        Map<Object, Object> vals = new HashMap<>();

        for (int i = 0; i < 100; i++)
            vals.put(key(i + 2_000_000), value(i));

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

        CacheEntryListenerConfiguration<Object, Object> lsnrCfg = new MutableCacheEntryListenerConfiguration<>(
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
    private void checkListenerOnStart(Map<Object, Object> vals) throws Exception {
        lsnrCfg = new MutableCacheEntryListenerConfiguration<>(
            new CreateUpdateRemoveExpireListenerFactory(),
            null,
            true,
            false
        );

        Ignite grid = startGrid(gridCount());

        try {
            awaitPartitionMapExchange();

            IgniteCache<Object, Object> cache = grid.cache(DEFAULT_CACHE_NAME);

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

            IgniteCache<Object, Object> cache = grid.cache(DEFAULT_CACHE_NAME);

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
        final IgniteCache<Object, Object> cache,
        final Factory<CacheEntryListener<Object, Object>> lsnrFactory,
        Integer key,
        boolean create,
        boolean update,
        boolean rmv,
        boolean expire) throws Exception {
        CacheEntryListenerConfiguration<Object, Object> lsnrCfg = new MutableCacheEntryListenerConfiguration<>(
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
    private void checkFilter(final IgniteCache<Object, Object> cache, Map<Object, Object> vals) throws Exception {
        evts = Collections.synchronizedList(new ArrayList<CacheEntryEvent<?, ?>>());

        final int expEvts = (vals.size() / 2) * 4; // Remove, create, update and expire for half of modified entries.

        evtsLatch = new CountDownLatch(expEvts);

        cache.removeAll(vals.keySet());

        cache.putAll(vals);

        final Map<Object, Object> newVals = new HashMap<>();

        for (Object key : vals.keySet())
            newVals.put(key, value(-1));

        cache.withExpiryPolicy(new ModifiedExpiryPolicy(new Duration(MILLISECONDS, 500))).putAll(newVals);

        U.sleep(1000);

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (Object key : newVals.keySet()) {
                    if (primaryCache(key, cache.getName()).get(key) != null)
                        return false;
                }

                return true;
            }
        }, 5000);

        evtsLatch.await(5000, MILLISECONDS);

        assertEquals(expEvts, evts.size());

        Set<Object> rmvd = new HashSet<>();
        Set<Object> created = new HashSet<>();
        Set<Object> updated = new HashSet<>();
        Set<Object> expired = new HashSet<>();

        for (CacheEntryEvent<?, ?> evt : evts) {
            Integer key;

            if (useObjects)
                key = ((ListenerTestKey)evt.getKey()).key;
            else
                key = (Integer)evt.getKey();

            assertTrue(key % 2 == 0);

            assertTrue(vals.keySet().contains(evt.getKey()));

            switch (evt.getEventType()) {
                case REMOVED:
                    assertEquals(evt.getOldValue(), evt.getValue());

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
                    assertEquals(value(-1), evt.getValue());

                    assertEquals(vals.get(evt.getKey()), evt.getOldValue());

                    assertTrue(rmvd.contains(evt.getKey()));

                    assertTrue(created.contains(evt.getKey()));

                    assertTrue(updated.add(evt.getKey()));

                    break;

                case EXPIRED:
                    assertEquals(evt.getOldValue(), evt.getValue());

                    assertEquals(value(-1), evt.getOldValue());

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
        final IgniteCache<Object, Object> cache,
        final CacheEntryListenerConfiguration<Object, Object> lsnrCfg,
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

        evts = Collections.synchronizedList(new ArrayList<CacheEntryEvent<?, ?>>());

        evtsLatch = new CountDownLatch(expEvts);

        cache.put(key(key), value(0));

        for (int i = 0; i < UPDATES; i++) {
            if (i % 2 == 0)
                cache.put(key(key), value(i + 1));
            else
                cache.invoke(key(key), new EntrySetValueProcessor(value(i + 1)));
        }

        // Invoke processor does not update value, should not trigger event.
        assertEquals(String.valueOf(UPDATES), cache.invoke(key(key), new EntryToStringProcessor()));

        assertFalse(cache.putIfAbsent(key(key), value(-1)));

        assertFalse(cache.remove(key(key), value(-1)));

        assertTrue(cache.remove(key(key)));

        IgniteCache<Object, Object> expirePlcCache =
            cache.withExpiryPolicy(new CreatedExpiryPolicy(new Duration(MILLISECONDS, 100)));

        expirePlcCache.put(key(key), value(10));

        U.sleep(700);

        if (!eagerTtl())
            assertNull(primaryCache(key(key), cache.getName()).get(key(key))); // Provoke expire event if eager ttl is disabled.

        IgniteCache<Object, Object> cache1 = cache;

        if (gridCount() > 1)
            cache1 = jcache(1); // Do updates from another node.

        cache1.put(key(key), value(1));

        cache1.put(key(key), value(2));

        assertTrue(cache1.remove(key(key)));

        IgniteCache<Object, Object> expirePlcCache1 =
            cache1.withExpiryPolicy(new CreatedExpiryPolicy(new Duration(MILLISECONDS, 100)));

        expirePlcCache1.put(key(key), value(20));

        U.sleep(200);

        if (!eagerTtl())
            assertNull(primaryCache(key(key), cache.getName()).get(key(key))); // Provoke expire event if eager ttl is disabled.

        evtsLatch.await(5000, MILLISECONDS);

        assertEquals(expEvts, evts.size());

        Iterator<CacheEntryEvent<?, ?>> iter = evts.iterator();

        if (create)
            checkEvent(iter, key, CREATED, 0, null);

        if (update) {
            for (int i = 0; i < UPDATES; i++)
                checkEvent(iter, key, UPDATED, i + 1, oldVal ? i : null);
        }

        if (rmv)
            checkEvent(iter, key, REMOVED, oldVal ? UPDATES : null, oldVal ? UPDATES : null);

        if (create)
            checkEvent(iter, key, CREATED, 10, null);

        if (expire)
            checkEvent(iter, key, EXPIRED, oldVal ? 10 : null, oldVal ? 10 : null);

        if (create)
            checkEvent(iter, key, CREATED, 1, null);

        if (update)
            checkEvent(iter, key, UPDATED, 2, oldVal ? 1 : null);

        if (rmv)
            checkEvent(iter, key, REMOVED, oldVal ? 2 : null, oldVal ? 2 : null);

        if (create)
            checkEvent(iter, key, CREATED, 20, null);

        if (expire)
            checkEvent(iter, key, EXPIRED, oldVal ? 20 : null, oldVal ? 20 : null);

        assertEquals(0, evts.size());

        log.info("Remove listener.");

        cache.deregisterCacheEntryListener(lsnrCfg);

        cache.put(key(key), value(1));

        cache.put(key(key), value(2));

        assertTrue(cache.remove(key(key)));

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
    private void checkEvent(Iterator<CacheEntryEvent<?, ?>> iter,
        Integer expKey,
        EventType expType,
        @Nullable Integer expVal,
        @Nullable Integer expOld) {
        assertTrue(iter.hasNext());

        CacheEntryEvent<?, ?> evt = iter.next();

        iter.remove();

        assertTrue(evt.getSource() instanceof IgniteCacheProxy);

        assertEquals(key(expKey), evt.getKey());

        assertEquals(expType, evt.getEventType());

        assertEquals(value(expVal), evt.getValue());

        assertEquals(value(expOld), evt.getOldValue());

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
    private static void onEvent(CacheEntryEvent<?, ?> evt) {
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
    private static class CreateUpdateRemoveExpireListenerFactory implements Factory<CacheEntryListener<Object, Object>> {
        /** {@inheritDoc} */
        @Override public CacheEntryListener<Object, Object> create() {
            return new CreateUpdateRemoveExpireListener();
        }
    }

    /**
     *
     */
    private static class NoOpCreateUpdateListenerFactory implements Factory<CacheEntryListener<Object, Object>> {
        /** {@inheritDoc} */
        @Override public CacheEntryListener<Object, Object> create() {
            return new NoOpCreateUpdateListener();
        }
    }

    /**
     *
     */
    private static class CreateUpdateListenerFactory implements Factory<CacheEntryListener<Object, Object>> {
        /** {@inheritDoc} */
        @Override public CacheEntryListener<Object, Object> create() {
            return new CreateUpdateListener();
        }
    }

    /**
     *
     */
    private static class CreateListenerFactory implements Factory<CacheEntryListener<Object, Object>> {
        /** {@inheritDoc} */
        @Override public CacheEntryListener<Object, Object> create() {
            return new CreateListener();
        }
    }

    /**
     *
     */
    private static class RemoveListenerFactory implements Factory<CacheEntryListener<Object, Object>> {
        /** {@inheritDoc} */
        @Override public CacheEntryListener<Object, Object> create() {
            return new RemoveListener();
        }
    }

    /**
     *
     */
    private static class UpdateListenerFactory implements Factory<CacheEntryListener<Object, Object>> {
        /** {@inheritDoc} */
        @Override public CacheEntryListener<Object, Object> create() {
            return new UpdateListener();
        }
    }

    /**
     *
     */
    private static class ExpireListenerFactory implements Factory<CacheEntryListener<Object, Object>> {
        /** {@inheritDoc} */
        @Override public CacheEntryListener<Object, Object> create() {
            return new ExpireListener();
        }
    }

    /**
     *
     */
    private static class TestFilterFactory implements Factory<CacheEntryEventFilter<Object, Object>> {
        /** {@inheritDoc} */
        @Override public CacheEntryEventFilter<Object, Object> create() {
            return new TestFilter();
        }
    }

    /**
     *
     */
    private static class CreateListener implements CacheEntryCreatedListener<Object, Object> {
        /** {@inheritDoc} */
        @Override public void onCreated(Iterable<CacheEntryEvent<?, ?>> evts) {
            for (CacheEntryEvent<?, ?> evt : evts)
                onEvent(evt);
        }
    }

    /**
     *
     */
    private static class UpdateListener implements CacheEntryUpdatedListener<Object, Object> {
        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> evts) {
            for (CacheEntryEvent<?, ?> evt : evts)
                onEvent(evt);
        }
    }

    /**
     *
     */
    private static class RemoveListener implements CacheEntryRemovedListener<Object, Object> {
        /** {@inheritDoc} */
        @Override public void onRemoved(Iterable<CacheEntryEvent<?, ?>> evts) {
            for (CacheEntryEvent<?, ?> evt : evts)
                onEvent(evt);
        }
    }

    /**
     *
     */
    private static class ExpireListener implements CacheEntryExpiredListener<Object, Object> {
        /** {@inheritDoc} */
        @Override public void onExpired(Iterable<CacheEntryEvent<?, ?>> evts) {
            for (CacheEntryEvent<?, ?> evt : evts)
                onEvent(evt);
        }
    }

    /**
     *
     */
    private static class TestFilter implements CacheEntryEventFilter<Object, Object>, Externalizable {
        /** {@inheritDoc} */
        @Override public boolean evaluate(CacheEntryEvent<?, ?> evt) {
            assert evt != null;
            assert evt.getSource() != null : evt;
            assert evt.getEventType() != null : evt;
            assert evt.getKey() != null : evt;

            Integer key;

            if (evt.getKey() instanceof ListenerTestKey)
                key = ((ListenerTestKey)evt.getKey()).key;
            else
                key = (Integer)evt.getKey();

            return key % 2 == 0;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            throw new UnsupportedOperationException("Filter must not be marshaled.");
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            throw new UnsupportedOperationException("Filter must not be unmarshaled.");
        }
    }

    /**
     *
     */
    private static class CreateUpdateListener implements CacheEntryCreatedListener<Object, Object>,
        CacheEntryUpdatedListener<Object, Object> {
        /** {@inheritDoc} */
        @Override public void onCreated(Iterable<CacheEntryEvent<?, ?>> evts) {
            for (CacheEntryEvent<?, ?> evt : evts)
                onEvent(evt);
        }

        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> evts) {
            for (CacheEntryEvent<?, ?> evt : evts)
                onEvent(evt);
        }
    }

    /**
     *
     */
    private static class NoOpCreateUpdateListener implements CacheEntryCreatedListener<Object, Object>,
        CacheEntryUpdatedListener<Object, Object> {
        /** {@inheritDoc} */
        @Override public void onCreated(Iterable<CacheEntryEvent<?, ?>> evts) {
            for (CacheEntryEvent<?, ?> evt : evts) {
                assertNotNull(evt);
                assertNotNull(evt.getSource());
                assertNotNull(evt.getEventType());
                assertNotNull(evt.getKey());
            }
        }

        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> evts) {
            for (CacheEntryEvent<?, ?> evt : evts) {
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
        implements CacheEntryRemovedListener<Object, Object>, CacheEntryExpiredListener<Object, Object> {
        /** {@inheritDoc} */
        @Override public void onRemoved(Iterable<CacheEntryEvent<?, ?>> evts) {
            for (CacheEntryEvent<?, ?> evt : evts)
                onEvent(evt);
        }

        /** {@inheritDoc} */
        @Override public void onExpired(Iterable<CacheEntryEvent<?, ?>> evts) {
            for (CacheEntryEvent<?, ?> evt : evts)
                onEvent(evt);
        }
    }

    /**
     *
     */
    private static class ExceptionFilter implements CacheEntryEventSerializableFilter<Object, Object> {
        /** {@inheritDoc} */
        @Override public boolean evaluate(CacheEntryEvent<?, ?> evt) {
            throw new RuntimeException("Test filter error.");
        }
    }

    /**
     *
     */
    private static class ExceptionListener extends CreateUpdateListener
        implements CacheEntryRemovedListener<Object, Object>, CacheEntryExpiredListener<Object, Object> {
        /** {@inheritDoc} */
        @Override public void onCreated(Iterable<CacheEntryEvent<?, ?>> evts) {
            error();
        }

        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> evts) {
            error();
        }

        /** {@inheritDoc} */
        @Override public void onExpired(Iterable<CacheEntryEvent<?, ?>> evts) {
            error();
        }

        /** {@inheritDoc} */
        @Override public void onRemoved(Iterable<CacheEntryEvent<?, ?>> evts) {
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
    protected static class EntryToStringProcessor implements EntryProcessor<Object, Object, String> {
        /** {@inheritDoc} */
        @Override public String process(MutableEntry<Object, Object> e, Object... args) {
            if (e.getValue() instanceof ListenerTestValue)
                return String.valueOf(((ListenerTestValue)e.getValue()).val1);

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
    protected static class EntrySetValueProcessor implements EntryProcessor<Object, Object, String> {
        /** */
        private Object val;

        /**
         * @param val Value to set.
         */
        public EntrySetValueProcessor(Object val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public String process(MutableEntry<Object, Object> e, Object... args)
            throws EntryProcessorException {
            e.setValue(val);

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(EntrySetValueProcessor.class, this);
        }
    }

    /**
     *
     */
    public static class SerializableFactory implements Factory<NonSerializableFilter> {
        /** {@inheritDoc} */
        @Override public NonSerializableFilter create() {
            return new NonSerializableFilter();
        }
    }

    /**
     *
     */
    public static class NonSerializableFilter implements CacheEntryEventFilter<Object, Object>, Externalizable {
        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            serialized.set(true);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            serialized.set(true);
        }

        /** {@inheritDoc} */
        @Override public boolean evaluate(CacheEntryEvent<?, ?> event) throws CacheEntryListenerException {
            return true;
        }
    }

    /**
     */
    public static class NonSerializableListener implements CacheEntryCreatedListener<Object, Object>, Externalizable {
        /** */
        private final AtomicBoolean serialized;

        /**
         * @param serialized Serialized flag.
         */
        public NonSerializableListener(AtomicBoolean serialized) {
            this.serialized = serialized;
        }

        /** {@inheritDoc} */
        @Override public void onCreated(Iterable<CacheEntryEvent<?, ?>> evts)
            throws CacheEntryListenerException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            serialized.set(true);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            // No-op.
        }
    }

    /**
     *
     */
    static class ListenerTestKey implements Serializable {
        /** */
        private final Integer key;

        /**
         * @param key Key.
         */
        public ListenerTestKey(Integer key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            ListenerTestKey that = (ListenerTestKey)o;

            return key.equals(that.key);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ListenerTestKey.class, this);
        }
    }

    /**
     *
     */
    static class ListenerTestValue implements Serializable {
        /** */
        private final Integer val1;

        /** */
        private final String val2;

        /**
         * @param val Value.
         */
        public ListenerTestValue(Integer val) {
            this.val1 = val;
            this.val2 = String.valueOf(val);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            ListenerTestValue that = (ListenerTestValue) o;

            return val1.equals(that.val1) && val2.equals(that.val2);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = val1.hashCode();

            res = 31 * res + val2.hashCode();

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ListenerTestValue.class, this);
        }
    }

    /**
     *
     */
    static class ExceptionFilterFactory implements Factory<CacheEntryEventSerializableFilter<Object, Object>> {
        /** {@inheritDoc} */
        @Override public CacheEntryEventSerializableFilter<Object, Object> create() {
            return new ExceptionFilter();
        }
    }
}
