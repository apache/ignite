/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.jetbrains.annotations.*;

import javax.cache.configuration.*;
import javax.cache.event.*;

import java.util.*;
import java.util.concurrent.*;

import static javax.cache.event.EventType.*;
import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 *
 */
public abstract class IgniteCacheEntryListenerAbstractTest extends IgniteCacheAbstractTest {
    /** */
    private static volatile List<CacheEntryEvent<? extends Integer, ? extends Integer>> evts;

    /** */
    private static volatile CountDownLatch evtsLatch;

    /** */
    private Integer lastKey = 0;

    /** */
    private CacheEntryListenerConfiguration lsnrCfg;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration cfg = super.cacheConfiguration(gridName);

        if (lsnrCfg != null)
            cfg.addCacheEntryListenerConfiguration(lsnrCfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testEvents() throws Exception {
        CacheEntryCreatedListener<Integer, Integer> createLsnr = new CacheEntryCreatedListener<Integer, Integer>() {
            @Override public void onCreated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                for (CacheEntryEvent<? extends Integer, ? extends Integer> evt : evts)
                    onEvent(evt);
            }
        };

        CacheEntryUpdatedListener<Integer, Integer> updateLsnr = new CacheEntryUpdatedListener<Integer, Integer>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                for (CacheEntryEvent<? extends Integer, ? extends Integer> evt : evts)
                    onEvent(evt);
            }
        };

        CacheEntryRemovedListener<Integer, Integer> rmvLsnr = new CacheEntryRemovedListener<Integer, Integer>() {
            @Override public void onRemoved(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                for (CacheEntryEvent<? extends Integer, ? extends Integer> evt : evts)
                    onEvent(evt);
            }
        };

        IgniteCache<Integer, Integer> cache = jcache();

        Map<Integer, Integer> vals = new HashMap<>();

        for (int i = 0; i < 100; i++)
            vals.put(i + 1_000_000, i);

        cache.putAll(vals); // Put some data in cache to make sure events are not generated for existing entries.

        for (Integer key : keys()) {
            log.info("Check create event [key=" + key + ']');

            checkEvents(cache, createLsnr, key, true, false, false);

            log.info("Check update event [key=" + key + ']');

            checkEvents(cache, updateLsnr, key, false, true, false);

            log.info("Check remove event [key=" + key + ']');

            checkEvents(cache, rmvLsnr, key, false, false, true);

            log.info("Check create/update events [key=" + key + ']');

            checkEvents(cache, new CreateUpdateListener(), key, true, true, false);

            log.info("Check create/update/remove events [key=" + key + ']');

            checkEvents(cache, new CreateUpdateRemoveListener(), key, true, true, true);
        }

        CacheEntryListenerConfiguration<Integer, Integer> lsnrCfg = new MutableCacheEntryListenerConfiguration<>(
            new Factory<CacheEntryListener<Integer, Integer>>() {
                @Override public CacheEntryListener<Integer, Integer> create() {
                    return new CreateUpdateRemoveListener();
                }
            },
            new TestFilterFactory(),
            true,
            false
        );

        cache.registerCacheEntryListener(lsnrCfg);

        log.info("Check filter.");

        checkFilter(cache, vals);

        cache.deregisterCacheEntryListener(lsnrCfg);

        cache.putAll(vals);

        checkListenerOnStart(vals);
    }

    /**
     * @param vals Values in cache.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void checkListenerOnStart(Map<Integer, Integer> vals) throws Exception {
        lsnrCfg = new MutableCacheEntryListenerConfiguration<>(
            new Factory<CacheEntryListener<Integer, Integer>>() {
                @Override public CacheEntryListener<Integer, Integer> create() {
                    return new CreateUpdateRemoveListener();
                }
            },
            null,
            true,
            false
        );

        Ignite grid = startGrid(gridCount());

        IgniteCache<Integer, Integer> cache = grid.jcache(null);

        Integer key = Integer.MAX_VALUE;

        log.info("Check create/update/remove events for listener in configuration [key=" + key + ']');

        checkEvents(cache, lsnrCfg, key, true, true, true);

        stopGrid(gridCount());

        lsnrCfg = new MutableCacheEntryListenerConfiguration<>(
            new Factory<CacheEntryListener<Integer, Integer>>() {
                @Override public CacheEntryListener<Integer, Integer> create() {
                    return new CreateUpdateRemoveListener();
                }
            },
            new TestFilterFactory(),
            true,
            false
        );

        grid = startGrid(gridCount());

        cache = grid.jcache(null);

        log.info("Check filter for listener in configuration.");

        checkFilter(cache, vals);

        stopGrid(gridCount());
    }

    /**
     * @param cache Cache.
     * @param lsnr Listener.
     * @param key Key.
     * @param create {@code True} if listens for create events.
     * @param update {@code True} if listens for update events.
     * @param rmv {@code True} if listens for remove events.
     * @throws Exception If failed.
     */
    private void checkEvents(
        final IgniteCache<Integer, Integer> cache,
        final CacheEntryListener<Integer, Integer> lsnr,
        Integer key,
        boolean create,
        boolean update,
        boolean rmv) throws Exception {
        CacheEntryListenerConfiguration<Integer, Integer> lsnrCfg = new MutableCacheEntryListenerConfiguration<>(
            new Factory<CacheEntryListener<Integer, Integer>>() {
                @Override public CacheEntryListener<Integer, Integer> create() {
                    return lsnr;
                }
            },
            null,
            true,
            false
        );

        cache.registerCacheEntryListener(lsnrCfg);

        checkEvents(cache, lsnrCfg, key, create, update, rmv);
    }

    /**
     * @param cache Cache.
     * @param vals Values in cache.
     * @throws Exception If failed.
     */
    private void checkFilter(IgniteCache<Integer, Integer> cache, Map<Integer, Integer> vals) throws Exception {
        evts = new ArrayList<>();

        final int expEvts = (vals.size() / 2) * 3; // Remove, create and update for half of modified entries.

        evtsLatch = new CountDownLatch(expEvts);

        cache.removeAll(vals.keySet());

        cache.putAll(vals);

        Map<Integer, Integer> newVals = new HashMap<>();

        for (Integer key : vals.keySet())
            newVals.put(key, -1);

        cache.putAll(newVals);

        evtsLatch.await(5000, TimeUnit.MILLISECONDS);

        assertEquals(expEvts, evts.size());

        Iterator<CacheEntryEvent<? extends Integer, ? extends Integer>> iter = evts.iterator();

        for (Integer key : vals.keySet()) {
            if (key % 2 == 0) {
                CacheEntryEvent<? extends Integer, ? extends Integer> evt = iter.next();

                assertTrue(evt.getKey() % 2 == 0);
                assertTrue(vals.keySet().contains(evt.getKey()));
                assertEquals(REMOVED, evt.getEventType());
                assertNull(evt.getValue());
                assertEquals(vals.get(evt.getKey()), evt.getOldValue());

                iter.remove();
            }
        }

        for (Integer key : vals.keySet()) {
            if (key % 2 == 0) {
                CacheEntryEvent<? extends Integer, ? extends Integer> evt = iter.next();

                assertTrue(evt.getKey() % 2 == 0);
                assertTrue(vals.keySet().contains(evt.getKey()));
                assertEquals(CREATED, evt.getEventType());
                assertEquals(vals.get(evt.getKey()), evt.getValue());
                assertNull(evt.getOldValue());

                iter.remove();
            }
        }

        for (Integer key : vals.keySet()) {
            if (key % 2 == 0) {
                CacheEntryEvent<? extends Integer, ? extends Integer> evt = iter.next();

                assertTrue(evt.getKey() % 2 == 0);
                assertTrue(vals.keySet().contains(evt.getKey()));
                assertEquals(UPDATED, evt.getEventType());
                assertEquals(-1, (int) evt.getValue());
                assertEquals(vals.get(evt.getKey()), evt.getOldValue());

                iter.remove();
            }
        }
    }

    /**
     * @param cache Cache.
     * @param lsnrCfg Listener configuration.
     * @param key Key.
     * @param create {@code True} if listens for create events.
     * @param update {@code True} if listens for update events.
     * @param rmv {@code True} if listens for remove events.
     * @throws Exception If failed.
     */
    private void checkEvents(
        final IgniteCache<Integer, Integer> cache,
        final CacheEntryListenerConfiguration<Integer, Integer> lsnrCfg,
        Integer key,
        boolean create,
        boolean update,
        boolean rmv) throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.registerCacheEntryListener(lsnrCfg);

                return null;
            }
        }, IllegalArgumentException.class, null);

        final int UPDATES = 10;

        int expEvts = 0;

        if (create)
            expEvts += 2;

        if (update)
            expEvts += (UPDATES + 1);

        if (rmv)
            expEvts += 2;

        evts = new ArrayList<>();

        evtsLatch = new CountDownLatch(expEvts);

        cache.put(key, 0);

        for (int i = 0; i < UPDATES; i++)
            cache.put(key, i + 1);

        assertFalse(cache.putIfAbsent(key, -1));

        assertFalse(cache.remove(key, -1));

        assertTrue(cache.remove(key));

        IgniteCache<Integer, Integer> cache1 = cache;

        if (gridCount() > 1)
            cache1 = jcache(1); // Do updates from another node.

        cache1.put(key, 1);

        cache1.put(key, 2);

        assertTrue(cache1.remove(key));

        evtsLatch.await(5000, TimeUnit.MILLISECONDS);

        assertEquals(expEvts, evts.size());

        Iterator<CacheEntryEvent<? extends Integer, ? extends Integer>> iter = evts.iterator();

        if (create)
            checkEvent(iter, key, CREATED, 0, null);

        if (update) {
            for (int i = 0; i < UPDATES; i++)
                checkEvent(iter, key, UPDATED, i + 1, i);
        }

        if (rmv)
            checkEvent(iter, key, REMOVED, null, UPDATES);

        if (create)
            checkEvent(iter, key, CREATED, 1, null);

        if (update)
            checkEvent(iter, key, UPDATED, 2, 1);

        if (rmv)
            checkEvent(iter, key, REMOVED, null, 2);

        assertEquals(0, evts.size());

        log.info("Remove listener. ");

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
        GridCache<Integer, Object> cache = cache(0);

        ArrayList<Integer> keys = new ArrayList<>();

        keys.add(primaryKeys(cache, 1, lastKey).get(0));

        if (gridCount() > 1) {
            keys.add(backupKeys(cache, 1, lastKey).get(0));

            if (cache.configuration().getCacheMode() != REPLICATED)
                keys.add(nearKeys(cache, 1, lastKey).get(0));
        }

        lastKey = Collections.max(keys) + 1;

        return keys;
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
        //System.out.println("Received event [evt=" + evt + ", thread=" + Thread.currentThread().getName() + ']');

        assert evt != null;
        assert evt.getSource() != null : evt;
        assert evt.getEventType() != null : evt;
        assert evt.getKey() != null : evt;

        evts.add(evt);

        evtsLatch.countDown();
    }

    /**
     *
     */
    static class TestFilterFactory implements Factory<CacheEntryEventFilter<Integer, Integer>> {
        /** {@inheritDoc} */
        @Override public CacheEntryEventFilter<Integer, Integer> create() {
            return new TestFilter();
        }
    }

    /**
     *
     */
    static class TestFilter implements CacheEntryEventFilter<Integer, Integer> {
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
    static class CreateUpdateListener implements CacheEntryCreatedListener<Integer, Integer>,
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
    static class CreateUpdateRemoveListener extends CreateUpdateListener
        implements CacheEntryRemovedListener<Integer, Integer> {
        /** {@inheritDoc} */
        @Override public void onRemoved(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
            for (CacheEntryEvent<? extends Integer, ? extends Integer> evt : evts)
                onEvent(evt);
        }
    }
}
