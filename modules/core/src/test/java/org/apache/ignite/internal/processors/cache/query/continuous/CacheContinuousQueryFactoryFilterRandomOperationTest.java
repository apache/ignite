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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.CacheQueryEntryEvent;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.testframework.GridTestUtils.SF;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryFactoryFilterRandomOperationTest.NonSerializableFilter.isAccepted;
import static org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryRandomOperationsTest.ContinuousDeploy.CLIENT;
import static org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryRandomOperationsTest.ContinuousDeploy.SERVER;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class CacheContinuousQueryFactoryFilterRandomOperationTest extends CacheContinuousQueryRandomOperationsTest {
    /** */
    private static final int NODES = 5;

    /** */
    private static final int KEYS = 50;

    /** */
    private static final int VALS = 10;

    /** */
    public static final int ITERATION_CNT = SF.applyLB(40, 5);

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInternalQuery() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(REPLICATED,
            1,
            ATOMIC,
            false);

        final IgniteCache<Object, Object> cache = grid(0).createCache(ccfg);

        UUID uuid = null;

        try {
            for (int i = 0; i < 10; i++)
                cache.put(i, i);

            final CountDownLatch latch = new CountDownLatch(5);

            CacheEntryUpdatedListener lsnr = new CacheEntryUpdatedListener() {
                @Override public void onUpdated(Iterable iterable) throws CacheEntryListenerException {
                    for (Object evt : iterable) {
                        latch.countDown();

                        log.info("Received event: " + evt);
                    }
                }
            };

            uuid = grid(0).context().cache().cache(cache.getName()).context().continuousQueries()
                .executeInternalQuery(lsnr, new SerializableFilter(), false, true, true, false);

            for (int i = 10; i < 20; i++)
                cache.put(i, i);

            assertTrue(latch.await(3, SECONDS));
        }
        finally {
            if (uuid != null)
                grid(0).context().cache().cache(cache.getName()).context().continuousQueries()
                    .cancelInternalQuery(uuid);

            grid(0).destroyCache(ccfg.getName());
        }
    }

    /** {@inheritDoc} */
    @Override protected void doTestContinuousQuery(CacheConfiguration<Object, Object> ccfg, ContinuousDeploy deploy)
        throws Exception {
        ignite(0).createCache(ccfg);

        try {
            long seed = System.currentTimeMillis();

            Random rnd = new Random(seed);

            log.info("Random seed: " + seed);

            List<BlockingQueue<CacheEntryEvent<?, ?>>> evtsQueues = new ArrayList<>();

            Collection<QueryCursor<?>> curs = new ArrayList<>();

            Collection<T2<Integer, MutableCacheEntryListenerConfiguration>> lsnrCfgs = new ArrayList<>();

            if (deploy == CLIENT)
                evtsQueues.add(registerListener(ccfg.getName(), NODES - 1, curs, lsnrCfgs, rnd.nextBoolean()));
            else if (deploy == SERVER)
                evtsQueues.add(registerListener(ccfg.getName(), rnd.nextInt(NODES - 1), curs, lsnrCfgs,
                    rnd.nextBoolean()));
            else {
                boolean isSync = rnd.nextBoolean();

                for (int i = 0; i < NODES - 1; i++)
                    evtsQueues.add(registerListener(ccfg.getName(), i, curs, lsnrCfgs, isSync));
            }

            ConcurrentMap<Object, Object> expData = new ConcurrentHashMap<>();

            Map<Integer, Long> partCntr = new ConcurrentHashMap<>();

            try {
                for (int i = 0; i < ITERATION_CNT; i++) {
                    if (i % 10 == 0)
                        log.info("Iteration: " + i);

                    for (int idx = 0; idx < NODES; idx++)
                        randomUpdate(rnd, evtsQueues, expData, partCntr, grid(idx).cache(ccfg.getName()));
                }
            }
            finally {
                for (QueryCursor<?> cur : curs)
                    cur.close();

                for (T2<Integer, MutableCacheEntryListenerConfiguration> e : lsnrCfgs)
                    grid(e.get1()).cache(ccfg.getName()).deregisterCacheEntryListener(e.get2());
            }
        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @param cacheName Cache name.
     * @param nodeIdx Node index.
     * @param curs Cursors.
     * @param lsnrCfgs Listener configurations.
     * @return Event queue
     */
    private BlockingQueue<CacheEntryEvent<?, ?>> registerListener(String cacheName,
        int nodeIdx,
        Collection<QueryCursor<?>> curs,
        Collection<T2<Integer, MutableCacheEntryListenerConfiguration>> lsnrCfgs,
        boolean sync) {
        final BlockingQueue<CacheEntryEvent<?, ?>> evtsQueue = new ArrayBlockingQueue<>(50_000);

        if (ThreadLocalRandom.current().nextBoolean()) {
            MutableCacheEntryListenerConfiguration<QueryTestKey, QueryTestValue> lsnrCfg =
                new MutableCacheEntryListenerConfiguration<>(
                    FactoryBuilder.factoryOf(new LocalNonSerialiseListener() {
                        @Override protected void onEvents(Iterable<CacheEntryEvent<? extends QueryTestKey,
                            ? extends QueryTestValue>> evts) {
                            for (CacheEntryEvent<?, ?> evt : evts)
                                evtsQueue.add(evt);
                        }
                    }),
                    createFilterFactory(),
                    true,
                    sync
                );

            grid(nodeIdx).cache(cacheName).registerCacheEntryListener((CacheEntryListenerConfiguration)lsnrCfg);

            lsnrCfgs.add(new T2<Integer, MutableCacheEntryListenerConfiguration>(nodeIdx, lsnrCfg));
        }
        else {
            ContinuousQuery<QueryTestKey, QueryTestValue> qry = new ContinuousQuery<>();

            qry.setLocalListener(new CacheEntryUpdatedListener<QueryTestKey, QueryTestValue>() {
                @Override public void onUpdated(Iterable<CacheEntryEvent<? extends QueryTestKey,
                    ? extends QueryTestValue>> evts) throws CacheEntryListenerException {
                    for (CacheEntryEvent<?, ?> evt : evts)
                        evtsQueue.add(evt);
                }
            });

            qry.setRemoteFilterFactory(createFilterFactory());

            QueryCursor<?> cur = grid(nodeIdx).cache(cacheName).query(qry);

            curs.add(cur);
        }

        return evtsQueue;
    }

    /**
     * @return Filter factory.
     */
    @NotNull protected Factory<? extends CacheEntryEventFilter<QueryTestKey, QueryTestValue>> createFilterFactory() {
        return new FilterFactory();
    }

    /**
     * @param rnd Random generator.
     * @param evtsQueues Events queue.
     * @param expData Expected cache data.
     * @param partCntr Partition counter.
     * @param cache Cache.
     * @throws Exception If failed.
     */
    private void randomUpdate(
        Random rnd,
        List<BlockingQueue<CacheEntryEvent<?, ?>>> evtsQueues,
        ConcurrentMap<Object, Object> expData,
        Map<Integer, Long> partCntr,
        IgniteCache<Object, Object> cache)
        throws Exception {
        Object key = new QueryTestKey(rnd.nextInt(KEYS));
        Object newVal = value(rnd);
        Object oldVal = expData.get(key);

        int op = rnd.nextInt(11);

        Ignite ignite = cache.unwrap(Ignite.class);

        Transaction tx = null;

        CacheAtomicityMode atomicityMode = atomicityMode(cache);

        boolean mvccEnabled = atomicityMode == TRANSACTIONAL_SNAPSHOT;

        if (atomicityMode != ATOMIC && rnd.nextBoolean()) {
            TransactionConcurrency concurrency = mvccEnabled ? PESSIMISTIC : txRandomConcurrency(rnd);
            TransactionIsolation isolation = mvccEnabled ? REPEATABLE_READ : txRandomIsolation(rnd);

            tx = ignite.transactions().txStart(concurrency, isolation);
        }

        try {
            // log.info("Random operation [key=" + key + ", op=" + op + ']');

            switch (op) {
                case 0: {
                    cache.put(key, newVal);

                    if (tx != null)
                        tx.commit();

                    updatePartitionCounter(cache, key, partCntr, false);

                    waitAndCheckEvent(evtsQueues, partCntr, affinity(cache), key, newVal, oldVal);

                    expData.put(key, newVal);

                    break;
                }

                case 1: {
                    cache.getAndPut(key, newVal);

                    if (tx != null)
                        tx.commit();

                    updatePartitionCounter(cache, key, partCntr, false);

                    waitAndCheckEvent(evtsQueues, partCntr, affinity(cache), key, newVal, oldVal);

                    expData.put(key, newVal);

                    break;
                }

                case 2: {
                    boolean res = cache.remove(key);

                    if (tx != null)
                        tx.commit();

                    // We don't update part counter if nothing was removed when MVCC enabled.
                    updatePartitionCounter(cache, key, partCntr, mvccEnabled && !res);

                    waitAndCheckEvent(evtsQueues, partCntr, affinity(cache), key, oldVal, oldVal);

                    expData.remove(key);

                    break;
                }

                case 3: {
                    Object res = cache.getAndRemove(key);

                    if (tx != null)
                        tx.commit();

                    // We don't update part counter if nothing was removed when MVCC enabled.
                    updatePartitionCounter(cache, key, partCntr, mvccEnabled && res == null);

                    waitAndCheckEvent(evtsQueues, partCntr, affinity(cache), key, oldVal, oldVal);

                    expData.remove(key);

                    break;
                }

                case 4: {
                    cache.invoke(key, new EntrySetValueProcessor(newVal, rnd.nextBoolean()));

                    if (tx != null)
                        tx.commit();

                    updatePartitionCounter(cache, key, partCntr, false);

                    waitAndCheckEvent(evtsQueues, partCntr, affinity(cache), key, newVal, oldVal);

                    expData.put(key, newVal);

                    break;
                }

                case 5: {
                    EntrySetValueProcessor proc = new EntrySetValueProcessor(null, rnd.nextBoolean());

                    cache.invoke(key, proc);

                    if (tx != null)
                        tx.commit();

                    // We don't update part counter if nothing was removed when MVCC enabled.
                    updatePartitionCounter(cache, key, partCntr, mvccEnabled && proc.getOldVal() == null);

                    waitAndCheckEvent(evtsQueues, partCntr, affinity(cache), key, oldVal, oldVal);

                    expData.remove(key);

                    break;
                }

                case 6: {
                    cache.putIfAbsent(key, newVal);

                    if (tx != null)
                        tx.commit();

                    if (oldVal == null) {
                        updatePartitionCounter(cache, key, partCntr, false);

                        waitAndCheckEvent(evtsQueues, partCntr, affinity(cache), key, newVal, null);

                        expData.put(key, newVal);
                    }
                    else
                        checkNoEvent(evtsQueues);

                    break;
                }

                case 7: {
                    cache.getAndPutIfAbsent(key, newVal);

                    if (tx != null)
                        tx.commit();

                    if (oldVal == null) {
                        updatePartitionCounter(cache, key, partCntr, false);

                        waitAndCheckEvent(evtsQueues, partCntr, affinity(cache), key, newVal, null);

                        expData.put(key, newVal);
                    }
                    else
                        checkNoEvent(evtsQueues);

                    break;
                }

                case 8: {
                    cache.replace(key, newVal);

                    if (tx != null)
                        tx.commit();

                    if (oldVal != null) {
                        updatePartitionCounter(cache, key, partCntr, false);

                        waitAndCheckEvent(evtsQueues, partCntr, affinity(cache), key, newVal, oldVal);

                        expData.put(key, newVal);
                    }
                    else
                        checkNoEvent(evtsQueues);

                    break;
                }

                case 9: {
                    cache.getAndReplace(key, newVal);

                    if (tx != null)
                        tx.commit();

                    if (oldVal != null) {
                        updatePartitionCounter(cache, key, partCntr, false);

                        waitAndCheckEvent(evtsQueues, partCntr, affinity(cache), key, newVal, oldVal);

                        expData.put(key, newVal);
                    }
                    else
                        checkNoEvent(evtsQueues);

                    break;
                }

                case 10: {
                    if (oldVal != null) {
                        Object replaceVal = value(rnd);

                        boolean success = replaceVal.equals(oldVal);

                        if (success) {
                            cache.replace(key, replaceVal, newVal);

                            if (tx != null)
                                tx.commit();

                            updatePartitionCounter(cache, key, partCntr, false);

                            waitAndCheckEvent(evtsQueues, partCntr, affinity(cache), key, newVal, oldVal);

                            expData.put(key, newVal);
                        }
                        else {
                            cache.replace(key, replaceVal, newVal);

                            if (tx != null)
                                tx.commit();

                            checkNoEvent(evtsQueues);
                        }
                    }
                    else {
                        cache.replace(key, value(rnd), newVal);

                        if (tx != null)
                            tx.commit();

                        checkNoEvent(evtsQueues);
                    }

                    break;
                }

                default:
                    fail("Op:" + op);
            }
        }
        finally {
            if (tx != null)
                tx.close();
        }
    }

    /**
     * @param rnd {@link Random}.
     * @return {@link TransactionIsolation}.
     */
    private TransactionIsolation txRandomIsolation(Random rnd) {
        int val = rnd.nextInt(3);

        if (val == 0)
            return READ_COMMITTED;
        else if (val == 1)
            return REPEATABLE_READ;
        else
            return SERIALIZABLE;
    }

    /**
     * @param rnd {@link Random}.
     * @return {@link TransactionConcurrency}.
     */
    private TransactionConcurrency txRandomConcurrency(Random rnd) {
        return rnd.nextBoolean() ? TransactionConcurrency.OPTIMISTIC : TransactionConcurrency.PESSIMISTIC;
    }

    /**
     * @param cache Cache.
     * @param key Key
     * @param cntrs Partition counters.
     * @param skipUpdCntr Skip update counter flag.
     */
    private void updatePartitionCounter(IgniteCache<Object, Object> cache, Object key, Map<Integer, Long> cntrs,
        boolean skipUpdCntr) {
        Affinity<Object> aff = cache.unwrap(Ignite.class).affinity(cache.getName());

        int part = aff.partition(key);

        Long partCntr = cntrs.get(part);

        if (partCntr == null)
            partCntr = 0L;

        if (!skipUpdCntr)
            partCntr++;

        cntrs.put(part, partCntr);
    }

    /**
     * @param rnd Random generator.
     * @return Cache value.
     */
    private static Object value(Random rnd) {
        return new QueryTestValue(rnd.nextInt(VALS));
    }

    /**
     * @param evtsQueues Event queue.
     * @param partCntrs Partition counters.
     * @param aff Affinity function.
     * @param key Key.
     * @param val Value.
     * @param oldVal Old value.
     * @throws Exception If failed.
     */
    private void waitAndCheckEvent(List<BlockingQueue<CacheEntryEvent<?, ?>>> evtsQueues,
        Map<Integer, Long> partCntrs,
        Affinity<Object> aff,
        Object key,
        Object val,
        Object oldVal)
        throws Exception {
        if ((val == null && oldVal == null
            || (val != null && !isAccepted((QueryTestValue)val)))) {
            checkNoEvent(evtsQueues);

            return;
        }

        for (BlockingQueue<CacheEntryEvent<?, ?>> evtsQueue : evtsQueues) {
            CacheEntryEvent<?, ?> evt = evtsQueue.poll(5, SECONDS);

            assertNotNull("Failed to wait for event [key=" + key + ", val=" + val + ", oldVal=" + oldVal + ']', evt);
            assertEquals(key, evt.getKey());
            assertEquals(val, evt.getValue());
            assertEquals(oldVal, evt.getOldValue());

            long cntr = partCntrs.get(aff.partition(key));
            CacheQueryEntryEvent qryEntryEvt = evt.unwrap(CacheQueryEntryEvent.class);

            assertNotNull(cntr);
            assertNotNull(qryEntryEvt);

            assertEquals(cntr, qryEntryEvt.getPartitionUpdateCounter());
        }
    }

    /**
     * @param evtsQueues Event queue.
     * @throws Exception If failed.
     */
    private void checkNoEvent(List<BlockingQueue<CacheEntryEvent<?, ?>>> evtsQueues) throws Exception {
        for (BlockingQueue<CacheEntryEvent<?, ?>> evtsQueue : evtsQueues) {
            CacheEntryEvent<?, ?> evt = evtsQueue.poll(50, MILLISECONDS);

            assertNull(evt);
        }
    }

    /**
     *
     */
    protected static class NonSerializableFilter
        implements CacheEntryEventSerializableFilter<CacheContinuousQueryRandomOperationsTest.QueryTestKey,
            CacheContinuousQueryRandomOperationsTest.QueryTestValue>, Externalizable {
        /** */
        public NonSerializableFilter() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean evaluate(CacheEntryEvent<? extends QueryTestKey, ? extends QueryTestValue> evt) {
            return isAccepted(evt.getValue());
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            fail("Entry filter should not be marshaled.");
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            fail("Entry filter should not be marshaled.");
        }

        /**
         * @param val Value.
         * @return {@code True} if value is even.
         */
        public static boolean isAccepted(QueryTestValue val) {
            return val == null || val.val1 % 2 == 0;
        }
    }

    /**
     *
     */
    protected static class SerializableFilter implements CacheEntryEventSerializableFilter<Integer, Integer> {
        /** */
        public SerializableFilter() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean evaluate(CacheEntryEvent<? extends Integer, ? extends Integer> evt)
            throws CacheEntryListenerException {
            return isAccepted(evt.getValue());
        }

        /**
         * @return {@code True} if value is even.
         */
        public static boolean isAccepted(Integer val) {
            return val == null || val % 2 == 0;
        }
    }

    /**
     *
     */
    protected static class FilterFactory implements Factory<NonSerializableFilter> {
        /** {@inheritDoc} */
        @Override public NonSerializableFilter create() {
            return new NonSerializableFilter();
        }
    }

    /**
     *
     */
    public abstract class LocalNonSerialiseListener implements
        CacheEntryUpdatedListener<QueryTestKey, QueryTestValue>,
        CacheEntryCreatedListener<QueryTestKey, QueryTestValue>,
        CacheEntryExpiredListener<QueryTestKey, QueryTestValue>,
        CacheEntryRemovedListener<QueryTestKey, QueryTestValue>,
        Externalizable {
        /** */
        public LocalNonSerialiseListener() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onCreated(Iterable<CacheEntryEvent<? extends QueryTestKey,
            ? extends QueryTestValue>> evts) throws CacheEntryListenerException {
            onEvents(evts);
        }

        /** {@inheritDoc} */
        @Override public void onExpired(Iterable<CacheEntryEvent<? extends QueryTestKey,
            ? extends QueryTestValue>> evts) throws CacheEntryListenerException {
            onEvents(evts);
        }

        /** {@inheritDoc} */
        @Override public void onRemoved(Iterable<CacheEntryEvent<? extends QueryTestKey,
            ? extends QueryTestValue>> evts) throws CacheEntryListenerException {
            onEvents(evts);
        }

        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<CacheEntryEvent<? extends QueryTestKey,
            ? extends QueryTestValue>> evts) throws CacheEntryListenerException {
            onEvents(evts);
        }

        /**
         * @param evts Events.
         */
        protected abstract void onEvents(Iterable<CacheEntryEvent<? extends QueryTestKey,
            ? extends QueryTestValue>> evts);

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            throw new UnsupportedOperationException("Failed. Listener should not be marshaled.");
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            throw new UnsupportedOperationException("Failed. Listener should not be unmarshaled.");
        }
    }
}
