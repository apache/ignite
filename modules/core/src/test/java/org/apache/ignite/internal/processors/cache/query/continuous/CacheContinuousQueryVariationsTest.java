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
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListener;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.EventType;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteAsyncCallback;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.IgniteCacheConfigVariationsAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.cache.event.EventType.CREATED;
import static javax.cache.event.EventType.REMOVED;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryVariationsTest.SerializableFilter.isAccepted;
import static org.apache.ignite.testframework.junits.IgniteConfigVariationsAbstractTest.DataMode.EXTERNALIZABLE;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class CacheContinuousQueryVariationsTest extends IgniteCacheConfigVariationsAbstractTest {
    /** */
    private static final int ITERATION_CNT = 20;

    /** */
    private static final int KEYS = 50;

    /** */
    private static final int VALS = 10;

    /** */
    public static boolean singleNode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode(igniteInstanceName.endsWith("0") && !singleNode);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void unconditionalCleanupAfterTests() {
        // No-op.
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRandomOperationJCacheApiKeepBinary() throws Exception {
        testRandomOperation(true, false, false, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRandomOperationJCacheApiAsyncCallback() throws Exception {
        testRandomOperation(true, false, false, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRandomOperationJCacheApiWithFilter() throws Exception {
        testRandomOperation(true, false, true, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRandomOperationJCacheApiWithFilterAsyncCallback() throws Exception {
        testRandomOperation(true, false, true, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRandomOperationJCacheApiSyncWithFilter() throws Exception {
        testRandomOperation(true, true, true, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRandomOperation() throws Exception {
        testRandomOperation(true, true, false, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRandomOperationWithKeepBinary() throws Exception {
        testRandomOperation(true, true, false, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRandomOperationWithAsyncCallback() throws Exception {
        testRandomOperation(true, true, false, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRandomOperationWithFilter() throws Exception {
        testRandomOperation(true, true, true, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRandomOperationWithFilterWithKeepBinary() throws Exception {
        testRandomOperation(true, true, true, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRandomOperationWithFilterAsyncCallback() throws Exception {
        testRandomOperation(true, true, true, true, false);
    }

    /**
     * @param jcacheApi Use JCache API.
     * @param syncNtf Use sync notification.
     * @param withFilter Use filter.
     * @param asyncCallback Filter is annotated IgniteAsyncCallback
     * @param keepBinary Keep binary.
     * @throws Exception If failed.
     */
    private void testRandomOperation(final boolean jcacheApi, final boolean syncNtf, final boolean withFilter,
        final boolean asyncCallback, final boolean keepBinary)
        throws Exception {
        if (keepBinary && !(getConfiguration().getMarshaller() == null
            || getConfiguration().getMarshaller().getClass() == BinaryMarshaller.class))
            return;

        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                long seed = System.currentTimeMillis();

                Random rnd = new Random(seed);

                log.info("Random seed: " + seed);

                // Register listener on all nodes.
                List<BlockingQueue<CacheEntryEvent<?, ?>>> evtsQueues = new ArrayList<>();

                Collection<QueryCursor<?>> curs = new ArrayList<>();

                Collection<MutableCacheEntryListenerConfiguration> lsnrCfgs = new ArrayList<>();

                for (int idx = 0; idx < G.allGrids().size(); idx++) {
                    final BlockingQueue<CacheEntryEvent<?, ?>> evtsQueue = new ArrayBlockingQueue<>(50_000);

                    CI1<Iterable<CacheEntryEvent<?, ?>>> clsr = new CI1<Iterable<CacheEntryEvent<?, ?>>>() {
                        @Override public void apply(Iterable<CacheEntryEvent<?, ?>> evts) {
                            for (CacheEntryEvent<?, ?> evt : evts)
                                evtsQueue.add(evt);
                        }
                    };

                    final CacheEntryUpdatedListener<Object, Object> lsnr = asyncCallback ?
                        new AsyncLocalNonSerializableListener(clsr) : new LocalNonSerializableListener(clsr);

                    IgniteCache<Object, Object> jcache = keepBinary ? jcache(idx).withKeepBinary() : jcache(idx);

                    if (jcacheApi) {
                        MutableCacheEntryListenerConfiguration<Object, Object> lsnrCfg =
                            new MutableCacheEntryListenerConfiguration<>(
                                new Factory<CacheEntryListener<? super Object, ? super Object>>() {
                                    @Override public CacheEntryListener<? super Object, ? super Object> create() {
                                        return lsnr;
                                    }
                                },
                                withFilter ?
                                    FactoryBuilder.factoryOf(
                                        asyncCallback ? new AsyncSerializableFilter(keepBinary, dataMode)
                                            : new SerializableFilter(keepBinary, dataMode))
                                    : null,
                                true,
                                syncNtf
                            );

                        jcache.registerCacheEntryListener(lsnrCfg);

                        lsnrCfgs.add(lsnrCfg);

                        evtsQueues.add(evtsQueue);
                    }
                    else {
                        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

                        qry.setLocalListener(lsnr);

                        qry.setRemoteFilterFactory(withFilter ?
                            FactoryBuilder.factoryOf(
                                asyncCallback ? new AsyncSerializableFilter(keepBinary, dataMode)
                                    : new SerializableFilter(keepBinary, dataMode))
                            : null);

                        curs.add(jcache.query(qry));

                        evtsQueues.add(evtsQueue);
                    }
                }

                ConcurrentMap<Object, Object> expData = new ConcurrentHashMap<>();

                try {
                    for (int i = 0; i < ITERATION_CNT; i++) {
                        if (i % 5 == 0)
                            log.info("Iteration: " + i);

                        for (int idx = 0; idx < G.allGrids().size(); idx++)
                            randomUpdate(rnd,
                                evtsQueues,
                                expData,
                                keepBinary ? jcache(idx).withKeepBinary() : jcache(idx),
                                keepBinary,
                                withFilter);
                    }
                }
                catch (Exception e) {
                    log.error("Got unexpected error: ", e);

                    throw e;
                }
                finally {
                    for (QueryCursor<?> cur : curs)
                        cur.close();

                    for (int i = 0; i < G.allGrids().size(); i++) {
                        for (MutableCacheEntryListenerConfiguration cfg : lsnrCfgs)
                            jcache(i).deregisterCacheEntryListener(cfg);
                    }
                }
            }
        });
    }

    /**
     * @param rnd Random generator.
     * @param evtsQueues Events queue.
     * @param expData Expected cache data.
     * @param cache Cache.
     * @throws Exception If failed.
     */
    private void randomUpdate(
        Random rnd,
        List<BlockingQueue<CacheEntryEvent<?, ?>>> evtsQueues,
        ConcurrentMap<Object, Object> expData,
        IgniteCache<Object, Object> cache,
        boolean keepBinary,
        boolean withFilter
    ) throws Exception {
        Object key = key(rnd.nextInt(KEYS));
        Object newVal = value(rnd.nextInt());
        Object oldVal = expData.get(key);

        int op = rnd.nextInt(11);

        Ignite ignite = cache.unwrap(Ignite.class);

        Transaction tx = null;

        if (cache.getConfiguration(CacheConfiguration.class).getAtomicityMode() == TRANSACTIONAL && rnd.nextBoolean())
            tx = ignite.transactions().txStart(txRandomConcurrency(rnd), txRandomIsolation(rnd));

        try {
            // log.info("Random operation [key=" + key + ", op=" + op + ']');

            switch (op) {
                case 0: {
                    cache.put(key, newVal);

                    if (tx != null)
                        tx.commit();

                    waitAndCheckEvent(evtsQueues, key, newVal, oldVal, keepBinary, withFilter);

                    expData.put(key, newVal);

                    break;
                }

                case 1: {
                    cache.getAndPut(key, newVal);

                    if (tx != null)
                        tx.commit();

                    waitAndCheckEvent(evtsQueues, key, newVal, oldVal, keepBinary, withFilter);

                    expData.put(key, newVal);

                    break;
                }

                case 2: {
                    cache.remove(key);

                    if (tx != null)
                        tx.commit();

                    waitAndCheckEvent(evtsQueues, key, oldVal, oldVal, keepBinary, withFilter);

                    expData.remove(key);

                    break;
                }

                case 3: {
                    cache.getAndRemove(key);

                    if (tx != null)
                        tx.commit();

                    waitAndCheckEvent(evtsQueues, key, oldVal, oldVal, keepBinary, withFilter);

                    expData.remove(key);

                    break;
                }

                case 4: {
                    cache.invoke(key, new EntrySetValueProcessor(newVal, rnd.nextBoolean()));

                    if (tx != null)
                        tx.commit();

                    waitAndCheckEvent(evtsQueues, key, newVal, oldVal, keepBinary, withFilter);

                    expData.put(key, newVal);

                    break;
                }

                case 5: {
                    cache.invoke(key, new EntrySetValueProcessor(null, rnd.nextBoolean()));

                    if (tx != null)
                        tx.commit();

                    waitAndCheckEvent(evtsQueues, key, oldVal, oldVal, keepBinary, withFilter);

                    expData.remove(key);

                    break;
                }

                case 6: {
                    cache.putIfAbsent(key, newVal);

                    if (tx != null)
                        tx.commit();

                    if (oldVal == null) {
                        waitAndCheckEvent(evtsQueues, key, newVal, null, keepBinary, withFilter);

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
                        waitAndCheckEvent(evtsQueues, key, newVal, null, keepBinary, withFilter);

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
                        waitAndCheckEvent(evtsQueues, key, newVal, oldVal, keepBinary, withFilter);

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
                        waitAndCheckEvent(evtsQueues, key, newVal, oldVal, keepBinary, withFilter);

                        expData.put(key, newVal);
                    }
                    else
                        checkNoEvent(evtsQueues);

                    break;
                }

                case 10: {
                    if (oldVal != null) {
                        Object replaceVal = value(rnd.nextInt(VALS));

                        boolean success = replaceVal.equals(oldVal);

                        if (success) {
                            cache.replace(key, replaceVal, newVal);

                            if (tx != null)
                                tx.commit();

                            waitAndCheckEvent(evtsQueues, key, newVal, oldVal, keepBinary, withFilter);

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
                        cache.replace(key, value(rnd.nextInt(VALS)), newVal);

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

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TimeUnit.MINUTES.toMillis(5);
    }

    /**
     * @param rnd {@link Random}.
     * @return {@link TransactionConcurrency}.
     */
    private TransactionConcurrency txRandomConcurrency(Random rnd) {
        return rnd.nextBoolean() ? TransactionConcurrency.OPTIMISTIC : TransactionConcurrency.PESSIMISTIC;
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
     * @param evtsQueues Event queue.
     * @param key Key.
     * @param val Value.
     * @param oldVal Old value.
     * @param keepBinary Keep binary.
     * @param withFilter With filter.
     * @throws Exception If failed.
     */
    private void waitAndCheckEvent(List<BlockingQueue<CacheEntryEvent<?, ?>>> evtsQueues,
        Object key,
        Object val,
        Object oldVal,
        boolean keepBinary, boolean withFilter)
        throws Exception {
        if (val == null && oldVal == null || (withFilter && val != null && !isAccepted(val, false, dataMode))) {
            checkNoEvent(evtsQueues);

            return;
        }

        for (BlockingQueue<CacheEntryEvent<?, ?>> evtsQueue : evtsQueues) {
            CacheEntryEvent<?, ?> evt = evtsQueue.poll(5, SECONDS);

            assertNotNull("Failed to wait for event [key=" + key + ", val=" + val + ", oldVal=" + oldVal + ']', evt);

            Object actKey = evt.getKey();
            Object actVal = evt.getValue();
            Object actOldVal = evt.getOldValue();

            if (keepBinary) {
                actKey = checkAndGetObject(actKey);
                actVal = checkAndGetObject(actVal);
                actOldVal = checkAndGetObject(actOldVal);
            }

            assertEquals(key, actKey);
            assertEquals(val, actVal);
            assertEquals(oldVal, actOldVal);
        }
    }

    /**
     * @param obj Binary object.
     * @return Deserialize value.
     */
    private Object checkAndGetObject(@Nullable Object obj) {
        if (obj != null) {
            assert obj instanceof BinaryObject || dataMode == EXTERNALIZABLE : obj;

            if (obj instanceof BinaryObject)
                obj = ((BinaryObject)obj).deserialize();
        }

        return obj;
    }

    /**
     * @param evtsQueues Event queue.
     * @throws Exception If failed.
     */
    private void checkNoEvent(List<BlockingQueue<CacheEntryEvent<?, ?>>> evtsQueues) throws Exception {
        for (BlockingQueue<CacheEntryEvent<?, ?>> evtsQueue : evtsQueues) {
            CacheEntryEvent<?, ?> evt = evtsQueue.poll(10, MILLISECONDS);

            assertNull(evt);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveRemoveScenario() throws Exception {
        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                IgniteCache<Object, Object> cache = jcache();

                ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

                final List<CacheEntryEvent<?, ?>> evts = new CopyOnWriteArrayList<>();

                qry.setLocalListener(new CacheEntryUpdatedListener<Object, Object>() {
                    @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> events)
                        throws CacheEntryListenerException {
                        for (CacheEntryEvent<?, ?> e : events)
                            evts.add(e);
                    }
                });

                Object key = key(1);

                try (QueryCursor qryCur = cache.query(qry)) {
                    for (int i = 0; i < ITERATION_CNT; i++) {
                        log.info("Start iteration: " + i);
                        // Not events.
                        cache.invoke(key, new EntrySetValueProcessor(true));

                        // Get events.
                        cache.put(key, value(1));
                        cache.remove(key);

                        // Not events.
                        cache.invoke(key, new EntrySetValueProcessor(null, false));
                        cache.invoke(key, new EntrySetValueProcessor(null, false));
                        cache.invoke(key, new EntrySetValueProcessor(true));
                        cache.remove(key);

                        // Get events.
                        cache.put(key, value(2));

                        // Not events.
                        cache.invoke(key, new EntrySetValueProcessor(true));

                        // Get events.
                        cache.invoke(key, new EntrySetValueProcessor(null, false));

                        // Not events.
                        cache.remove(key);

                        // Get events.
                        cache.put(key, value(3));
                        cache.put(key, value(4));

                        // Not events.
                        cache.invoke(key, new EntrySetValueProcessor(true));
                        cache.putIfAbsent(key, value(5));
                        cache.putIfAbsent(key, value(5));
                        cache.putIfAbsent(key, value(5));
                        cache.invoke(key, new EntrySetValueProcessor(true));
                        cache.remove(key, value(5));

                        // Get events.
                        cache.remove(key, value(4));
                        cache.putIfAbsent(key, value(5));

                        // Not events.
                        cache.replace(key, value(3), value(2));
                        cache.replace(key, value(3), value(2));
                        cache.replace(key, value(3), value(2));

                        // Get events.
                        cache.replace(key, value(5), value(6));

                        assert GridTestUtils.waitForCondition(new PA() {
                            @Override public boolean apply() {
                                return evts.size() == 9;
                            }
                        }, 5_000);

                        checkEvent(evts.get(0), CREATED, value(1), null);
                        checkEvent(evts.get(1), REMOVED, value(1), value(1));
                        checkEvent(evts.get(2), CREATED, value(2), null);
                        checkEvent(evts.get(3), REMOVED, value(2), value(2));
                        checkEvent(evts.get(4), CREATED, value(3), null);
                        checkEvent(evts.get(5), EventType.UPDATED, value(4), value(3));
                        checkEvent(evts.get(6), REMOVED, value(4), value(4));
                        checkEvent(evts.get(7), CREATED, value(5), null);
                        checkEvent(evts.get(8), EventType.UPDATED, value(6), value(5));

                        cache.remove(key);
                        cache.remove(key);

                        //Wait when remove event will be added to evts
                        while (evts.size() != 10) {
                            Thread.sleep(100);
                        }

                        evts.clear();

                        log.info("Finish iteration: " + i);
                    }
                }
            }
        });
    }

    /**
     * @param event Event.
     * @param type Event type.
     * @param val Value.
     * @param oldVal Old value.
     */
    private void checkEvent(CacheEntryEvent<?, ?> event, EventType type, Object val, Object oldVal) {
        assertEquals(event.getEventType(), type);
        assertEquals(event.getValue(), val);
        assertEquals(event.getOldValue(), oldVal);
    }

    /**
     *
     */
    protected static class EntrySetValueProcessor implements EntryProcessor<Object, Object, Object> {
        /** */
        private Object val;

        /** */
        private boolean retOld;

        /** */
        private boolean skipModify;

        /**
         * @param skipModify If {@code true} then entry will not be modified.
         */
        public EntrySetValueProcessor(boolean skipModify) {
            this.skipModify = skipModify;
        }

        /**
         * @param val Value to set.
         * @param retOld Return old value flag.
         */
        public EntrySetValueProcessor(Object val, boolean retOld) {
            this.val = val;
            this.retOld = retOld;
        }

        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Object, Object> e, Object... args) {
            if (skipModify)
                return null;

            Object old = retOld ? e.getValue() : null;

            if (val != null)
                e.setValue(val);
            else
                e.remove();

            return old;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(EntrySetValueProcessor.class, this);
        }
    }

    /**
     *
     */
    @IgniteAsyncCallback
    public static class AsyncSerializableFilter extends SerializableFilter {
        /**
         *
         */
        public AsyncSerializableFilter() {
            // No-op.
        }

        /**
         * @param keepBinary Keep binary.
         * @param dataMode Data mode.
         */
        public AsyncSerializableFilter(boolean keepBinary, DataMode dataMode) {
            super(keepBinary, dataMode);
        }
    }

    /**
     *
     */
    public static class SerializableFilter implements CacheEntryEventSerializableFilter<Object, Object> {
        /** */
        private boolean keepBinary;

        /** */
        private DataMode dataMode;

        /** */
        public SerializableFilter() {
            // No-op.
        }

        /**
         * @param keepBinary Keep binary.
         * @param dataMode Data mode.
         */
        public SerializableFilter(boolean keepBinary, DataMode dataMode) {
            this.keepBinary = keepBinary;
            this.dataMode = dataMode;
        }

        /** {@inheritDoc} */
        @Override public boolean evaluate(CacheEntryEvent<?, ?> event)
            throws CacheEntryListenerException {
            return isAccepted(event.getValue(), keepBinary, dataMode);
        }

        /**
         * @param val Value.
         * @param keepBinary Keep binary.
         * @param dataMode Data mode.
         * @return {@code True} if value is even.
         */
        public static boolean isAccepted(Object val, boolean keepBinary, DataMode dataMode) {
            if (val != null) {
                int val0 = 0;

                if (val instanceof TestObject) {
                    assert !keepBinary || dataMode == EXTERNALIZABLE : val;

                    val0 = valueOf(val);
                }
                else if (val instanceof BinaryObject) {
                    assert keepBinary : val;

                    val0 = ((BinaryObject)val).field("val");
                }
                else
                    fail("Unexpected object: " + val);

                return val0 % 2 == 0;
            }

            return true;
        }
    }

    /**
     *
     */
    @IgniteAsyncCallback
    public static class AsyncLocalNonSerializableListener extends LocalNonSerializableListener {
        /**
         * @param clsr Closure.
         */
        AsyncLocalNonSerializableListener(IgniteInClosure<Iterable<CacheEntryEvent<?, ?>>> clsr) {
            super(clsr);
        }

        /**
         *
         */
        public AsyncLocalNonSerializableListener() {
            // No-op.
        }
    }

    /**
     *
     */
    public static class LocalNonSerializableListener implements
        CacheEntryUpdatedListener<Object, Object>,
        CacheEntryCreatedListener<Object, Object>,
        CacheEntryExpiredListener<Object, Object>,
        CacheEntryRemovedListener<Object, Object>,
        Externalizable {
        /** */
        IgniteInClosure<Iterable<CacheEntryEvent<?, ?>>> clsr;

        /**
         * @param clsr Closure.
         */
        LocalNonSerializableListener(IgniteInClosure<Iterable<CacheEntryEvent<?, ?>>> clsr) {
            this.clsr = clsr;
        }

        /** */
        public LocalNonSerializableListener() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onCreated(Iterable<CacheEntryEvent<?, ?>> evts) throws CacheEntryListenerException {
            onEvents(evts);
        }

        /** {@inheritDoc} */
        @Override public void onExpired(Iterable<CacheEntryEvent<?, ?>> evts) throws CacheEntryListenerException {
            onEvents(evts);
        }

        /** {@inheritDoc} */
        @Override public void onRemoved(Iterable<CacheEntryEvent<?, ?>> evts) throws CacheEntryListenerException {
            onEvents(evts);
        }

        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> evts) throws CacheEntryListenerException {
            onEvents(evts);
        }

        /**
         * @param evts Events.
         */
        private void onEvents(Iterable<CacheEntryEvent<?, ?>> evts) {
            clsr.apply(evts);
        }

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
