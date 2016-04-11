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

package org.apache.ignite.yardstick.cache.load;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheTypeMetadata;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.apache.ignite.yardstick.IgniteBenchmarkUtils;
import org.apache.ignite.yardstick.cache.load.model.ModelUtil;
import org.springframework.util.CollectionUtils;

/**
 * Ignite cache random operation benchmark.
 */
public class IgniteCacheRandomOperationBenchmark extends IgniteAbstractBenchmark {
    /** */
    public static final int operations = Operation.values().length;

    /** List off all available cache. */
    private List<IgniteCache> availableCaches;

    /** List of available transactional cache. */
    private List<IgniteCache> transactionalCaches;

    /** Map cache name on key classes. */
    private Map<String, Class[]> keysCacheClasses;

    /** Map cache name on value classes. */
    private Map<String, Class[]> valuesCacheClasses;

    /**
     * Replace value entry processor.
     */
    private BenchmarkReplaceValueEntryProcessor replaceValueEntryProcessor;

    /**
     * Remove entry processor.
     */
    private BenchmarkRemoveEntryProcessor removeEntryProcessor;

    /**
     * Scan query predicate.
     */
    BenchmarkIgniteBiPredicate igniteBiPredicate;

    /**
     * @throws Exception If failed.
     */
    private void searchCache() throws Exception {
        availableCaches = new ArrayList<>(ignite().cacheNames().size());
        transactionalCaches = new ArrayList<>();
        keysCacheClasses = new HashMap<>();
        valuesCacheClasses = new HashMap<>();
        replaceValueEntryProcessor = new BenchmarkReplaceValueEntryProcessor(null);
        removeEntryProcessor = new BenchmarkRemoveEntryProcessor();
        igniteBiPredicate = new BenchmarkIgniteBiPredicate();

        for (String name : ignite().cacheNames()) {
            IgniteCache<Object, Object> cache = ignite().cache(name);

            CacheConfiguration configuration = cache.getConfiguration(CacheConfiguration.class);

            if (isClassDefinedinConfig(configuration)) {
                if (true) continue;

                ArrayList<Class> keys = new ArrayList<>();
                ArrayList<Class> values = new ArrayList<>();
                int i = 0;
                if (configuration.getIndexedTypes() != null) {
                    for (Class clazz : configuration.getIndexedTypes()) {
                        if (ModelUtil.canCreateInstance(clazz))
                            if (i % 2 == 0)
                                keys.add(clazz);
                            else
                                values.add(clazz);
                        i++;
                    }
                }

                if (configuration.getQueryEntities() != null) {
                    Collection<QueryEntity> entries = configuration.getQueryEntities();
                    for (QueryEntity queryEntity : entries) {
                        Class keyClass = getClass().forName(queryEntity.getKeyType());
                        Class valueClass = getClass().forName(queryEntity.getValueType());
                        if (ModelUtil.canCreateInstance(keyClass) && ModelUtil.canCreateInstance(valueClass)) {
                            keys.add(keyClass);
                            values.add(valueClass);
                        }
                    }
                }

                if (configuration.getTypeMetadata() != null) {
                    Collection<CacheTypeMetadata> entries = configuration.getTypeMetadata();
                    for (CacheTypeMetadata cacheTypeMetadata : entries) {
                        Class keyClass = getClass().forName(cacheTypeMetadata.getKeyType());
                        Class valueClass = getClass().forName(cacheTypeMetadata.getValueType());
                        if (ModelUtil.canCreateInstance(keyClass) && ModelUtil.canCreateInstance(valueClass)) {
                            keys.add(keyClass);
                            values.add(valueClass);
                        }
                    }
                }

                if (keys.size() == 0 || values.size() == 0)
                    continue;

                keysCacheClasses.put(name, keys.toArray(new Class[] {}));
                valuesCacheClasses.put(name, values.toArray(new Class[] {}));
            }

            if (configuration.getAtomicityMode() == CacheAtomicityMode.TRANSACTIONAL)
                transactionalCaches.add(cache);

            availableCaches.add(cache);
        }
    }

    /**
     * @param configuration Ignite cache configuration.
     * @return True if defined.
     */
    private boolean isClassDefinedinConfig(CacheConfiguration configuration) {
        return (configuration.getIndexedTypes() != null && configuration.getIndexedTypes().length > 0)
            || !CollectionUtils.isEmpty(configuration.getQueryEntities())
            || !CollectionUtils.isEmpty(configuration.getTypeMetadata());
    }

    /**
     * @throws Exception If fail.
     */
    private void preLoading() throws Exception {
        Thread[] threads = new Thread[availableCaches.size()];
        for (int i = 0; i < availableCaches.size(); i++) {
            final String cacheName = availableCaches.get(i).getName();
            threads[i] = new Thread() {
                @Override
                public void run() {
                    try (IgniteDataStreamer dataLdr = ignite().dataStreamer(cacheName)) {
                        for (int i = 0; i < args.preloadAmount() && !isInterrupted(); i++) {
                            int id = nextRandom(args.range());

                            dataLdr.addData(createRandomKey(id, cacheName), createRandomValue(id, cacheName));
                        }
                    }
                }
            };
            threads[i].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
    }

    /**
     * @param id Object identifier.
     * @param cacheName Name of Ignite cache.
     * @return Random object.
     */
    private Object createRandomKey(int id, String cacheName) {
        Class clazz = randomKeyClass(cacheName);
        return ModelUtil.create(clazz, id);
    }

    /**
     * @param cacheName Ignite cache name.
     * @return Random key class.
     */
    private Class randomKeyClass(String cacheName) {
        Class[] keys;
        if (keysCacheClasses.containsKey(cacheName)) {
            keys = valuesCacheClasses.get(cacheName);
        }
        else {
            keys = ModelUtil.keyClasses();
        }
        return keys[nextRandom(keys.length)];
    }

    /**
     * @param id Object identifier.
     * @param cacheName Name of Ignite cache.
     * @return Random object.
     */
    private Object createRandomValue(int id, String cacheName) {
        Class clazz = randomValueClass(cacheName);
        return ModelUtil.create(clazz, id);
    }

    /**
     * @param cacheName Ignite cache name.
     * @return Random value class.
     */
    private Class randomValueClass(String cacheName) {
        Class[] values;

        if (valuesCacheClasses.containsKey(cacheName)) {
            values = valuesCacheClasses.get(cacheName);
        }
        else {
            values = ModelUtil.valueClasses();
        }
        return values[nextRandom(values.length)];
    }

    /** {@inheritDoc} */
    @Override
    protected void init() throws Exception {
        super.init();

        searchCache();

        preLoading();
    }

    /** {@inheritDoc} */
    @Override
    public boolean test(Map<Object, Object> map) throws Exception {
        if (nextBoolean()) {
            executeInTransaction();

            executeWithoutTransaction(true);
        }
        else
            executeWithoutTransaction(false);

        return true;
    }

    /**
     * @param withoutTransactionCache Without transaction cache.
     * @throws Exception If fail.
     */
    private void executeWithoutTransaction(boolean withoutTransactionCache) throws Exception {
        for (IgniteCache cache : availableCaches) {
            if (withoutTransactionCache && transactionalCaches.contains(cache))
                continue;

            executeRandomOperation(cache);
        }
    }

    /**
     * @param cache Ignite cache.
     * @throws Exception If fail.
     */
    private void executeRandomOperation(IgniteCache cache) throws Exception {
        switch (Operation.valueOf(nextRandom(operations))) {
            case PUT:
                doPut(cache);
                break;

            case PUTALL:
                doPutAll(cache);
                break;

            case GET:
                doGet(cache);
                break;

            case GETALL:
                doGetAll(cache);
                break;

            case INVOKE:
                doInvoke(cache);
                break;

            case INVOKEALL:
                doInvokeAll(cache);
                break;

            case REMOVE:
                doRemove(cache);
                break;

            case REMOVEALL:
                doRemoveAll(cache);
                break;

            case PUTIFABSENT:
                doPutIfAbsent(cache);
                break;

            case REPLACE:
                doReplace(cache);
                break;

            case SCANQUERY:
                doScanQuery(cache);
        }
    }

    /**
     * Execute operations in transaction.
     *
     * @throws Exception if fail.
     */
    private void executeInTransaction() throws Exception {
        IgniteBenchmarkUtils.doInTransaction(ignite().transactions(),
            TransactionConcurrency.fromOrdinal(nextRandom(TransactionConcurrency.values().length)),
            TransactionIsolation.fromOrdinal(nextRandom(TransactionIsolation.values().length)),
            new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    for (IgniteCache cache : transactionalCaches)
                        if (nextBoolean())
                            executeRandomOperation(cache);
                    return null;
                }
            });
    }

    /**
     * @param cache Ignite cache.
     * @throws Exception If failed.
     */
    private void doPut(IgniteCache cache) throws Exception {
        int i = nextRandom(args.range());

        cache.put(createRandomKey(i, cache.getName()), createRandomValue(i, cache.getName()));
    }

    /**
     * @param cache Ignite cache.
     * @throws Exception If failed.
     */
    private void doPutAll(IgniteCache cache) throws Exception {
        Map putMap = new TreeMap();
        Class keyCass = randomKeyClass(cache.getName());

        for (int cnt = 0; cnt < args.batch(); cnt++) {
            int i = nextRandom(args.range());

            putMap.put(ModelUtil.create(keyCass, i), createRandomValue(i, cache.getName()));
        }

        cache.putAll(putMap);
    }

    /**
     * @param cache Ignite cache.
     * @throws Exception If failed.
     */
    private void doGet(IgniteCache cache) throws Exception {
        int i = nextRandom(args.range());

        cache.get(createRandomKey(i, cache.getName()));
    }

    /**
     * @param cache Ignite cache.
     * @throws Exception If failed.
     */
    private void doGetAll(IgniteCache cache) throws Exception {
        Set keys = new TreeSet();
        Class keyClass = randomKeyClass(cache.getName());

        for (int cnt = 0; cnt < args.batch(); cnt++) {
            int i = nextRandom(args.range());

            keys.add(ModelUtil.create(keyClass, i));
        }

        cache.getAll(keys);
    }

    /**
     * @param cache Ignite cache.
     * @throws Exception If failed.
     */
    private void doInvoke(final IgniteCache cache) throws Exception {
        final int i = nextRandom(args.range());

        if (nextBoolean())
            cache.invoke(createRandomKey(i, cache.getName()), replaceValueEntryProcessor,
                createRandomValue(i + 1, cache.getName()));
        else
            cache.invoke(createRandomKey(i, cache.getName()), removeEntryProcessor);

    }

    /**
     * Entry processor for local benchmark replace value task.
     */
    private static class BenchmarkReplaceValueEntryProcessor implements EntryProcessor, Serializable {

        /**
         * New value for update during process by default.
         */
        private Object newValue;

        /**
         * @param newValue default new value
         */
        public BenchmarkReplaceValueEntryProcessor(Object newValue) {
            this.newValue = newValue;
        }

        @Override
        public Object process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
            Object newValue = arguments == null || arguments[0] == null ? this.newValue : arguments[0];
            Object oldValue = entry.getValue();
            entry.setValue(newValue);

            return oldValue;
        }
    }

    /**
     * Entry processor for local benchmark remove entry task.
     */
    private static class BenchmarkRemoveEntryProcessor implements EntryProcessor, Serializable {

        @Override
        public Object process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
            Object oldValue = entry.getValue();
            entry.remove();

            return oldValue;
        }
    }

    /**
     * @param cache Ignite cache.
     * @throws Exception If failed.
     */
    private void doInvokeAll(final IgniteCache cache) throws Exception {
        Map<Object, EntryProcessor> map = new TreeMap<>();
        Class keyClass = randomKeyClass(cache.getName());

        for (int cnt = 0; cnt < args.batch(); cnt++) {
            int i = nextRandom(args.range());
            Object key = ModelUtil.create(keyClass, i);

            if (nextBoolean())
                map.put(key,
                    new BenchmarkReplaceValueEntryProcessor(createRandomValue(i + 1, cache.getName())));
            else
                map.put(key, removeEntryProcessor);
        }

        cache.invokeAll(map);
    }

    /**
     * @param cache Ignite cache.
     * @throws Exception If failed.
     */
    private void doRemove(IgniteCache cache) throws Exception {
        int i = nextRandom(args.range());

        cache.remove(createRandomKey(i, cache.getName()));
    }

    /**
     * @param cache Ignite cache.
     * @throws Exception If failed.
     */
    private void doRemoveAll(IgniteCache cache) throws Exception {
        Set keys = new TreeSet();
        Class keyClass = randomKeyClass(cache.getName());

        for (int cnt = 0; cnt < args.batch(); cnt++) {
            int i = nextRandom(args.range());

            keys.add(ModelUtil.create(keyClass, i));
        }

        cache.removeAll(keys);
    }

    /**
     * @param cache Ignite cache.
     * @throws Exception If failed.
     */
    private void doPutIfAbsent(IgniteCache cache) throws Exception {
        int i = nextRandom(args.range());

        cache.putIfAbsent(createRandomKey(i, cache.getName()), createRandomValue(i, cache.getName()));
    }

    /**
     * @param cache Ignite cache.
     * @throws Exception If failed.
     */
    private void doReplace(IgniteCache cache) throws Exception {
        int i = nextRandom(args.range());

        cache.replace(createRandomKey(i, cache.getName()),
            createRandomValue(i, cache.getName()),
            createRandomValue(i + 1, cache.getName()));
    }

    /**
     * @param cache Ignite cache.
     * @throws Exception If failed.
     */
    private void doScanQuery(IgniteCache cache) throws Exception {

        cache.query(new ScanQuery(igniteBiPredicate)).getAll();
    }

    /**
     * Scan query predicate class.
     */
    private static class BenchmarkIgniteBiPredicate implements IgniteBiPredicate {

        /**
         * @param key Cache key.
         * @param val Cache value.
         * @return true If is hit.
         */
        @Override
        public boolean apply(Object key, Object val) {
            return val.hashCode() % 45 == 0;
        }
    }

    /**
     * @return Nex random boolean value.
     */
    protected boolean nextBoolean() {
        return ThreadLocalRandom.current().nextBoolean();
    }

    /**
     * Cache operation enum.
     */
    static enum Operation {
        /** Put operation. */
        PUT,

        /** Put all operation. */
        PUTALL,

        /** Get operation. */
        GET,

        /** Get all operation. */
        GETALL,

        /** Invoke operation. */
        INVOKE,

        /** Invoke all operation. */
        INVOKEALL,

        /** Remove operation. */
        REMOVE,

        /** Remove all operation. */
        REMOVEALL,

        /** Put if absent operation. */
        PUTIFABSENT,

        /** Replace operation. */
        REPLACE,

        /** Scan query operation. */
        SCANQUERY;

        /**
         * @param num Number of operation.
         * @return Operation.
         */
        public static Operation valueOf(int num) {
            return values()[num];
        }
    }
}
