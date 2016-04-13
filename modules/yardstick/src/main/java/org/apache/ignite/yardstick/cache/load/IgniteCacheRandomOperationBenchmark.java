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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheTypeMetadata;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
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

    /** Amount partitions. */
    public static final int SCAN_QUERY_PARTITIN_AMOUNT = 10;

    /** List off all available cache. */
    protected List<IgniteCache> availableCaches;

    /** List of available transactional cache. */
    protected List<IgniteCache> transactionalCaches;

/** List of affinity cache. */
    protected List<IgniteCache> affinityCaches;

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
    static BenchmarkIgniteBiPredicate igniteBiPredicate = new BenchmarkIgniteBiPredicate();

    /**
     * @throws Exception If failed.
     */
    private void searchCache() throws Exception {
        availableCaches = new ArrayList<>(ignite().cacheNames().size());
        transactionalCaches = new ArrayList<>();
        affinityCaches = new ArrayList<>();
        keysCacheClasses = new HashMap<>();
        valuesCacheClasses = new HashMap<>();
        replaceValueEntryProcessor = new BenchmarkReplaceValueEntryProcessor(null);
        removeEntryProcessor = new BenchmarkRemoveEntryProcessor();

        for (String name : ignite().cacheNames()) {
            IgniteCache<Object, Object> cache = ignite().cache(name);

            CacheConfiguration configuration = cache.getConfiguration(CacheConfiguration.class);

            if (isClassDefinedinConfig(configuration)) {
                if (configuration.getMemoryMode() == CacheMemoryMode.OFFHEAP_TIERED)
                    continue;

                ArrayList<Class> keys = new ArrayList<>();
                ArrayList<Class> values = new ArrayList<>();

                if (configuration.getQueryEntities() != null) {
                    Collection<QueryEntity> entries = configuration.getQueryEntities();

                    for (QueryEntity queryEntity : entries) {
                        if (queryEntity.getKeyType() != null) {
                            Class keyClass = getClass().forName(queryEntity.getKeyType());
                            if (ModelUtil.canCreateInstance(keyClass))
                                keys.add(keyClass);
                        }

                        if (queryEntity.getValueType() != null) {
                            Class valueClass = getClass().forName(queryEntity.getValueType());
                            if (ModelUtil.canCreateInstance(valueClass))
                                values.add(valueClass);
                        }
                    }
                }

                if (configuration.getTypeMetadata() != null) {
                    Collection<CacheTypeMetadata> entries = configuration.getTypeMetadata();

                    for (CacheTypeMetadata cacheTypeMetadata : entries) {

                        if (cacheTypeMetadata.getKeyType() != null) {
                            Class keyClass = getClass().forName(cacheTypeMetadata.getKeyType());
                            if (ModelUtil.canCreateInstance(keyClass))
                                keys.add(keyClass);
                        }

                        if (cacheTypeMetadata.getValueType() != null) {
                            Class valueClass = getClass().forName(cacheTypeMetadata.getValueType());
                            if (ModelUtil.canCreateInstance(valueClass))
                                values.add(valueClass);
                        }
                    }
                }

                if (keys.size() == 0 || values.size() == 0)
                    continue;

                keysCacheClasses.put(name, keys.toArray(new Class[] {}));
                valuesCacheClasses.put(name, values.toArray(new Class[] {}));
            }

            if (configuration.getCacheMode() != CacheMode.LOCAL)
                affinityCaches.add(cache);

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
        if (args.preloadAmount() > args.range())
            throw new IllegalArgumentException("Preloading amount (\"-pa\", \"--preloadAmount\") must by less then the" +
                " range (\"-r\", \"--range\").");

        Thread[] threads = new Thread[availableCaches.size()];

        for (int i = 0; i < availableCaches.size(); i++) {
            final String cacheName = availableCaches.get(i).getName();

            threads[i] = new Thread() {
                @Override public void run() {
                    try (IgniteDataStreamer dataLdr = ignite().dataStreamer(cacheName)) {
                        for (int i = 0; i < args.preloadAmount() && !isInterrupted(); i++)
                            dataLdr.addData(createRandomKey(i, cacheName), createRandomValue(i, cacheName));
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
     * Building a map that contains mapping of node ID to a list of partitions stored on the node.
     *
     * @param cacheName Name of Ignite cache.
     * @return Node to partitions map.
     */
    private Map<UUID, List<Integer>> personCachePartitions(String cacheName) {
        // Getting affinity for person cache.
        Affinity affinity = ignite().affinity(cacheName);

        // Building a list of all partitions numbers.
        List<Integer> randmPartitions = new ArrayList<>(10);

        if (affinity.partitions() <= SCAN_QUERY_PARTITIN_AMOUNT)
            for (int i = 0; i < affinity.partitions(); i++)
                randmPartitions.add(i);
        else {
            for (int i=0; i < SCAN_QUERY_PARTITIN_AMOUNT; i++) {
                int partitionNumber;

                do
                    partitionNumber = nextRandom(affinity.partitions());
                while (randmPartitions.contains(partitionNumber));

                randmPartitions.add(partitionNumber);
            }
        }

        Collections.sort(randmPartitions);

        // Getting partition to node mapping.
        Map<Integer, ClusterNode> partPerNodes = affinity.mapPartitionsToNodes(randmPartitions);

        // Building node to partitions mapping.
        Map<UUID, List<Integer>> nodesToPart = new HashMap<>();

        for (Map.Entry<Integer, ClusterNode> entry : partPerNodes.entrySet()) {
            List<Integer> nodeParts = nodesToPart.get(entry.getValue().id());

            if (nodeParts == null) {
                nodeParts = new ArrayList<>();
                nodesToPart.put(entry.getValue().id(), nodeParts);
            }

            nodeParts.add(entry.getKey());
        }

        return nodesToPart;
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

        if (keysCacheClasses.containsKey(cacheName))
            keys = keysCacheClasses.get(cacheName);
        else
            keys = ModelUtil.keyClasses();

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

        if (valuesCacheClasses.containsKey(cacheName))
            values = valuesCacheClasses.get(cacheName);
        else
            values = ModelUtil.valueClasses();

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
        if (!affinityCaches.contains(cache))
            return;
        Map<UUID, List<Integer>> partitionsMap = personCachePartitions(cache.getName());
        ScanQueryBroadcastClosure closure = new ScanQueryBroadcastClosure(cache.getName(), partitionsMap);
        ClusterGroup clusterGroup = ignite().cluster().forNodeIds(partitionsMap.keySet());
        IgniteCompute compute = ignite().compute(clusterGroup);
        compute.broadcast(closure);
    }

    /**
     * Closure for scan query executing.
     */
    private static class ScanQueryBroadcastClosure implements IgniteRunnable {

        /**
         * Ignite node.
         */
        @IgniteInstanceResource
        private Ignite node;

        /**
         * Information about partition.
         */
        private Map<UUID, List<Integer>> cachePartition;

        /**
         * Name of Ignite cache.
         */
        private String cacheName;

        /**
         * @param cacheName Name of Ignite cache.
         * @param cachePartition Partition by node for Ignite cache.
         */
        public ScanQueryBroadcastClosure(String cacheName, Map<UUID, List<Integer>> cachePartition) {
            this.cachePartition = cachePartition;
            this.cacheName = cacheName;
        }

        @Override
        public void run() {

            IgniteCache cache = node.cache(cacheName);

            // Getting a list of the partitions owned by this node.
            List<Integer> myPartitions = cachePartition.get(node.cluster().localNode().id());

            for (Integer part : myPartitions) {
                if (ThreadLocalRandom.current().nextBoolean())
                    continue;

                ScanQuery scanQuery = new ScanQuery();

                scanQuery.setPartition(part);
                scanQuery.setFilter(igniteBiPredicate);

                try (QueryCursor cursor = cache.query(scanQuery)) {
                    for (Object obj : cursor)
                        ;
                }

            }
        }
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
