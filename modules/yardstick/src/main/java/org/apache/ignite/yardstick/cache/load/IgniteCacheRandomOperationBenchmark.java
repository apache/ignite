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
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheTypeMetadata;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
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
import org.springframework.util.StringUtils;
import org.yardstickframework.BenchmarkConfiguration;

/**
 * Ignite cache random operation benchmark.
 */
public class IgniteCacheRandomOperationBenchmark extends IgniteAbstractBenchmark {
    /** */
    public static final int operations = Operation.values().length;

    /** Scan query predicate. */
    private static BenchmarkIgniteBiPredicate igniteBiPred = new BenchmarkIgniteBiPredicate();

    /** Amount partitions. */
    private static final int SCAN_QUERY_PARTITIN_AMOUNT = 10;

    /** List off all available cache. */
    private List<IgniteCache> availableCaches;

    /** List of available transactional cache. */
    private List<IgniteCache> txCaches;

    /** List of affinity cache. */
    private List<IgniteCache> affCaches;

    /** Map cache name on key classes. */
    private Map<String, Class[]> keysCacheClasses;

    /** Map cache name on value classes. */
    private Map<String, Class[]> valuesCacheClasses;

    /** List of query descriptors by cache names */
    private Map<String, List<SqlCacheDescriptor>> cacheSqlDescriptors;

    /**
     * Replace value entry processor.
     */
    private BenchmarkReplaceValueEntryProcessor replaceEntryProc;

    /**
     * Remove entry processor.
     */
    private BenchmarkRemoveEntryProcessor rmvEntryProc;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        searchCache();

        preLoading();
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> map) throws Exception {
        if (nextBoolean()) {
            executeInTransaction();

            executeOutOfTx(true);
        }
        else
            executeOutOfTx(false);

        return true;
    }

    /**
     * @throws Exception If failed.
     */
    private void searchCache() throws Exception {
        availableCaches = new ArrayList<>(ignite().cacheNames().size());
        txCaches = new ArrayList<>();
        affCaches = new ArrayList<>();
        keysCacheClasses = new HashMap<>();
        valuesCacheClasses = new HashMap<>();
        replaceEntryProc = new BenchmarkReplaceValueEntryProcessor(null);
        rmvEntryProc = new BenchmarkRemoveEntryProcessor();
        cacheSqlDescriptors = new HashMap<>();

        for (String cacheName : ignite().cacheNames()) {
            IgniteCache<Object, Object> cache = ignite().cache(cacheName);

            CacheConfiguration configuration = cache.getConfiguration(CacheConfiguration.class);

            if (isClassDefinedInConfig(configuration)) {
                if (configuration.getMemoryMode() == CacheMemoryMode.OFFHEAP_TIERED) {
                    throw new IgniteException("Off-heap mode is unsupported by the load test due to bugs IGNITE-2982" +
                        " and IGNITE-2997");
                }

                ArrayList<Class> keys = new ArrayList<>();
                ArrayList<Class> values = new ArrayList<>();

                if (configuration.getQueryEntities() != null) {
                    Collection<QueryEntity> entries = configuration.getQueryEntities();

                    for (QueryEntity queryEntity : entries) {
                        if (queryEntity.getKeyType() != null) {
                            Class keyCls = getClass().forName(queryEntity.getKeyType());

                            if (ModelUtil.canCreateInstance(keyCls))
                                keys.add(keyCls);
                            else
                                throw new IgniteException("Class is unknown for the load test. Make sure you " +
                                    "specified its full name [clsName=" + queryEntity.getKeyType() + ']');
                        }

                        if (queryEntity.getValueType() != null) {
                            Class valCls = getClass().forName(queryEntity.getValueType());

                            if (ModelUtil.canCreateInstance(valCls))
                                values.add(valCls);
                            else
                                throw new IgniteException("Class is unknown for the load test. Make sure you " +
                                    "specified its full name [clsName=" + queryEntity.getKeyType() + ']');

                            cofigureCacheSqlDescriptor(cacheName, queryEntity, valCls);
                        }
                    }
                }

                if (configuration.getTypeMetadata() != null) {
                    Collection<CacheTypeMetadata> entries = configuration.getTypeMetadata();

                    for (CacheTypeMetadata cacheTypeMetadata : entries) {
                        if (cacheTypeMetadata.getKeyType() != null) {
                            Class keyCls = getClass().forName(cacheTypeMetadata.getKeyType());

                            if (ModelUtil.canCreateInstance(keyCls))
                                keys.add(keyCls);
                            else
                                throw new IgniteException("Class is unknown for the load test. Make sure you " +
                                    "specified its full name [clsName=" + cacheTypeMetadata.getKeyType() + ']');
                        }

                        if (cacheTypeMetadata.getValueType() != null) {
                            Class valCls = getClass().forName(cacheTypeMetadata.getValueType());

                            if (ModelUtil.canCreateInstance(valCls))
                                values.add(valCls);
                            else
                                throw new IgniteException("Class is unknown for the load test. Make sure you " +
                                    "specified its full name [clsName=" + cacheTypeMetadata.getKeyType() + ']');
                        }
                    }
                }

                if (keys.size() == 0 || values.size() == 0)
                    continue;

                keysCacheClasses.put(cacheName, keys.toArray(new Class[] {}));
                valuesCacheClasses.put(cacheName, values.toArray(new Class[] {}));
            }
            else
                keysCacheClasses.put(cacheName,
                    new Class[] {randomKeyClass(cacheName)});

            if (configuration.getCacheMode() != CacheMode.LOCAL)
                affCaches.add(cache);

            if (configuration.getAtomicityMode() == CacheAtomicityMode.TRANSACTIONAL)
                txCaches.add(cache);

            availableCaches.add(cache);
        }
    }

    /**
     * @param cacheName Ignite cache name.
     * @param queryEntity Query entry.
     * @param valCls Class of value.
     * @throws ClassNotFoundException
     */
    private void cofigureCacheSqlDescriptor(String cacheName, QueryEntity queryEntity, Class valCls)
        throws ClassNotFoundException {
        List<SqlCacheDescriptor> descs = cacheSqlDescriptors.get(cacheName);

        if (descs == null) {
            descs = new ArrayList<>();

            cacheSqlDescriptors.put(cacheName, descs);
        }

        Map<String, Class> indexedFields = new HashMap<>();

        for (QueryIndex index : queryEntity.getIndexes()) {
            for (String iField : index.getFieldNames()) {
                indexedFields.put(iField,
                    getClass().forName(queryEntity.getFields().get(iField)));
            }
        }

        descs.add(new SqlCacheDescriptor(valCls, queryEntity.getFields().keySet(),
            indexedFields));
    }

    /**
     * @param configuration Ignite cache configuration.
     * @return True if defined.
     */
    private boolean isClassDefinedInConfig(CacheConfiguration configuration) {
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

        for (Thread thread : threads)
            thread.join();
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
            for (int i = 0; i < SCAN_QUERY_PARTITIN_AMOUNT; i++) {
                int partNum;

                do
                    partNum = nextRandom(affinity.partitions());
                while (randmPartitions.contains(partNum));

                randmPartitions.add(partNum);
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

    /**
     * @param withoutTransactionCache Without transaction cache.
     * @throws Exception If fail.
     */
    private void executeOutOfTx(boolean withoutTransactionCache) throws Exception {
        for (IgniteCache cache : availableCaches) {
            if (withoutTransactionCache && txCaches.contains(cache))
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

            case PUT_ALL:
                doPutAll(cache);
                break;

            case GET:
                doGet(cache);
                break;

            case GET_ALL:
                doGetAll(cache);
                break;

            case INVOKE:
                doInvoke(cache);
                break;

            case INVOKE_ALL:
                doInvokeAll(cache);
                break;

            case REMOVE:
                doRemove(cache);
                break;

            case REMOVE_ALL:
                doRemoveAll(cache);
                break;

            case PUT_IF_ABSENT:
                doPutIfAbsent(cache);
                break;

            case REPLACE:
                doReplace(cache);
                break;

            case SCAN_QUERY:
                doScanQuery(cache);
                break;

            case SQL_QUERY:
                doSqlQuery(cache);
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
                    for (IgniteCache cache : txCaches)
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

        Class keyCls = randomKeyClass(cache.getName());

        for (int cnt = 0; cnt < args.batch(); cnt++) {
            int i = nextRandom(args.range());

            keys.add(ModelUtil.create(keyCls, i));
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
            cache.invoke(createRandomKey(i, cache.getName()), replaceEntryProc,
                createRandomValue(i + 1, cache.getName()));
        else
            cache.invoke(createRandomKey(i, cache.getName()), rmvEntryProc);

    }

    /**
     * Entry processor for local benchmark replace value task.
     */
    private static class BenchmarkReplaceValueEntryProcessor implements EntryProcessor, Serializable {
        /**
         * New value for update during process by default.
         */
        private Object newVal;

        /**
         * @param newVal default new value
         */
        public BenchmarkReplaceValueEntryProcessor(Object newVal) {
            this.newVal = newVal;
        }

        /** {@inheritDoc} */
        @Override public Object process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
            Object newVal = arguments == null || arguments[0] == null ? this.newVal : arguments[0];
            Object oldVal = entry.getValue();
            entry.setValue(newVal);

            return oldVal;
        }
    }

    /**
     * Entry processor for local benchmark remove entry task.
     */
    private static class BenchmarkRemoveEntryProcessor implements EntryProcessor, Serializable {
        /** {@inheritDoc} */
        @Override public Object process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
            Object oldVal = entry.getValue();

            entry.remove();

            return oldVal;
        }
    }

    /**
     * @param cache Ignite cache.
     * @throws Exception If failed.
     */
    private void doInvokeAll(final IgniteCache cache) throws Exception {
        Map<Object, EntryProcessor> map = new TreeMap<>();

        Class keyCls = randomKeyClass(cache.getName());

        for (int cnt = 0; cnt < args.batch(); cnt++) {
            int i = nextRandom(args.range());
            Object key = ModelUtil.create(keyCls, i);

            if (nextBoolean())
                map.put(key,
                    new BenchmarkReplaceValueEntryProcessor(createRandomValue(i + 1, cache.getName())));
            else
                map.put(key, rmvEntryProc);
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
        Class keyCls = randomKeyClass(cache.getName());

        for (int cnt = 0; cnt < args.batch(); cnt++) {
            int i = nextRandom(args.range());

            keys.add(ModelUtil.create(keyCls, i));
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
        if (!affCaches.contains(cache))
            return;

        Map<UUID, List<Integer>> partitionsMap = personCachePartitions(cache.getName());

        ScanQueryBroadcastClosure c = new ScanQueryBroadcastClosure(cache.getName(), partitionsMap);

        ClusterGroup clusterGrp = ignite().cluster().forNodeIds(partitionsMap.keySet());

        IgniteCompute compute = ignite().compute(clusterGrp);

        compute.broadcast(c);
    }

    /**
     * @param cache Ignite cache.
     * @throws Exception If failed.
     */
    private void doSqlQuery(IgniteCache cache) throws Exception {
        List<SqlCacheDescriptor> descriptors = cacheSqlDescriptors.get(cache.getName());

        if (descriptors != null && !descriptors.isEmpty()) {
            SqlCacheDescriptor randomDescriptor = descriptors.get(nextRandom(descriptors.size()));

            int id = nextRandom(args.range());

            Query sq = nextBoolean() ? randomDescriptor.getSqlQuery(id) : randomDescriptor.getSqlFieldsQuery(id);

            try (QueryCursor cursor = cache.query(sq)) {
                for (Object obj : cursor)
                    ;
            }
        }
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
        private Map<UUID, List<Integer>> cachePart;

        /**
         * Name of Ignite cache.
         */
        private String cacheName;

        /**
         * @param cacheName Name of Ignite cache.
         * @param cachePart Partition by node for Ignite cache.
         */
        public ScanQueryBroadcastClosure(String cacheName, Map<UUID, List<Integer>> cachePart) {
            this.cachePart = cachePart;
            this.cacheName = cacheName;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            IgniteCache cache = node.cache(cacheName);

            // Getting a list of the partitions owned by this node.
            List<Integer> myPartitions = cachePart.get(node.cluster().localNode().id());

            for (Integer part : myPartitions) {
                if (ThreadLocalRandom.current().nextBoolean())
                    continue;

                ScanQuery scanQry = new ScanQuery();

                scanQry.setPartition(part);
                scanQry.setFilter(igniteBiPred);

                try (QueryCursor cursor = cache.query(scanQry)) {
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
        @Override public boolean apply(Object key, Object val) {
            return val.hashCode() % 45 == 0;
        }
    }

    /**
     * Query descriptor.
     */
    private static class SqlCacheDescriptor {

        /**
         * Class of value.
         */
        private Class valueClass;

        /**
         * Select fields.
         */
        private Set<String> fields;

        /**
         * Indexed fields.
         */
        private Map<String, Class> indexedFieldsByCls;

        /**
         * @param valueClass Class of value.
         * @param fields All select fields.
         * @param indexedFieldsByClass Indexed fields.
         */
        public SqlCacheDescriptor(Class valueClass, Set<String> fields,
            Map<String, Class> indexedFieldsByClass) {
            this.valueClass = valueClass;
            this.fields = fields;
            this.indexedFieldsByCls = indexedFieldsByClass;
        }

        /**
         * @param id Query id.
         * @return Condition string.
         */
        private String makeQuerySelect(int id) {
            return StringUtils.collectionToDelimitedString(fields, ", ");
        }

        /**
         * @param id Query id.
         * @return Condition string.
         */
        private String makeQueryCondition(int id) {
            StringBuffer sb = new StringBuffer();

            for (String iField : indexedFieldsByCls.keySet()) {
                Class cl = indexedFieldsByCls.get(iField);

                if (!Number.class.isAssignableFrom(cl) && !String.class.equals(cl))
                    continue;

                if (sb.length() != 0) {
                    switch (id % 3 % 2) {
                        case 0:
                            sb.append(" OR ");
                            break;
                        case 1:
                            sb.append(" AND ");
                            break;
                    }
                }

                if (Number.class.isAssignableFrom(cl)) {
                    sb.append(iField);
                    switch (id % 2) {
                        case 0:
                            sb.append(" > ");
                            break;
                        case 1:
                            sb.append(" < ");
                            break;
                    }
                    sb.append(id);
                }
                else if (String.class.equals(cl))
                    sb.append("lower(").append(iField).append(") LIKE lower('%").append(id).append("%')");

            }
            return sb.toString();
        }

        /**
         * @param id Query id.
         * @return
         */
        public SqlQuery getSqlQuery(int id) {
            return new SqlQuery(valueClass, makeQueryCondition(id));
        }

        /**
         * @param id Query id.
         * @return
         */
        public SqlFieldsQuery getSqlFieldsQuery(int id) {
            return new SqlFieldsQuery(String.format("SELECT %s FROM %s WHERE %s",
                makeQuerySelect(id), valueClass.getSimpleName(), makeQueryCondition(id)));
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
        PUT_ALL,

        /** Get operation. */
        GET,

        /** Get all operation. */
        GET_ALL,

        /** Invoke operation. */
        INVOKE,

        /** Invoke all operation. */
        INVOKE_ALL,

        /** Remove operation. */
        REMOVE,

        /** Remove all operation. */
        REMOVE_ALL,

        /** Put if absent operation. */
        PUT_IF_ABSENT,

        /** Replace operation. */
        REPLACE,

        /** Scan query operation. */
        SCAN_QUERY,

        /** SQL query operation. */
        SQL_QUERY;

        /**
         * @param num Number of operation.
         * @return Operation.
         */
        public static Operation valueOf(int num) {
            return values()[num];
        }
    }
}
