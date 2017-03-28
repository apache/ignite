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

import java.io.BufferedReader;
import java.io.FileReader;
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
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.CacheException;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheTypeMetadata;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.apache.ignite.yardstick.IgniteBenchmarkUtils;
import org.apache.ignite.yardstick.IgniteNode;
import org.apache.ignite.yardstick.cache.load.model.ModelUtil;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.BeansException;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

/**
 * Ignite cache random operation benchmark.
 */
public class IgniteCacheRandomOperationBenchmark extends IgniteAbstractBenchmark {
    /** */
    public static final int operations = Operation.values().length;

    /***/
    public static final int CONTINUOUS_QUERY_PER_CACHE = 3;

    /** Scan query predicate. */
    private static BenchmarkIgniteBiPredicate igniteBiPred = new BenchmarkIgniteBiPredicate();

    /** Amount partitions. */
    private static final int SCAN_QUERY_PARTITION_AMOUNT = 10;

    /** List off all available cache. */
    private List<IgniteCache<Object, Object>> availableCaches;

    /** List of available transactional cache. */
    private List<IgniteCache<Object, Object>> txCaches;

    /** List of affinity cache. */
    private List<IgniteCache<Object, Object>> affCaches;

    /** Map cache name on key classes. */
    private Map<String, Class[]> keysCacheClasses;

    /** Map cache name on value classes. */
    private Map<String, Class[]> valuesCacheClasses;

    /** List of query descriptors by cache names. */
    private Map<String, List<SqlCacheDescriptor>> cacheSqlDescriptors;

    /** List of SQL queries. */
    private List<TestQuery> queries;

    /** List of allowed cache operations which will be executed. */
    private List<Operation> allowedLoadTestOps;

    /**
     * Replace value entry processor.
     */
    private BenchmarkReplaceValueEntryProcessor replaceEntryProc;

    /**
     * Remove entry processor.
     */
    private BenchmarkRemoveEntryProcessor rmvEntryProc;

    /**
     * Map of statistic information.
     */
    private Map<String, AtomicLong> operationStatistics;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        if (args.additionalCachesNumber() > 0)
            createAdditionalCaches();

        searchCache();

        preLoading();
    }

    /**
     * Creates additional caches.
     *
     * @throws Exception If failed.
     */
    private void createAdditionalCaches() throws Exception {
        Map<String, CacheConfiguration> cfgMap;

        try {
            // Loading spring application context and getting all of the caches configurations in the map.
            cfgMap = IgniteNode.loadConfiguration(args.configuration()).get2().getBeansOfType(CacheConfiguration.class);
        }
        catch (BeansException e) {
            throw new Exception("Failed to instantiate bean [type=" + CacheConfiguration.class + ", err=" +
                e.getMessage() + ']', e);
        }

        if (cfgMap == null || cfgMap.isEmpty())
            throw new Exception("Failed to find cache configurations in: " + args.configuration());

        // Getting cache configuration from the map using name specified in property file.
        CacheConfiguration<Object, Object> ccfg = cfgMap.get(args.additionalCachesName());

        if (ccfg == null)
            throw new Exception("Failed to find cache configuration [cache=" + args.additionalCachesName() +
                ", cfg=" + args.configuration() + ']');

        for (int i = 0; i < args.additionalCachesNumber(); i++) {
            CacheConfiguration<Object, Object> newCfg = new CacheConfiguration<>(ccfg);

            newCfg.setName("additional_" + args.additionalCachesName() + "_cache_" + i);

            try {
                ignite().createCache(newCfg);
            }
            catch (CacheException e) {
                BenchmarkUtils.error("Failed to create additional cache [ name = " + args.additionalCachesName() +
                    ", err" + e.getMessage() + ']', e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onException(Throwable e) {
        BenchmarkUtils.error("The benchmark of random operation failed.", e);

        super.onException(e);
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> map) throws Exception {
        if (nextBoolean()) {
            executeInTransaction(map);

            executeOutOfTx(map, true);
        }
        else
            executeOutOfTx(map, false);

        return true;
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        try {
            BenchmarkUtils.println("Benchmark statistics");

            for (String cacheName : ignite().cacheNames()) {
                BenchmarkUtils.println(String.format("Operations over cache '%s'", cacheName));

                for (Operation op : Operation.values()) {
                    BenchmarkUtils.println(cfg, String.format("%s: %s", op,
                        operationStatistics.get(op + "_" + cacheName)));
                }
            }
        }
        finally {
            super.tearDown();
        }
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
        operationStatistics = new HashMap<>();

        loadQueries();

        loadAllowedOperations();

        for (String cacheName : ignite().cacheNames()) {
            IgniteCache<Object, Object> cache = ignite().cache(cacheName);

            for (Operation op : Operation.values())
                operationStatistics.put(op + "_" + cacheName, new AtomicLong(0));

            CacheConfiguration configuration = cache.getConfiguration(CacheConfiguration.class);

            if (isClassDefinedInConfig(configuration)) {
                if (configuration.getMemoryMode() == CacheMemoryMode.OFFHEAP_TIERED &&
                    configuration.getQueryEntities().size() > 2) {
                    throw new IgniteException("Off-heap mode is unsupported by the load test due to bugs IGNITE-2982" +
                        " and IGNITE-2997");
                }

                ArrayList<Class> keys = new ArrayList<>();
                ArrayList<Class> values = new ArrayList<>();

                if (configuration.getQueryEntities() != null) {
                    Collection<QueryEntity> entries = configuration.getQueryEntities();

                    for (QueryEntity queryEntity : entries) {
                        try {
                            if (queryEntity.getKeyType() != null) {
                                Class keyCls = Class.forName(queryEntity.getKeyType());

                                if (ModelUtil.canCreateInstance(keyCls))
                                    keys.add(keyCls);
                                else
                                    throw new IgniteException("Class is unknown for the load test. Make sure you " +
                                        "specified its full name [clsName=" + queryEntity.getKeyType() + ']');
                            }

                            if (queryEntity.getValueType() != null) {
                                Class valCls = Class.forName(queryEntity.getValueType());

                                if (ModelUtil.canCreateInstance(valCls))
                                    values.add(valCls);
                                else
                                    throw new IgniteException("Class is unknown for the load test. Make sure you " +
                                        "specified its full name [clsName=" + queryEntity.getKeyType() + ']');

                                configureCacheSqlDescriptor(cacheName, queryEntity, valCls);
                            }
                        }
                        catch (ClassNotFoundException e) {
                            BenchmarkUtils.println(e.getMessage());
                            BenchmarkUtils.println("This can be a BinaryObject. Ignoring exception.");

                            if (!cacheSqlDescriptors.containsKey(cacheName))
                                cacheSqlDescriptors.put(cacheName, new ArrayList<SqlCacheDescriptor>());
                        }
                    }
                }

                if (configuration.getTypeMetadata() != null) {
                    Collection<CacheTypeMetadata> entries = configuration.getTypeMetadata();

                    for (CacheTypeMetadata cacheTypeMetadata : entries) {
                        try {
                            if (cacheTypeMetadata.getKeyType() != null) {
                                Class keyCls = Class.forName(cacheTypeMetadata.getKeyType());

                                if (ModelUtil.canCreateInstance(keyCls))
                                    keys.add(keyCls);
                                else
                                    throw new IgniteException("Class is unknown for the load test. Make sure you " +
                                        "specified its full name [clsName=" + cacheTypeMetadata.getKeyType() + ']');
                            }

                            if (cacheTypeMetadata.getValueType() != null) {
                                Class valCls = Class.forName(cacheTypeMetadata.getValueType());

                                if (ModelUtil.canCreateInstance(valCls))
                                    values.add(valCls);
                                else
                                    throw new IgniteException("Class is unknown for the load test. Make sure you " +
                                        "specified its full name [clsName=" + cacheTypeMetadata.getKeyType() + ']');
                            }
                        }
                        catch (ClassNotFoundException e) {
                            BenchmarkUtils.println(e.getMessage());
                            BenchmarkUtils.println("This can be a BinaryObject. Ignoring exception.");

                            if (!cacheSqlDescriptors.containsKey(cacheName))
                                cacheSqlDescriptors.put(cacheName, new ArrayList<SqlCacheDescriptor>());
                        }
                    }
                }

                keysCacheClasses.put(cacheName, keys.toArray(new Class[] {}));

                valuesCacheClasses.put(cacheName, values.toArray(new Class[] {}));
            }
            else
                keysCacheClasses.put(cacheName, new Class[] {randomKeyClass(cacheName)});

            valuesCacheClasses.put(cacheName, determineValueClasses(cacheName));

            if (configuration.getCacheMode() != CacheMode.LOCAL)
                affCaches.add(cache);

            if (configuration.getAtomicityMode() == CacheAtomicityMode.TRANSACTIONAL)
                txCaches.add(cache);

            availableCaches.add(cache);
        }
    }

    /**
     * Load allowed operation from parameters.
     */
    private void loadAllowedOperations() {
        allowedLoadTestOps = new ArrayList<>();

        if (args.allowedLoadTestOps().isEmpty())
            Collections.addAll(allowedLoadTestOps, Operation.values());
        else {
            for (String opName : args.allowedLoadTestOps())
                allowedLoadTestOps.add(Operation.valueOf(opName.toUpperCase()));
        }
    }

    /**
     * Load query from file.
     *
     * @throws Exception If fail.
     */
    private void loadQueries() throws Exception {
        queries = new ArrayList<>();

        if (args.loadTestQueriesFile() != null) {
            try (FileReader fr = new FileReader(args.loadTestQueriesFile())) {
                try (BufferedReader br = new BufferedReader(fr)) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (line.trim().isEmpty())
                            continue;

                        boolean distributedJoins = false;

                        int commentIdx = line.lastIndexOf('#');

                        if (commentIdx >= 0) {
                            if (line.toUpperCase().indexOf("DISTRIBUTEDJOINS", commentIdx) > 0)
                                distributedJoins = true;

                            line = line.substring(0, commentIdx);
                        }

                        line = line.trim();

                        TestQuery qry = new TestQuery(line, distributedJoins);

                        queries.add(qry);

                        BenchmarkUtils.println("Loaded query: " + qry);
                    }
                }
            }
        }
    }

    /**
     * @param cacheName Ignite cache name.
     * @param qryEntity Query entry.
     * @param valCls Class of value.
     * @throws ClassNotFoundException If fail.
     */
    private void configureCacheSqlDescriptor(String cacheName, QueryEntity qryEntity, Class valCls)
        throws ClassNotFoundException {
        List<SqlCacheDescriptor> descs = cacheSqlDescriptors.get(cacheName);

        if (descs == null) {
            descs = new ArrayList<>();

            cacheSqlDescriptors.put(cacheName, descs);
        }

        Map<String, Class> indexedFields = new HashMap<>();

        for (QueryIndex index : qryEntity.getIndexes()) {
            for (String iField : index.getFieldNames()) {
                indexedFields.put(iField,
                    Class.forName(qryEntity.getFields().get(iField)));
            }
        }

        descs.add(new SqlCacheDescriptor(valCls, qryEntity.getFields().keySet(),
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
            throw new IllegalArgumentException("Preloading amount (\"-pa\", \"--preloadAmount\") " +
                "must by less then the range (\"-r\", \"--range\").");

        startPreloadLogging(args.preloadLogsInterval());

        Thread[] threads = new Thread[availableCaches.size()];

        for (int i = 0; i < availableCaches.size(); i++) {
            final String cacheName = availableCaches.get(i).getName();

            threads[i] = new Thread() {
                @Override public void run() {
                    try (IgniteDataStreamer<Object, Object> dataLdr = ignite().dataStreamer(cacheName)) {
                        for (int i = 0; i < args.preloadAmount() && !isInterrupted(); i++)
                            dataLdr.addData(createRandomKey(i, cacheName), createRandomValue(i, cacheName));
                    }
                }
            };

            threads[i].start();
        }

        for (Thread thread : threads)
            thread.join();

        stopPreloadLogging();
    }

    /**
     * Building a map that contains mapping of node ID to a list of partitions stored on the node.
     *
     * @param cacheName Name of Ignite cache.
     * @return Node to partitions map.
     */
    private Map<UUID, List<Integer>> personCachePartitions(String cacheName) {
        // Getting affinity for person cache.
        Affinity<Object> affinity = ignite().affinity(cacheName);

        // Building a list of all partitions numbers.
        List<Integer> rndParts = new ArrayList<>(10);

        if (affinity.partitions() <= SCAN_QUERY_PARTITION_AMOUNT)
            for (int i = 0; i < affinity.partitions(); i++)
                rndParts.add(i);
        else {
            for (int i = 0; i < SCAN_QUERY_PARTITION_AMOUNT; i++) {
                int partNum;

                do
                    partNum = nextRandom(affinity.partitions());
                while (rndParts.contains(partNum));

                rndParts.add(partNum);
            }
        }

        Collections.sort(rndParts);

        // Getting partition to node mapping.
        Map<Integer, ClusterNode> partPerNodes = affinity.mapPartitionsToNodes(rndParts);

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
        Class[] keys = keysCacheClasses.containsKey(cacheName)
            ? keysCacheClasses.get(cacheName) : ModelUtil.keyClasses();

        return keys[nextRandom(keys.length)];
    }

    /**
     * @param cacheName Cache name.
     * @return Set classes for cache.
     */
    private Class[] determineValueClasses(@NotNull String cacheName) {
        return cacheName.toLowerCase().contains("fat-values") ? ModelUtil.fatValueClasses() :
            ModelUtil.simpleValueClasses();
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
        Class[] values = valuesCacheClasses.containsKey(cacheName)
            ? valuesCacheClasses.get(cacheName) : ModelUtil.valueClasses();

        return values[nextRandom(values.length)];
    }

    /**
     * @param map Parameters map.
     * @param withoutTransactionCache Without transaction cache.
     * @throws Exception If fail.
     */
    private void executeOutOfTx(Map<Object, Object> map, boolean withoutTransactionCache) throws Exception {
        for (IgniteCache<Object, Object> cache : availableCaches) {
            if (withoutTransactionCache && txCaches.contains(cache))
                continue;

            executeRandomOperation(map, cache);
        }
    }

    /**
     * @param map Parameters map.
     * @param cache Ignite cache.
     * @throws Exception If fail.
     */
    private void executeRandomOperation(Map<Object, Object> map, IgniteCache<Object, Object> cache) throws Exception {
        Operation op = nextRandomOperation();

        switch (op) {
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
                break;

            case CONTINUOUS_QUERY:
                doContinuousQuery(cache, map);
        }
        storeStatistics(cache.getName(), map, op);
    }

    /**
     * @return Operation.
     */
    @NotNull private Operation nextRandomOperation() {
        return allowedLoadTestOps.get(nextRandom(allowedLoadTestOps.size()));
    }

    /**
     * @param cacheName Ignite cache name.
     * @param map Parameters map.
     * @param op Operation.
     */
    private void storeStatistics(String cacheName, Map<Object, Object> map, Operation op) {
        String opCacheKey = op + "_" + cacheName;

        Long opAmount = (Long)map.get("amount");

        opAmount = opAmount == null ? 1L : opAmount + 1;

        Integer opCacheCnt = (Integer)map.get(opCacheKey);

        opCacheCnt = opCacheCnt == null ? 1 : opCacheCnt + 1;

        if (opAmount % 100 == 0)
            updateStat(map);
        else
            map.put(opCacheKey, opCacheCnt);

        map.put("amount", opAmount);
    }

    /**
     * @param map Parameters map.
     */
    private void updateStat(Map<Object, Object> map) {
        for (Operation op : Operation.values())
            for (String cacheName : ignite().cacheNames()) {
                String opCacheKey = op + "_" + cacheName;

                Integer val = (Integer)map.get(opCacheKey);

                if (val != null) {
                    operationStatistics.get(opCacheKey).addAndGet(val.longValue());

                    map.put(opCacheKey, 0);
                }

            }
    }

    /**
     * Execute operations in transaction.
     *
     * @param map Parameters map.
     * @throws Exception if fail.
     */
    private void executeInTransaction(final Map<Object, Object> map) throws Exception {
        IgniteBenchmarkUtils.doInTransaction(ignite().transactions(),
            TransactionConcurrency.fromOrdinal(nextRandom(TransactionConcurrency.values().length)),
            TransactionIsolation.fromOrdinal(nextRandom(TransactionIsolation.values().length)),

            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    for (IgniteCache<Object, Object> cache : txCaches)
                        if (nextBoolean())
                            executeRandomOperation(map, cache);

                    return null;
                }
            });
    }

    /**
     * @param cache Ignite cache.
     * @throws Exception If failed.
     */
    private void doPut(IgniteCache<Object, Object> cache) throws Exception {
        int i = nextRandom(args.range());

        cache.put(createRandomKey(i, cache.getName()), createRandomValue(i, cache.getName()));
    }

    /**
     * @param cache Ignite cache.
     * @throws Exception If failed.
     */
    private void doPutAll(IgniteCache<Object, Object> cache) throws Exception {
        Map<Object, Object> putMap = new TreeMap<>();

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
    private void doGet(IgniteCache<Object, Object> cache) throws Exception {
        int i = nextRandom(args.range());

        cache.get(createRandomKey(i, cache.getName()));
    }

    /**
     * @param cache Ignite cache.
     * @throws Exception If failed.
     */
    private void doGetAll(IgniteCache<Object, Object> cache) throws Exception {
        Set<Object> keys = new TreeSet<>();

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
    private void doInvoke(final IgniteCache<Object, Object> cache) throws Exception {
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
    private static class BenchmarkReplaceValueEntryProcessor implements EntryProcessor<Object, Object, Object>, Serializable {
        /**
         * New value for update during process by default.
         */
        private Object newVal;

        /**
         * @param newVal default new value
         */
        private BenchmarkReplaceValueEntryProcessor(Object newVal) {
            this.newVal = newVal;
        }

        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Object, Object> entry, Object... arguments) {
            Object newVal = arguments == null || arguments[0] == null ? this.newVal : arguments[0];

            Object oldVal = entry.getValue();

            entry.setValue(newVal);

            return oldVal;
        }
    }

    /**
     * Entry processor for local benchmark remove entry task.
     */
    private static class BenchmarkRemoveEntryProcessor implements EntryProcessor<Object, Object, Object>, Serializable {
        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Object, Object> entry, Object... arguments) {
            Object oldVal = entry.getValue();

            entry.remove();

            return oldVal;
        }
    }

    /**
     * @param cache Ignite cache.
     * @throws Exception If failed.
     */
    private void doInvokeAll(IgniteCache<Object, Object> cache) throws Exception {
        Map<Object, EntryProcessor<Object, Object, Object>> map = new TreeMap<>();

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
    private void doRemove(IgniteCache<Object, Object> cache) throws Exception {
        int i = nextRandom(args.range());

        cache.remove(createRandomKey(i, cache.getName()));
    }

    /**
     * @param cache Ignite cache.
     * @throws Exception If failed.
     */
    private void doRemoveAll(IgniteCache<Object, Object> cache) throws Exception {
        Set<Object> keys = new TreeSet<>();

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
    private void doPutIfAbsent(IgniteCache<Object, Object> cache) throws Exception {
        int i = nextRandom(args.range());

        cache.putIfAbsent(createRandomKey(i, cache.getName()), createRandomValue(i, cache.getName()));
    }

    /**
     * @param cache Ignite cache.
     * @throws Exception If failed.
     */
    private void doReplace(IgniteCache<Object, Object> cache) throws Exception {
        int i = nextRandom(args.range());

        cache.replace(createRandomKey(i, cache.getName()),
            createRandomValue(i, cache.getName()),
            createRandomValue(i + 1, cache.getName()));
    }

    /**
     * @param cache Ignite cache.
     * @throws Exception If failed.
     */
    private void doScanQuery(IgniteCache<Object, Object> cache) throws Exception {
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
    private void doSqlQuery(IgniteCache<Object, Object> cache) throws Exception {
        List<SqlCacheDescriptor> descriptors = cacheSqlDescriptors.get(cache.getName());

        if (descriptors != null) {
            Query sq = null;

            if (queries.isEmpty()) {
                if (!descriptors.isEmpty()) {
                    SqlCacheDescriptor randomDesc = descriptors.get(nextRandom(descriptors.size()));

                    int id = nextRandom(args.range());

                    sq = nextBoolean() ? randomDesc.getSqlQuery(id) : randomDesc.getSqlFieldsQuery(id);
                }
            }
            else {
                TestQuery qry = queries.get(nextRandom(queries.size()));

                String sql = randomizeSql(qry.sql);

                sq = new SqlFieldsQuery(sql);

                ((SqlFieldsQuery)sq).setDistributedJoins(qry.distributedJoin);
            }

            if (sq != null)
                try (QueryCursor cursor = cache.query(sq)) {
                    for (Object obj : cursor) {
                        // No-op.
                    }
                }
        }
    }

    /**
     * @return SQL string.
     */
    private String randomizeSql(String sql) {
        int cnt = StringUtils.countOccurrencesOf(sql, "%s");

        Integer[] sub = new Integer[cnt];

        for (int i = 0; i < cnt; i++)
            sub[i] = nextRandom(args.range());

        sql = String.format(sql, sub);

        return sql;
    }

    /**
     * @param cache Ignite cache.
     * @param map Parameters map.
     * @throws Exception If failed.
     */
    private void doContinuousQuery(IgniteCache<Object, Object> cache, Map<Object, Object> map) throws Exception {
        List<QueryCursor> cursors = (ArrayList<QueryCursor>)map.get(cache.getName());

        if (cursors == null) {
            cursors = new ArrayList<>(CONTINUOUS_QUERY_PER_CACHE);
            map.put(cache.getName(), cursors);
        }

        if (cursors.size() == CONTINUOUS_QUERY_PER_CACHE) {
            QueryCursor cursor = cursors.get(nextRandom(cursors.size()));
            cursor.close();
            cursors.remove(cursor);
        }

        ContinuousQuery qry = new ContinuousQuery();

        qry.setLocalListener(new ContinuousQueryUpdater());

        qry.setRemoteFilterFactory(FactoryBuilder.factoryOf(new ContinuousQueryFilter()));

        cursors.add(cache.query(qry));
    }

    /**
     * @return Nex random boolean value.
     */
    private boolean nextBoolean() {
        return ThreadLocalRandom.current().nextBoolean();
    }

    /**
     * Continuous query updater class.
     */
    private static class ContinuousQueryUpdater implements CacheEntryUpdatedListener, Serializable {

        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable iterable) throws CacheEntryListenerException {
            for (Object o : iterable) {
                // No-op.
            }
        }
    }

    /**
     * Continuous query filter class.
     */
    private static class ContinuousQueryFilter implements CacheEntryEventSerializableFilter, Serializable {
        /** */
        private boolean flag = true;

        /** {@inheritDoc} */
        @Override public boolean evaluate(CacheEntryEvent evt) throws CacheEntryListenerException {
            return flag = !flag;
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
        private ScanQueryBroadcastClosure(String cacheName, Map<UUID, List<Integer>> cachePart) {
            this.cachePart = cachePart;
            this.cacheName = cacheName;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            IgniteCache cache = node.cache(cacheName);

            // Getting a list of the partitions owned by this node.
            List<Integer> myPartitions = cachePart.get(node.cluster().localNode().id());

            for (Integer part : myPartitions) {

                ScanQuery scanQry = new ScanQuery();

                scanQry.setPartition(part);

                scanQry.setFilter(igniteBiPred);

                try (QueryCursor cursor = cache.query(scanQry)) {
                    for (Object obj : cursor) {
                        // No-op.
                    }
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
            return true;
        }
    }

    /**
     * Query descriptor.
     */
    private static class SqlCacheDescriptor {
        /**
         * Class of value.
         */
        private Class valCls;

        /**
         * Select fields.
         */
        private Set<String> fields;

        /**
         * Indexed fields.
         */
        private Map<String, Class> indexedFieldsByCls;

        /**
         * @param valCls Class of value.
         * @param fields All select fields.
         * @param indexedFieldsByCls Indexed fields.
         */
        SqlCacheDescriptor(Class valCls, Set<String> fields,
            Map<String, Class> indexedFieldsByCls) {
            this.valCls = valCls;
            this.fields = fields;
            this.indexedFieldsByCls = indexedFieldsByCls;
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
            StringBuilder sb = new StringBuilder();

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
         * @return SQL query object.
         */
        SqlQuery getSqlQuery(int id) {
            return new SqlQuery(valCls, makeQueryCondition(id));
        }

        /**
         * @param id Query id.
         * @return SQL filed query object.
         */
        SqlFieldsQuery getSqlFieldsQuery(int id) {
            return new SqlFieldsQuery(String.format("SELECT %s FROM %s WHERE %s",
                makeQuerySelect(id), valCls.getSimpleName(), makeQueryCondition(id)));
        }

    }

    /**
     * Cache operation enum.
     */
    private enum Operation {
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
        SQL_QUERY,

        /** Continuous Query. */
        CONTINUOUS_QUERY;

        /**
         * @param num Number of operation.
         * @return Operation.
         */
        public static Operation valueOf(int num) {
            return values()[num];
        }
    }

    /**
     *
     */
    private static class TestQuery {
        /** */
        private final String sql;

        /** */
        private final boolean distributedJoin;

        /**
         * @param sql SQL.
         * @param distributedJoin Distributed join flag.
         */
        public TestQuery(String sql, boolean distributedJoin) {
            this.sql = sql;
            this.distributedJoin = distributedJoin;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestQuery.class, this);
        }
    }
}
