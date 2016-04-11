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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.apache.ignite.yardstick.cache.load.model.ModelUtil;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Ignite cache random operation benchmark
 */
public class IgniteCacheRandomOperationBenchmark extends IgniteAbstractBenchmark {

    /** */
    public static final int operations = Operation.values().length;

    /** list off all available cache*/
    private List<IgniteCache> availableCaches;
    /** list of available transactional cache*/
    private List<IgniteCache> transactionalCaches;
    /** Map cache name on key classes*/
    private Map<String, Class[]> keysCacheClasses;
    /** Map cache name on value classes*/
    private Map<String, Class[]> valuesCacheClasses;

    /**
     * @throws Exception If failed
     */
    private void searchCache() throws Exception {
        availableCaches = new ArrayList<>(ignite().cacheNames().size());
        transactionalCaches = new ArrayList<>();
        keysCacheClasses = new HashMap<>();
        valuesCacheClasses = new HashMap<>();

        for (String name: ignite().cacheNames()) {
            IgniteCache<Object, Object> cache = ignite().cache(name);

            CacheConfiguration configuration = cache.getConfiguration(CacheConfiguration.class);
            if (configuration.getIndexedTypes() != null && configuration.getIndexedTypes().length != 0) {
                ArrayList<Class> keys = new ArrayList<>();
                ArrayList<Class> values = new ArrayList<>();
                int i = 0;
                for (Class clazz: configuration.getIndexedTypes()) {
                    if (ModelUtil.canCreateInstance(clazz))
                        if (i % 2 == 0)
                            keys.add(clazz);
                        else
                            values.add(clazz);
                    i++;
                }
                if (keys.size() == 0 || values.size() == 0)
                    continue;

                keysCacheClasses.put(name, keys.toArray(new Class[]{}));
                valuesCacheClasses.put(name, values.toArray(new Class[]{}));


            }

            if (configuration.getAtomicityMode() == CacheAtomicityMode.TRANSACTIONAL)
                transactionalCaches.add(cache);

            availableCaches.add(cache);
        }
    }

    /**
     * Preloading
     * @throws Exception if fail
     */
    private void preLoading() throws Exception {
        for (IgniteCache cache: availableCaches)
            for (int i=0; i<args.preloadAmount(); i++) {
                int id = nextRandom(args.range());
                cache.put(createRandomKye(id, cache.getName()), createRandomValue(id, cache.getName()));
            }
    }

    /**
     * @param id object identifier
     * @param cacheName name of Ignite cache
     * @return random object
     */
    private Object createRandomKye(int id, String cacheName) {
        int cntKeys;
        Class[] keys;
        if (keysCacheClasses.containsKey(cacheName)) {
            cntKeys = valuesCacheClasses.get(cacheName).length;
            keys = valuesCacheClasses.get(cacheName);
        } else {
            cntKeys = ModelUtil.keyClasses().length;
            keys = ModelUtil.keyClasses();
        }
        return ModelUtil.create(keys[nextRandom(cntKeys)], id);
    }

    /**
     * @param id object identifier
     * @param cacheName name of Ignite cache
     * @return random object
     */
    private Object createRandomValue(int id, String cacheName) {
        int cntValues;
        Class[] values;
        if (valuesCacheClasses.containsKey(cacheName)) {
            cntValues = valuesCacheClasses.get(cacheName).length;
            values = valuesCacheClasses.get(cacheName);
        } else {
            cntValues = ModelUtil.valueClasses().length;
            values = ModelUtil.valueClasses();
        }
        return ModelUtil.create(values[nextRandom(cntValues)], id);
    }

    /** {@inheritDoc} */
    @Override protected void init() throws Exception {
        super.init();

        searchCache();

        preLoading();
    }

    /** {@inheritDoc} */
    @Override
    public boolean test(Map<Object, Object> map) throws Exception {
        boolean executeInTransaction = nextRandom(100) % 2 == 0;
        if (executeInTransaction) {
            executeInTransaction();
            executeWithoutTransaction(true);
        }
        else
            executeWithoutTransaction(false);
        return true;
    }

    /**
     * @param withoutTransactionCache without transaction cache
     * @throws Exception if fail
     */
    private void executeWithoutTransaction(boolean withoutTransactionCache) throws Exception {
        for (IgniteCache cache: availableCaches) {
            if (withoutTransactionCache && transactionalCaches.contains(cache))
                continue;
            executeRandomOperation(cache);
        }
    }

    /**
     * @param cache Ignite cache
     * @throws Exception if fail
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
     * Execute operations in transaction
     * @throws Exception if faiil
     */
    private void executeInTransaction() throws Exception {
        try (Transaction tx = ignite().transactions().txStart(args.txConcurrency(), args.txIsolation())){
            for (IgniteCache cache: transactionalCaches) {
                executeRandomOperation(cache);
            }
            tx.commit();
        }
    }

    /**
     * @param cache ignite cache
     * @throws Exception If failed
     */
    private void doPut(IgniteCache cache) throws Exception {
        int i = nextRandom(args.range());
        cache.put(createRandomKye(i, cache.getName()),
            createRandomValue(i, cache.getName()));
    }

    /**
     * @param cache ignite cache
     * @throws Exception If failed
     */
    private void doPutAll(IgniteCache cache) throws Exception {
        Map putMap = new TreeMap();
        for (int cnt=0; cnt<args.batch(); cnt++) {
            int i = nextRandom(args.range());
            putMap.put(createRandomKye(i, cache.getName()),
                createRandomValue(i, cache.getName()));
        }
        cache.putAll(putMap);
    }

    /**
     * @param cache ignite cache
     * @throws Exception If failed
     */
    private void doGet(IgniteCache cache) throws Exception {
        int i = nextRandom(args.range());
        cache.get(createRandomKye(i, cache.getName()));
    }

    /**
     * @param cache ignite cache
     * @throws Exception If failed
     */
    private void doGetAll(IgniteCache cache) throws Exception {
        Set keys = new TreeSet();
        for (int cnt=0; cnt<args.batch(); cnt++) {
            int i = nextRandom(args.range());
            keys.add(createRandomKye(i, cache.getName()));
        }
        cache.get(keys);
    }

    /**
     * @param cache ignite cache
     * @throws Exception If failed
     */
    private void doInvoke(final IgniteCache cache) throws Exception {
        final int i = nextRandom(args.range());
        cache.invoke(createRandomKye(i, cache.getName()), new EntryProcessor() {
                @Override public Object process(MutableEntry entry1, Object... arguments) throws EntryProcessorException {
                    try {
                        entry1.setValue(createRandomValue(i + 1, cache.getName()));
                    } catch (Exception e) {
                        throw new EntryProcessorException(e);
                    }
                    return null;
                }
            }
        );
    }

    /**
     * @param cache ignite cache
     * @throws Exception If failed
     */
    private void doInvokeAll(final IgniteCache cache) throws Exception {
        Set keys = new TreeSet();
        for (int cnt=0; cnt<args.batch(); cnt++) {
            int i = nextRandom(args.range());
            keys.add(createRandomKye(i, cache.getName()));
        }
        cache.invokeAll(keys, new EntryProcessor() {
                @Override public Object process(MutableEntry entry1, Object... arguments) {
                    try {
                        int i = nextRandom(args.range());
                        entry1.setValue(createRandomValue(i + 1, cache.getName()));
                    } catch (Exception e) {
                        throw new EntryProcessorException(e);
                    }
                    return null;
                }
            }
        );
    }

    /**
     * @param cache ignite cache
     * @throws Exception If failed
     */
    private void doRemove(IgniteCache cache) throws Exception {
        int i = nextRandom(args.range());
        cache.remove(createRandomKye(i, cache.getName()));
    }

    /**
     * @param cache ignite cache
     * @throws Exception If failed
     */
    private void doRemoveAll(IgniteCache cache) throws Exception {
        Set keys = new TreeSet();
        for (int cnt=0; cnt<args.batch(); cnt++) {
            int i = nextRandom(args.range());
            keys.add(createRandomKye(i, cache.getName()));
        }
        cache.removeAll(keys);
    }

    /**
     * @param cache ignite cache
     * @throws Exception If failed
     */
    private void doPutIfAbsent(IgniteCache cache) throws Exception {
        int i = nextRandom(args.range());
        cache.putIfAbsent(createRandomKye(i, cache.getName()),
            createRandomValue(i, cache.getName()));
    }

    /**
     * @param cache ignite cache
     * @throws Exception If failed
     */
    private void doReplace(IgniteCache cache) throws Exception {
        int i = nextRandom(args.range());
        cache.replace(createRandomKye(i, cache.getName()),
            createRandomValue(i, cache.getName()),
            createRandomValue(i + 1, cache.getName()));
    }

    /**
     * @param cache ignite cache
     * @throws Exception If failed
     */
    private void doScanQuery(IgniteCache cache) throws Exception {
        IgniteBiPredicate filter = new IgniteBiPredicate() {
            /**
             * @param key cache key
             * @param val cache value
             * @return true if is hit
             */
            @Override public boolean apply(Object key, Object val) {
                return val.hashCode() % 45 == 0;
            }
        };

        try (QueryCursor cursor = cache.query(new ScanQuery(filter))) {
            for (Object val : cursor);
        }
    }

    /**
     * Cache operation enum
     */
    static enum Operation {
        /** put operation */
        PUT,

        /** put all operation*/
        PUTALL,

        /** get operation*/
        GET ,

        /** get all operation*/
        GETALL ,

        /** invoke opearation*/
        INVOKE,

        /** invoke all operation*/
        INVOKEALL,

        /** remove operation*/
        REMOVE,

        /** remove all operation*/
        REMOVEALL,

        /** put if absent operation*/
        PUTIFABSENT,

        /** replace operation*/
        REPLACE,

        /** scan query operation*/
        SCANQUERY;

        /**
         * @param num numbre of opearation
         * @return operation
         */
        public static Operation valueOf(int num) {
            return values()[num];
        }
    }
}
