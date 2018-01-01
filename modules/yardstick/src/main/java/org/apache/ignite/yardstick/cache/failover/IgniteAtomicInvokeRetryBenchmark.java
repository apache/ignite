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

package org.apache.ignite.yardstick.cache.failover;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Invoke retry failover benchmark. <p> Each client maintains a local map that it updates together with cache. Client
 * invokes an increment closure for all generated keys and atomically increments value for corresponding keys in the
 * local map. To validate cache contents, all writes from the client are stopped, values in the local map are compared
 * to the values in the cache.
 */
public class IgniteAtomicInvokeRetryBenchmark extends IgniteFailoverAbstractBenchmark<String, Set> {
    /** */
    private final ConcurrentMap<String, AtomicLong> nextValMap = new ConcurrentHashMap<>();

    /** */
    private final ReadWriteLock rwl = new ReentrantReadWriteLock(true);

    /** */
    private volatile Exception ex;

    /** {@inheritDoc} */
    @Override public void setUp(final BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        Thread thread = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    final int timeout = args.cacheOperationTimeoutMillis();
                    final int range = args.range();

                    while (!Thread.currentThread().isInterrupted()) {
                        Thread.sleep(args.cacheConsistencyCheckingPeriod() * 1000);

                        rwl.writeLock().lock();

                        try {
                            println("Start cache validation.");

                            long startTime = U.currentTimeMillis();

                            Map<String, Set> badCacheEntries = new HashMap<>();

                            for (Map.Entry<String, AtomicLong> e : nextValMap.entrySet()) {
                                String key = e.getKey();

                                Set set = cache.getAsync(key).get(timeout);

                                if (set == null || e.getValue() == null || !Objects.equals(e.getValue().get(), (long)set.size()))
                                    badCacheEntries.put(key, set);
                            }

                            if (!badCacheEntries.isEmpty()) {
                                // Print all usefull information and finish.
                                for (Map.Entry<String, Set> e : badCacheEntries.entrySet()) {
                                    String key = e.getKey();

                                    println("Got unexpected set size [key='" + key + "', expSize=" + nextValMap.get(key)
                                        + ", cacheVal=" + e.getValue() + "]");
                                }

                                println("Next values map contant:");
                                for (Map.Entry<String, AtomicLong> e : nextValMap.entrySet())
                                    println("Map Entry [key=" + e.getKey() + ", val=" + e.getValue() + "]");

                                println("Cache content:");

                                for (int k2 = 0; k2 < range; k2++) {
                                    String key2 = "key-" + k2;

                                    Object val = cache.getAsync(key2).get(timeout);

                                    if (val != null)
                                        println("Cache Entry [key=" + key2 + ", val=" + val + "]");

                                }

                                throw new IgniteConsistencyException("Cache and local map are in inconsistent state " +
                                    "[badKeys=" + badCacheEntries.keySet() + ']');
                            }

                            println("Clearing all data.");

                            cache.removeAllAsync().get(timeout);

                            nextValMap.clear();

                            println("Cache validation successfully finished in "
                                + (U.currentTimeMillis() - startTime) / 1000 + " sec.");
                        }
                        finally {
                            rwl.writeLock().unlock();
                        }
                    }
                }
                catch (Throwable e) {
                    ex = new Exception(e);

                    println("Got exception: " + e);

                    e.printStackTrace();

                    if (e instanceof Error)
                        throw (Error)e;
                }
            }
        }, "cache-" + cacheName() + "-validator");

        thread.setDaemon(true);

        thread.start();
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        final int k = nextRandom(args.range());

        String key = "key-" + k;

        rwl.readLock().lock();

        try {
            if (ex != null)
                throw ex;

            AtomicLong nextAtomicVal = nextValMap.putIfAbsent(key, new AtomicLong(1));

            Long nextVal = 1L;

            if (nextAtomicVal != null)
                nextVal = nextAtomicVal.incrementAndGet();

            cache.invokeAsync(key, new AddInSetEntryProcessor(), nextVal).get(args.cacheOperationTimeoutMillis());
        }
        finally {
            rwl.readLock().unlock();
        }

        if (ex != null)
            throw ex;

        return true;
    }

    /** {@inheritDoc} */
    @Override protected String cacheName() {
        return "atomic-invoke-retry";
    }

    /**
     */
    private static class AddInSetEntryProcessor implements CacheEntryProcessor<String, Set, Object> {
        /** */
        private static final long serialVersionUID = 0;

        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<String, Set> entry,
            Object... arguments) throws EntryProcessorException {
            assert !F.isEmpty(arguments);

            Object val = arguments[0];

            Set set;

            if (!entry.exists())
                set = new HashSet<>();
            else
                set = entry.getValue();

            set.add(val);

            entry.setValue(set);

            return null;
        }
    }
}
