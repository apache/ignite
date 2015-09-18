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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Atomic retries failover benchmark. Client generates continuous load to the cluster
 * (random get, put, invoke, remove operations)
 */
public class IgniteTransactionalInvokeRetryBenchmark extends IgniteFailoverAbstractBenchmark<String, Long> {
    /** */
    public static final int KEY_RANGE = 100_000;

    /** */
    public static final int CNT_KEYS_IN_LINE = 5;

    /** */
    private final ConcurrentMap<String, Long> map = new ConcurrentHashMap<>();

    /** */
    private final ReadWriteLock rwl = new ReentrantReadWriteLock(true);

    /** */
    private volatile boolean isValidCacheState = true;

    /** {@inheritDoc} */
    @Override public void setUp(final BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        Thread thread = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    while (true) {
                        Thread.sleep(2 * 60 * 1000); // 2 min. // TODO as param

                        rwl.writeLock().lock();

                        try {
                            for (int k = 0; k < KEY_RANGE; k++) {
                                for (int i = 0; i < CNT_KEYS_IN_LINE; k++) {
                                    String key = "key-" + k + "-" + cfg.memberId() + "-" + i;

                                    Long cacheVal = cache.get(key);
                                    Long mapVal = map.get(key);

                                    if (!cacheVal.equals(mapVal)) {
                                        println(cfg, "Got different values [key='" + key + "', cacheVal=" + cacheVal
                                            + ", localMapVal=" + mapVal + "]");

                                        isValidCacheState = false;
                                    }
                                }
                            }
                        }
                        finally {
                            rwl.writeLock().unlock();
                        }
                    }
                }
                catch (InterruptedException e) {
                    println("[CACHE-VALIDATOR] Got exception: " + e);
                    e.printStackTrace();
                }
            }
        }, "cache-validator");

        thread.setDaemon(true);

        thread.start();
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        final int k = nextRandom(KEY_RANGE);

        final String[] keys = new String[CNT_KEYS_IN_LINE]; // TODO impl number.

        assert keys.length > 0 : "Count of keys = " + keys.length;

        for (int i = 0; i < keys.length; i++)
            keys[i] = "key-" + k + "-" + cfg.memberId() + "-" + i;

        for (String key : keys) {
            rwl.readLock().lock();

            try {
                if (!isValidCacheState)
                    return isValidCacheState;

                Long newVal = cache.invoke(key, new IncrementCacheEntryProcessor());

                map.put(key, newVal);
            }
            finally {
                rwl.readLock().unlock();
            }
        }

        return isValidCacheState;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<String, Long> cache() {
        return ignite().cache("atomic");
    }

    /**
     */
    private static class IncrementCacheEntryProcessor implements CacheEntryProcessor<String, Long, Long> {
        /** */
        private static final long serialVersionUID = 0;

        /** {@inheritDoc} */
        @Override public Long process(MutableEntry<String, Long> entry, Object... arguments) throws EntryProcessorException {
            long newVal = entry.getValue() == null ? 0 : entry.getValue() + 1;

            entry.setValue(newVal);

            return newVal;
        }
    }
}
