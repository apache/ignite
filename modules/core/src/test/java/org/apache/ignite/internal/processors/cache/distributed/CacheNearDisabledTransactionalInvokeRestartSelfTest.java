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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Invoke retry consistency test.
 */
public class CacheNearDisabledTransactionalInvokeRestartSelfTest extends CacheAbstractRestartSelfTest {
    /** */
    public static final int RANGE = 100;

    /** */
    private static final int KEYS_CNT = 5;

    /** */
    protected final ConcurrentMap<String, AtomicLong> map = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected void checkCache(IgniteEx ignite, IgniteCache cache) {
        log.info("Start cache validation.");

        long startTime = U.currentTimeMillis();

        Map<String, Long> notEqualsCacheVals = new HashMap<>();
        Map<String, Long> notEqualsLocMapVals = new HashMap<>();

        for (int k = 0; k < RANGE; k++) {
            if (k % 10_000 == 0)
                log.info("Start validation for keys like 'key-" + k + "-*'");

            for (int i = 0; i < KEYS_CNT; i++) {
                String key = "key-" + k + "-" + i;

                Long cacheVal = (Long)cache.get(key);

                AtomicLong aVal = map.get(key);
                Long mapVal = aVal != null ? aVal.get() : null;

                if (!Objects.equals(cacheVal, mapVal)) {
                    notEqualsCacheVals.put(key, cacheVal);
                    notEqualsLocMapVals.put(key, mapVal);
                }
            }
        }

        assert notEqualsCacheVals.size() == notEqualsLocMapVals.size() : "Invalid state " +
            "[cacheMapVals=" + notEqualsCacheVals + ", mapVals=" + notEqualsLocMapVals + "]";

        if (!notEqualsCacheVals.isEmpty()) {
            // Print all usefull information and finish.
            for (Map.Entry<String, Long> eLocMap : notEqualsLocMapVals.entrySet()) {
                String key = eLocMap.getKey();
                Long mapVal = eLocMap.getValue();
                Long cacheVal = notEqualsCacheVals.get(key);

                U.error(log, "Got different values [key='" + key
                    + "', cacheVal=" + cacheVal + ", localMapVal=" + mapVal + "]");
            }

            log.info("Local driver map contant:\n " + map);

            log.info("Cache content:");

            for (int k2 = 0; k2 < RANGE; k2++) {
                for (int i2 = 0; i2 < KEYS_CNT; i2++) {
                    String key2 = "key-" + k2 + "-" + i2;

                    Long val = (Long)cache.get(key2);

                    if (val != null)
                        log.info("Entry [key=" + key2 + ", val=" + val + "]");
                }
            }

            throw new IllegalStateException("Cache and local map are in inconsistent state [badKeys="
                + notEqualsCacheVals.keySet() + ']');
        }

        log.info("Cache validation successfully finished in "
            + (U.currentTimeMillis() - startTime) / 1000 + " sec.");
    }

    /** {@inheritDoc} */
    @Override protected void updateCache(IgniteEx ignite, IgniteCache cache) {
        final int k = ThreadLocalRandom.current().nextInt(RANGE);

        final String[] keys = new String[KEYS_CNT];

        for (int i = 0; i < keys.length; i++)
            keys[i] = "key-" + k + "-" + i;

        for (String key : keys) {
            cache.invoke(key, new IncrementCacheEntryProcessor());

            AtomicLong prevVal = map.putIfAbsent(key, new AtomicLong(0));

            if (prevVal != null)
                prevVal.incrementAndGet();
        }
    }

    /**
     */
    private static class IncrementCacheEntryProcessor implements CacheEntryProcessor<String, Long, Long> {
        /** */
        private static final long serialVersionUID = 0;

        /** {@inheritDoc} */
        @Override public Long process(MutableEntry<String, Long> entry,
            Object... arguments) throws EntryProcessorException {
            long newVal = entry.getValue() == null ? 0 : entry.getValue() + 1;

            entry.setValue(newVal);

            return newVal;
        }
    }
}
