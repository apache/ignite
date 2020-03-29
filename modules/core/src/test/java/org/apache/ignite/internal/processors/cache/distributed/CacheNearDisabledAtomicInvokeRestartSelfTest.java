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
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Invoke retry consistency test.
 */
public class CacheNearDisabledAtomicInvokeRestartSelfTest extends CacheAbstractRestartSelfTest {
    /** */
    public static final int RANGE = 50;

    /** */
    private static final long FIRST_VAL = 1;

    /** */
    private final ConcurrentMap<String, AtomicLong> nextValMap = new ConcurrentHashMap<>();

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
        return ATOMIC;
    }

    /** */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected void checkCache(IgniteEx ignite, IgniteCache cache) throws Exception {
        log.info("Start cache validation.");

        long startTime = U.currentTimeMillis();

        Map<String, Set> badCacheEntries = new HashMap<>();

        for (Map.Entry<String, AtomicLong> e : nextValMap.entrySet()) {
            String key = e.getKey();

            Set set = (Set)cache.get(key);

            if (set == null || e.getValue() == null || !Objects.equals(e.getValue().get(), (long)set.size()))
                badCacheEntries.put(key, set);
        }

        if (!badCacheEntries.isEmpty()) {
            // Print all usefull information and finish.
            for (Map.Entry<String, Set> e : badCacheEntries.entrySet()) {
                String key = e.getKey();

                U.error(log, "Got unexpected set size [key='" + key + "', expSize=" + nextValMap.get(key)
                    + ", cacheVal=" + e.getValue() + "]");
            }

            log.info("Next values map contant:");
            for (Map.Entry<String, AtomicLong> e : nextValMap.entrySet())
                log.info("Map Entry [key=" + e.getKey() + ", val=" + e.getValue() + "]");

            log.info("Cache content:");

            for (int k2 = 0; k2 < RANGE; k2++) {
                String key2 = "key-" + k2;

                Object val = cache.get(key2);

                if (val != null)
                    log.info("Cache Entry [key=" + key2 + ", val=" + val + "]");

            }

            fail("Cache and local map are in inconsistent state [badKeys=" + badCacheEntries.keySet() + ']');
        }

        log.info("Clearing all data.");

        cache.removeAll();
        nextValMap.clear();

        log.info("Cache validation successfully finished in "
            + (U.currentTimeMillis() - startTime) / 1000 + " sec.");
    }

    /** {@inheritDoc} */
    @Override protected void updateCache(IgniteEx ignite, IgniteCache cache) {
        final int k = ThreadLocalRandom.current().nextInt(RANGE);

        String key = "key-" + k;

        AtomicLong nextAtomicVal = nextValMap.putIfAbsent(key, new AtomicLong(FIRST_VAL));

        Long nextVal = FIRST_VAL;

        if (nextAtomicVal != null)
            nextVal = nextAtomicVal.incrementAndGet();

        cache.invoke(key, new AddInSetEntryProcessor(), nextVal);
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
