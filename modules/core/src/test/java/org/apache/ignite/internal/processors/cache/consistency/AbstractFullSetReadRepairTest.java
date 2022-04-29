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

package org.apache.ignite.internal.processors.cache.consistency;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.ReadRepairStrategy;
import org.apache.ignite.internal.processors.cache.consistency.ReadRepairDataGenerator.InconsistentMapping;
import org.apache.ignite.internal.processors.cache.consistency.ReadRepairDataGenerator.ReadRepairData;
import org.apache.ignite.internal.processors.cache.distributed.near.consistency.IgniteIrreparableConsistencyViolationException;
import org.junit.Test;

/**
 *
 */
public abstract class AbstractFullSetReadRepairTest extends AbstractReadRepairTest {
    /**
     *
     */
    protected static final Consumer<ReadRepairData> GET_CHECK_AND_REPAIR = (data) -> {
        IgniteCache<Integer, Object> cache = data.cache;

        Set<Integer> keys = data.data.keySet();
        boolean raw = data.raw;
        boolean async = data.async;
        boolean binary = data.binary;
        ReadRepairStrategy strategy = data.strategy;

        assert keys.size() == 1;

        for (Map.Entry<Integer, InconsistentMapping> entry : data.data.entrySet()) { // Once.
            Integer key = entry.getKey();
            Object repaired = entry.getValue().repaired;

            Object res;

            if (binary) {
                Object obj = get(cache.withKeepBinary(), key, raw, async, strategy);

                res = unwrapBinaryIfNeeded(binary, obj);
            }
            else
                res = get(cache, key, raw, async, strategy);

            assertEquals(repaired, res);
        }
    };

    /**
     *
     */
    protected static final Consumer<ReadRepairData> GETALL_CHECK_AND_REPAIR = (data) -> {
        IgniteCache<Integer, Object> cache = data.cache;

        Set<Integer> keys = data.data.keySet();
        boolean raw = data.raw;
        boolean async = data.async;
        boolean binary = data.binary;
        ReadRepairStrategy strategy = data.strategy;

        assert !keys.isEmpty();

        Map<Integer, Object> res;

        if (binary) {
            Map<Integer, Object> objs = getAll(cache.withKeepBinary(), keys, raw, async, strategy);

            res = new HashMap<>();

            for (Map.Entry<Integer, Object> entry : objs.entrySet()) {
                Object obj = entry.getValue();

                res.put(entry.getKey(), unwrapBinaryIfNeeded(binary, obj));
            }
        }
        else
            res = getAll(cache, keys, raw, async, strategy);

        for (Map.Entry<Integer, Object> entry : res.entrySet()) {
            Object repaired = data.data.get(entry.getKey()).repaired;

            assertEquals(repaired, entry.getValue());
        }
    };

    /**
     *
     */
    protected static final Consumer<ReadRepairData> GET_NULL = (data) -> {
        IgniteCache<Integer, Object> cache = data.binary ? data.cache.withKeepBinary() : data.cache;

        Set<Integer> keys = data.data.keySet();
        boolean raw = data.raw;
        boolean async = data.async;
        ReadRepairStrategy strategy = data.strategy;

        assert keys.size() == 1;

        for (Integer key : data.data.keySet()) { // Once.
            Integer missed = -key; // Negative to gain null.

            Object res = get(cache, missed, raw, async, strategy);

            assertEquals(null, res);
        }
    };

    /**
     *
     */
    protected static final Consumer<ReadRepairData> GET_ALL_NULL = (data) -> {
        IgniteCache<Integer, Object> cache = data.binary ? data.cache.withKeepBinary() : data.cache;

        Set<Integer> keys = data.data.keySet();
        boolean raw = data.raw;
        boolean async = data.async;
        ReadRepairStrategy strategy = data.strategy;

        Set<Integer> missed = keys.stream().map(key -> -key).collect(Collectors.toCollection(TreeSet::new)); // Negative to gain null.

        Map<Integer, Object> res = getAll(cache, missed, raw, async, strategy);

        assertTrue(res.isEmpty());
    };

    /**
     *
     */
    protected static final Consumer<ReadRepairData> CONTAINS_CHECK_AND_REPAIR = (data) -> {
        IgniteCache<Integer, Object> cache = data.binary ? data.cache.withKeepBinary() : data.cache;

        Set<Integer> keys = data.data.keySet();
        boolean async = data.async;
        ReadRepairStrategy strategy = data.strategy;

        assert keys.size() == 1;

        for (Map.Entry<Integer, InconsistentMapping> entry : data.data.entrySet()) { // Once.
            Integer key = entry.getKey();
            Object repaired = entry.getValue().repaired;

            boolean res = async ?
                cache.withReadRepair(strategy).containsKeyAsync(key).get() :
                cache.withReadRepair(strategy).containsKey(key);

            assertEquals(repaired != null, res);
        }
    };

    /**
     *
     */
    protected static final Consumer<ReadRepairData> CONTAINS_ALL_CHECK_AND_REPAIR = (data) -> {
        IgniteCache<Integer, Object> cache = data.binary ? data.cache.withKeepBinary() : data.cache;

        Set<Integer> keys = data.data.keySet();
        boolean async = data.async;
        ReadRepairStrategy strategy = data.strategy;

        boolean res = async ?
            cache.withReadRepair(strategy).containsKeysAsync(keys).get() :
            cache.withReadRepair(strategy).containsKeys(keys);

        boolean allRepaired = true;

        for (Integer key : keys) {
            Object repaired = data.data.get(key).repaired;

            if (repaired == null)
                allRepaired = false;
        }

        assertEquals(allRepaired, res);
    };

    /**
     *
     */
    private static Object get(IgniteCache<Integer, Object> cache, Integer key, boolean raw, boolean async,
        ReadRepairStrategy strategy) {
        Object res;

        if (raw) {
            CacheEntry<Integer, Object> rawEntry = async ?
                cache.withReadRepair(strategy).getEntryAsync(key).get() :
                cache.withReadRepair(strategy).getEntry(key);

            res = rawEntry != null ? rawEntry.getValue() : null;
        }
        else
            res = async ?
                cache.withReadRepair(strategy).getAsync(key).get() :
                cache.withReadRepair(strategy).get(key);

        return res;
    }

    /**
     *
     */
    private static Map<Integer, Object> getAll(IgniteCache<Integer, Object> cache, Set<Integer> keys, boolean raw,
        boolean async, ReadRepairStrategy strategy) {
        Map<Integer, Object> res;

        if (raw) {
            Collection<CacheEntry<Integer, Object>> entryRes =
                async ?
                    cache.withReadRepair(strategy).getEntriesAsync(keys).get() :
                    cache.withReadRepair(strategy).getEntries(keys);

            res = new HashMap<>();

            for (CacheEntry<Integer, Object> entry : entryRes)
                res.put(entry.getKey(), entry.getValue());
        }
        else {
            res = async ?
                cache.withReadRepair(strategy).getAllAsync(keys).get() :
                cache.withReadRepair(strategy).getAll(keys);
        }

        return res;
    }

    /**
     *
     */
    protected static final BiConsumer<ReadRepairData, IgniteIrreparableConsistencyViolationException> CHECK_REPAIRED =
        (data, e) -> {
            IgniteCache<Integer, Object> cache = data.cache;
            boolean raw = data.raw;

            for (Map.Entry<Integer, InconsistentMapping> entry : data.data.entrySet()) {
                Integer key = entry.getKey();

                // Checking only repaired entries, while entries listed at exception were not repaired.
                if (e != null && (e.irreparableKeys().contains(key) ||
                    (e.repairableKeys() != null && e.repairableKeys().contains(key))))
                    continue;

                Object repaired = entry.getValue().repaired;

                Object res;

                if (raw) {
                    CacheEntry<Integer, Object> rawEntry = cache.getEntry(key);

                    res = rawEntry != null ? rawEntry.getValue() : null;
                }
                else
                    res = cache.get(key);

                assertEquals(repaired, res);
            }
        };

    /**
     *
     */
    protected final BiConsumer<ReadRepairData, Runnable> repairIfRepairable = (data, r) -> {
        try {
            r.run();

            assertTrue(data.repairable());
        }
        catch (RuntimeException e) {
            Throwable cause = e.getCause();

            if (cause == null) {
                e.printStackTrace();

                fail("Unexpected exception: " + e.getMessage());
            }

            if (!(cause instanceof IgniteIrreparableConsistencyViolationException)) {
                cause.printStackTrace();

                fail("Unexpected exception: " + cause.getMessage());
            }

            assertFalse(data.repairable());

            check(data, (IgniteIrreparableConsistencyViolationException)cause, true);
        }
    };

    /**
     * @param data Data.
     */
    protected void check(ReadRepairData data, IgniteIrreparableConsistencyViolationException e, boolean evtRecorded) {
        Collection<Object> irreparableKeys = e != null ? e.irreparableKeys() : null;

        if (e != null) {
            Collection<Object> repairableKeys = e.repairableKeys();

            if (repairableKeys != null)
                assertTrue(Collections.disjoint(repairableKeys, irreparableKeys));

            Collection<Object> expectedToBeIrreparableKeys = data.data.entrySet().stream()
                .filter(entry -> !entry.getValue().repairable)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

            assertEqualsCollectionsIgnoringOrder(expectedToBeIrreparableKeys, irreparableKeys);
        }

        assertEquals(irreparableKeys == null, data.repairable());

        if (evtRecorded)
            checkEvent(data, e);
        else
            checkEventMissed();

        CHECK_REPAIRED.accept(data, e);
    }

    /**
     *
     */
    @Test
    public void test() throws Exception {
        assertFalse(clsAwareNodes.isEmpty());

        for (Ignite initiator : clsAwareNodes) {
            test(initiator, 1, false); // just get
            test(initiator, 1, true); // 1 (all keys available at primary)
            test(initiator, 2, true); // less than backups
            test(initiator, 3, true); // equals to backups
            test(initiator, 4, true); // equals to backups + primary
            test(initiator, 10, true); // more than backups + primary
        }
    }

    /**
     *
     */
    private void test(Ignite initiator, Integer cnt, boolean all) throws Exception {
        testGet(initiator, cnt, all);
        testGetNull(initiator, cnt, all);
        testContains(initiator, cnt, all);
    }

    /**
     *
     */
    protected abstract void testGet(Ignite initiator, Integer cnt, boolean all) throws Exception;

    /**
     *
     */
    protected abstract void testGetNull(Ignite initiator, Integer cnt, boolean all) throws Exception;

    /**
     *
     */
    protected abstract void testContains(Ignite initiator, Integer cnt, boolean all) throws Exception;
}
