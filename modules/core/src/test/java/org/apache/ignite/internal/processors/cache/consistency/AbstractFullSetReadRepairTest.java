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
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheEntry;
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
    protected static final Consumer<ReadRepairData> GET_CHECK_AND_REPAIR = (rrd) -> {
        for (Integer key : rrd.data.keySet()) { // Once.
            assertEquals(rrd.data.get(key).repaired, get(rrd));
        }
    };

    /**
     *
     */
    protected static final Consumer<ReadRepairData> GETALL_CHECK_AND_REPAIR = (rrd) -> {
        Map<Integer, Object> res = getAll(rrd);

        for (Integer key : rrd.data.keySet())
            assertEquals(rrd.data.get(key).repaired, res.get(key));
    };

    /**
     *
     */
    protected static final Consumer<ReadRepairData> GET_NULL = (rrd) -> assertNull(get(invertKeys(rrd)));

    /**
     *
     */
    protected static final Consumer<ReadRepairData> GET_ALL_NULL = (rrd) -> assertTrue(getAll(invertKeys(rrd)).isEmpty());

    /**
     *
     */
    protected static final Consumer<ReadRepairData> CONTAINS_CHECK_AND_REPAIR = (rrd) -> {
        Set<Integer> keys = rrd.data.keySet();

        assert keys.size() == 1;

        for (Map.Entry<Integer, InconsistentMapping> entry : rrd.data.entrySet()) { // Once.
            Integer key = entry.getKey();
            Object repaired = entry.getValue().repaired;

            boolean res = rrd.async ?
                rrd.cache.withReadRepair(rrd.strategy).containsKeyAsync(key).get() :
                rrd.cache.withReadRepair(rrd.strategy).containsKey(key);

            assertEquals(repaired != null, res);
        }
    };

    /**
     *
     */
    protected static final Consumer<ReadRepairData> CONTAINS_ALL_CHECK_AND_REPAIR = (rrd) -> {
        Set<Integer> keys = rrd.data.keySet();

        assert !keys.isEmpty();

        boolean res = rrd.async ?
            rrd.cache.withReadRepair(rrd.strategy).containsKeysAsync(keys).get() :
            rrd.cache.withReadRepair(rrd.strategy).containsKeys(keys);

        boolean containsAll = true;

        for (Integer key : keys) {
            Object repaired = rrd.data.get(key).repaired;

            if (repaired == null)
                containsAll = false;
        }

        assertEquals(containsAll, res);
    };

    /**
     *
     */
    private static Object get(ReadRepairData rrd) {
        Set<Integer> keys = rrd.data.keySet();

        assert keys.size() == 1;

        Integer key = keys.iterator().next();

        Object res;

        if (rrd.raw) {
            CacheEntry<Integer, Object> rawEntry = rrd.async ?
                rrd.cache.withReadRepair(rrd.strategy).getEntryAsync(key).get() :
                rrd.cache.withReadRepair(rrd.strategy).getEntry(key);

            res = rawEntry != null ? rawEntry.getValue() : null;
        }
        else
            res = rrd.async ?
                rrd.cache.withReadRepair(rrd.strategy).getAsync(key).get() :
                rrd.cache.withReadRepair(rrd.strategy).get(key);

        return unwrapBinaryIfNeeded(rrd.binary, res);
    }

    /**
     *
     */
    private static Map<Integer, Object> getAll(ReadRepairData rrd) {
        Set<Integer> keys = rrd.data.keySet();

        assert !keys.isEmpty();

        Map<Integer, Object> objs;

        if (rrd.raw) {
            Collection<CacheEntry<Integer, Object>> entryRes =
                rrd.async ?
                    rrd.cache.withReadRepair(rrd.strategy).getEntriesAsync(keys).get() :
                    rrd.cache.withReadRepair(rrd.strategy).getEntries(keys);

            objs = new HashMap<>();

            for (CacheEntry<Integer, Object> entry : entryRes)
                objs.put(entry.getKey(), entry.getValue());
        }
        else {
            objs = rrd.async ?
                rrd.cache.withReadRepair(rrd.strategy).getAllAsync(keys).get() :
                rrd.cache.withReadRepair(rrd.strategy).getAll(keys);
        }

        Map<Integer, Object> res = new HashMap<>();

        for (Map.Entry<Integer, Object> entry : objs.entrySet()) {
            Object obj = entry.getValue();

            res.put(entry.getKey(), unwrapBinaryIfNeeded(rrd.binary, obj));
        }

        return res;
    }

    /**
     * Inverts keys to gain null on get attempt.
     */
    private static ReadRepairData invertKeys(ReadRepairData rrd) {
        return new ReadRepairData(
            rrd.cache,
            rrd.data.entrySet().stream().collect(
                Collectors.toMap(
                    e -> -1 * e.getKey(), // Negative key to gain null.
                    Map.Entry::getValue,
                    (k, v) -> {
                        throw new IllegalStateException(String.format("Duplicate key %s", k));
                    },
                    TreeMap::new)),
            rrd.raw,
            rrd.async,
            rrd.strategy,
            rrd.binary);
    }

    /**
     *
     */
    protected static final BiConsumer<ReadRepairData, IgniteIrreparableConsistencyViolationException> CHECK_REPAIRED =
        (rrd, e) -> {
            boolean raw = rrd.raw;

            for (Map.Entry<Integer, InconsistentMapping> entry : rrd.data.entrySet()) {
                Integer key = entry.getKey();

                // Checking only repaired entries, while entries listed at exception were not repaired.
                if (e != null && (e.irreparableKeys().contains(key) ||
                    (e.repairableKeys() != null && e.repairableKeys().contains(key))))
                    continue;

                Object repaired = entry.getValue().repaired;

                Object res;

                if (raw) {
                    CacheEntry<Integer, Object> rawEntry = rrd.cache.getEntry(key);

                    res = rawEntry != null ? rawEntry.getValue() : null;
                }
                else
                    res = rrd.cache.get(key);

                assertEquals(repaired, unwrapBinaryIfNeeded(rrd.binary, res));
            }
        };

    /**
     *
     */
    protected final BiConsumer<ReadRepairData, Runnable> repairIfRepairable = (rrd, r) -> {
        try {
            r.run();

            assertTrue(rrd.repairable());
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

            assertFalse(rrd.repairable());

            check(rrd, (IgniteIrreparableConsistencyViolationException)cause, true);
        }
    };

    /**
     * @param rrd Data.
     */
    protected void check(ReadRepairData rrd, IgniteIrreparableConsistencyViolationException e, boolean evtRecorded) {
        Collection<Object> irreparableKeys = e != null ? e.irreparableKeys() : null;

        if (e != null) {
            Collection<Object> repairableKeys = e.repairableKeys();

            if (repairableKeys != null)
                assertTrue(Collections.disjoint(repairableKeys, irreparableKeys));

            Collection<Object> expectedToBeIrreparableKeys = rrd.data.entrySet().stream()
                .filter(entry -> !entry.getValue().repairable)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

            assertEqualsCollectionsIgnoringOrder(expectedToBeIrreparableKeys, irreparableKeys);
        }

        assertEquals(irreparableKeys == null, rrd.repairable());

        if (evtRecorded)
            checkEvent(rrd, e);
        else
            checkEventMissed();

        CHECK_REPAIRED.accept(rrd, e);
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
    private void test(Ignite initiator, int cnt, boolean all) throws Exception {
        testGet(initiator, cnt, all);
        testGetNull(initiator, cnt, all);
        testContains(initiator, cnt, all);
    }

    /**
     *
     */
    protected abstract void testGet(Ignite initiator, int cnt, boolean all) throws Exception;

    /**
     *
     */
    protected abstract void testGetNull(Ignite initiator, int cnt, boolean all) throws Exception;

    /**
     *
     */
    protected abstract void testContains(Ignite initiator, int cnt, boolean all) throws Exception;
}
