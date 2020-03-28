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
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.internal.util.typedef.G;
import org.junit.Test;

public abstract class AbstractFullSetReadRepairTest extends AbstractReadRepairTest {
    /**
     *
     */
    protected static final Consumer<ReadRepairData> GET_CHECK_AND_FIX = (data) -> {
        IgniteCache<Integer, Integer> cache = data.cache;
        Set<Integer> keys = data.data.keySet();
        boolean raw = data.raw;
        boolean async = data.async;

        assert keys.size() == 1;

        for (Map.Entry<Integer, InconsistentMapping> entry : data.data.entrySet()) { // Once.
            Integer key = entry.getKey();
            Integer latest = entry.getValue().latest;

            Integer res =
                raw ?
                    async ?
                        cache.withReadRepair().getEntryAsync(key).get().getValue() :
                        cache.withReadRepair().getEntry(key).getValue() :
                    async ?
                        cache.withReadRepair().getAsync(key).get() :
                        cache.withReadRepair().get(key);

            assertEquals(latest, res);

            checkEvent(data);
        }
    };

    /**
     *
     */
    protected static final Consumer<ReadRepairData> GETALL_CHECK_AND_FIX = (data) -> {
        IgniteCache<Integer, Integer> cache = data.cache;
        Set<Integer> keys = data.data.keySet();
        boolean raw = data.raw;
        boolean async = data.async;

        assert !keys.isEmpty();

        if (raw) {
            Collection<CacheEntry<Integer, Integer>> res =
                async ?
                    cache.withReadRepair().getEntriesAsync(keys).get() :
                    cache.withReadRepair().getEntries(keys);

            for (CacheEntry<Integer, Integer> entry : res)
                assertEquals(data.data.get(entry.getKey()).latest, entry.getValue());
        }
        else {
            Map<Integer, Integer> res =
                async ?
                    cache.withReadRepair().getAllAsync(keys).get() :
                    cache.withReadRepair().getAll(keys);

            for (Map.Entry<Integer, Integer> entry : res.entrySet())
                assertEquals(data.data.get(entry.getKey()).latest, entry.getValue());
        }

        checkEvent(data);
    };

    /**
     *
     */
    protected static final Consumer<ReadRepairData> GET_NULL = (data) -> {
        IgniteCache<Integer, Integer> cache = data.cache;
        Set<Integer> keys = data.data.keySet();
        boolean raw = data.raw;
        boolean async = data.async;

        assert keys.size() == 1;

        for (Map.Entry<Integer, InconsistentMapping> entry : data.data.entrySet()) { // Once.
            Integer key = entry.getKey();
            Integer missed = key * -1; // Negative to gain null.

            Object res =
                raw ?
                    async ?
                        cache.withReadRepair().getEntryAsync(missed).get() :
                        cache.withReadRepair().getEntry(missed) :
                    async ?
                        cache.withReadRepair().getAsync(missed).get() :
                        cache.withReadRepair().get(missed);

            assertEquals(null, res);

            checkEvent( // Checking on null expectations.
                new ReadRepairData(
                    cache,
                    Collections.singletonMap(
                        missed,
                        new InconsistentMapping(new HashMap<>(), null, null)),
                    raw,
                    async));
        }
    };

    /**
     *
     */
    protected static final Consumer<ReadRepairData> CONTAINS_CHECK_AND_FIX = (data) -> {
        IgniteCache<Integer, Integer> cache = data.cache;
        Set<Integer> keys = data.data.keySet();
        boolean async = data.async;

        assert keys.size() == 1;

        for (Map.Entry<Integer, InconsistentMapping> entry : data.data.entrySet()) { // Once.
            Integer key = entry.getKey();

            boolean res = async ?
                cache.withReadRepair().containsKeyAsync(key).get() :
                cache.withReadRepair().containsKey(key);

            assertEquals(true, res);

            checkEvent(data);
        }
    };

    /**
     *
     */
    protected static final Consumer<ReadRepairData> CONTAINS_ALL_CHECK_AND_FIX = (data) -> {
        IgniteCache<Integer, Integer> cache = data.cache;
        Set<Integer> keys = data.data.keySet();
        boolean async = data.async;

        boolean res = async ?
            cache.withReadRepair().containsKeysAsync(keys).get() :
            cache.withReadRepair().containsKeys(keys);

        assertEquals(true, res);

        checkEvent(data);
    };

    /**
     *
     */
    protected static final Consumer<ReadRepairData> ENSURE_FIXED = (data) -> {
        IgniteCache<Integer, Integer> cache = data.cache;
        boolean raw = data.raw;

        for (Map.Entry<Integer, InconsistentMapping> entry : data.data.entrySet()) {
            Integer key = entry.getKey();
            Integer latest = entry.getValue().latest;

            Integer res = raw ?
                cache.getEntry(key).getValue() :
                cache.get(key);

            assertEquals(latest, res);
        }
    };

    /**
     *
     */
    @Test
    public void test() throws Exception {
        for (Ignite node : G.allGrids()) {
            testGetVariations(node);
            testGetNull(node);
            testContainsVariations(node);
        }
    }

    /**
     *
     */
    private void testGetVariations(Ignite initiator) throws Exception {
        testGet(initiator, 1, false); // just get
        testGet(initiator, 1, true); // 1 (all keys available at primary)
        testGet(initiator, 2, true); // less than backups
        testGet(initiator, 3, true); // equals to backups
        testGet(initiator, 4, true); // equals to backups + primary
        testGet(initiator, 10, true); // more than backups
    }

    /**
     *
     */
    private void testContainsVariations(Ignite initiator) throws Exception {
        testContains(initiator, 1, false); // just contains
        testContains(initiator, 1, true); // 1 (all keys available at primary)
        testContains(initiator, 2, true); // less than backups
        testContains(initiator, 3, true); // equals to backups
        testContains(initiator, 4, true); // equals to backups + primary
        testContains(initiator, 10, true); // more than backups
    }

    /**
     *
     */
    protected abstract void testGet(Ignite initiator, Integer cnt, boolean all) throws Exception;

    /**
     *
     */
    protected abstract void testGetNull(Ignite initiator) throws Exception;

    /**
     *
     */
    protected abstract void testContains(Ignite initiator, Integer cnt, boolean all) throws Exception;
}
