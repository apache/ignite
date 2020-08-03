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
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.near.consistency.IgniteConsistencyViolationException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Ideally, this test should be equal to {@link ImplicitTransactionalReadRepairTest} except atomicy mode
 * configuration.
 *
 * Implement repair on atomic caches to make this happen :)
 */
@RunWith(Parameterized.class)
public class AtomicReadRepairTest extends ImplicitTransactionalReadRepairTest {
    /**
     *
     */
    private static final Consumer<ReadRepairData> GET_CHECK_AND_FAIL = (data) -> {
        IgniteCache<Integer, Integer> cache = data.cache;
        Set<Integer> keys = data.data.keySet();
        boolean raw = data.raw;
        boolean async = data.async;

        assert keys.size() == 1;

        for (Map.Entry<Integer, InconsistentMapping> entry : data.data.entrySet()) { // Once.
            try {
                Integer key = entry.getKey();

                Integer res =
                    raw ?
                        async ?
                            cache.withReadRepair().getEntryAsync(key).get().getValue() :
                            cache.withReadRepair().getEntry(key).getValue() :
                        async ?
                            cache.withReadRepair().getAsync(key).get() :
                            cache.withReadRepair().get(key);

                fail("Should not happen.");
            }
            catch (CacheException e) {
                assertTrue(e.getCause() instanceof IgniteConsistencyViolationException);
            }
        }
    };

    /**
     *
     */
    private static final Consumer<ReadRepairData> GETALL_CHECK_AND_FAIL = (data) -> {
        IgniteCache<Integer, Integer> cache = data.cache;
        Set<Integer> keys = data.data.keySet();
        boolean raw = data.raw;
        boolean async = data.async;

        assert !keys.isEmpty();

        try {
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

            fail("Should not happen.");
        }
        catch (CacheException e) {
            assertTrue(e.getCause() instanceof IgniteConsistencyViolationException);
        }
    };

    /**
     *
     */
    private static final Consumer<ReadRepairData> CONTAINS_CHECK_AND_FAIL = (data) -> {
        IgniteCache<Integer, Integer> cache = data.cache;
        Set<Integer> keys = data.data.keySet();
        boolean async = data.async;

        assert keys.size() == 1;

        for (Map.Entry<Integer, InconsistentMapping> entry : data.data.entrySet()) { // Once.
            try {
                Integer key = entry.getKey();

                boolean res = async ?
                    cache.withReadRepair().containsKeyAsync(key).get() :
                    cache.withReadRepair().containsKey(key);

                fail("Should not happen.");
            }
            catch (Exception e) {
                assertTrue(e.getCause() instanceof IgniteConsistencyViolationException);
            }
        }
    };

    /**
     *
     */
    private static final Consumer<ReadRepairData> CONTAINS_ALL_CHECK_AND_FAIL = (data) -> {
        IgniteCache<Integer, Integer> cache = data.cache;
        Set<Integer> keys = data.data.keySet();
        boolean async = data.async;

        try {
            boolean res = async ?
                cache.withReadRepair().containsKeysAsync(keys).get() :
                cache.withReadRepair().containsKeys(keys);

            fail("Should not happen.");
        }
        catch (Exception e) {
            assertTrue(e.getCause() instanceof IgniteConsistencyViolationException);
        }
    };

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicyMode() {
        return CacheAtomicityMode.ATOMIC;
    }

    /** {@inheritDoc} */
    @Override protected void testGet(Ignite initiator, Integer cnt, boolean all) throws Exception {
        prepareAndCheck(
            initiator,
            cnt,
            raw,
            async,
            (ReadRepairData data) -> {
                if (all)
                    GETALL_CHECK_AND_FAIL.accept(data);
                else
                    GET_CHECK_AND_FAIL.accept(data);
            });
    }

    /** {@inheritDoc} */
    @Override protected void testContains(Ignite initiator, Integer cnt, boolean all) throws Exception {
        prepareAndCheck(
            initiator,
            cnt,
            raw,
            async,
            (ReadRepairData data) -> {
                if (all)
                    CONTAINS_ALL_CHECK_AND_FAIL.accept(data);
                else
                    CONTAINS_CHECK_AND_FAIL.accept(data);
            });
    }
}
