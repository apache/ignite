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

package org.apache.ignite.internal.processors.cache.transform;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Checks transformation algorithm change.
 */
@RunWith(Parameterized.class)
public class CacheObjectsTransformationEvolutionTest extends AbstractCacheObjectsTransformationTest {
    /** Atomicity mode. */
    @Parameterized.Parameter
    public CacheAtomicityMode mode;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "mode={0}")
    public static Collection<?> parameters() {
        List<Object[]> res = new ArrayList<>();

        for (CacheAtomicityMode mode : new CacheAtomicityMode[] {CacheAtomicityMode.TRANSACTIONAL, CacheAtomicityMode.ATOMIC})
            res.add(new Object[] {mode});

        return res;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cfg = super.cacheConfiguration();

        cfg.setAtomicityMode(mode);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setCacheObjectTransformer(new ControllableCacheObjectTransformer());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        ControllableCacheObjectTransformer.tCntr.clear();
        ControllableCacheObjectTransformer.rCntr.clear();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInt() throws Exception {
        doTest(i -> i);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testString() throws Exception {
        doTest(String::valueOf);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testObject() throws Exception {
        doTest(i -> new BinarizableData(String.valueOf(i), null, i));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBinaryObject() throws Exception {
        doTest(i -> {
            BinaryObjectBuilder builder = grid(0).binary().builder(BinarizableData.class.getName());

            builder.setField("str", String.valueOf(i));
            builder.setField("i", i);

            return builder.build();
        });
    }

    /**
     * @throws Exception If failed.
     */
    private void doTest(Function<Integer, Object> kvGen) throws Exception {
        prepareCluster();

        IgniteCache<Object, Object> cache =
            primaryNode(0/*any*/, CACHE_NAME).getOrCreateCache(AbstractCacheObjectsTransformationTest.CACHE_NAME);

        int cnt = 100;

        int totalCnt = 0;
        int transformCnt = 0;
        int restoreCnt = 0;

        int[] shifts = new int[] {-3, 0/*disabled*/, 7, 42};

        ThreadLocalRandom rdm = ThreadLocalRandom.current();

        Object obj = kvGen.apply(0);

        boolean binarizable = obj instanceof BinarizableData;
        boolean binary = obj instanceof BinaryObject;

        // Regular put, brandnew value.
        for (int i = 0; i < cnt; i++) {
            for (int shift : shifts) {
                ControllableCacheObjectTransformer.transformationShift(shift);

                Object kv = kvGen.apply(++key);

                cache.put(kv, kv);
            }
        }

        transformCnt += cnt; // Put on primary.

        if (binarizable || binary) // Binary array is required at backups at put (e.g. to wait for proper Metadata)
            restoreCnt += (NODES - 1) * cnt; // Put on backups.

        totalCnt += cnt;

        // Already used value.
        for (int i = 0; i < cnt; i++) {
            for (int shift : shifts) {
                ControllableCacheObjectTransformer.transformationShift(-shift);

                Object kv = kvGen.apply(++key);

                cache.put(-key, kv); // Initial put causes value transformation.

                ControllableCacheObjectTransformer.transformationShift(shift);

                cache.put(kv, kv); // Using already transformed value as a key and as a value.
            }
        }

        if (!binary) { // Binary value already marshalled using the previous shift.
            transformCnt += cnt; // Put on primary.
            totalCnt += cnt;
        }

        if (binarizable)
            // Will be transformed (and restored!) using the actual shift, while BinaryObject will keep the previous transformation result.
            restoreCnt += (NODES - 1) * cnt; // Put on backups.

        // Value got from the cache.
        for (int i = 0; i < cnt; i++) {
            for (int shift : shifts) {
                ControllableCacheObjectTransformer.transformationShift(-shift);

                Object kv = kvGen.apply(++key);

                cache.put(-key, kv);

                Object kv0 = cache.withKeepBinary().get(-key);

                ControllableCacheObjectTransformer.transformationShift(shift);

                cache.put(kv0, kv0); // Using the value which was obtained from the cache as a key and as a value.
            }
        }

        if (!binary && !binarizable) { // Binary and binarizable (objects) value already marshalled using the previous shift.
            transformCnt += cnt; // Put on primary.
            totalCnt += cnt;
        }

        // Value replace.
        for (int i = 0; i < cnt; i++) {
            for (int shift : shifts) {
                ControllableCacheObjectTransformer.transformationShift(-shift);

                Object kv = kvGen.apply(++key);

                cache.put(kv, kv);

                ControllableCacheObjectTransformer.transformationShift(shift);

                Object kv2 = kvGen.apply(key); // Brandnew kv to guarantee transtormation.

                assertEquals(kv, kv2);

                assertTrue(cache.replace(kv2, kv2, -42)); // Replacing kv via kv2 (same object, but different bytes).

                assertTrue(cache.replace(kv, kvGen.apply(key))); // Replacing -42 with generated value.
            }
        }

        if (mode == CacheAtomicityMode.TRANSACTIONAL) // Values at replace required to be marshalled
            transformCnt += cnt * 3; // [kv2 (as a value), -42, generated].
        else
            transformCnt += cnt * 2; // [-42, generated]. Atomic operation compares with previous values without transformaton.

        if (binarizable || binary) { // Binary array is required at backups at put (e.g. to wait for proper Metadata)
            if (mode == CacheAtomicityMode.TRANSACTIONAL)
                restoreCnt += (NODES - 1) * cnt * 2; // Double replace on backups (restoration of both transfered binary objects).
            else
                restoreCnt += (NODES - 1) * cnt; // Previous value will not be transfered to backups (only generated).
        }

        totalCnt += cnt;

        // Value replace via Entry Processor.
        for (int i = 0; i < cnt; i++) {
            for (int shift : shifts) {
                ControllableCacheObjectTransformer.transformationShift(-shift);

                Object kv = kvGen.apply(++key);

                cache.put(kv, kv);

                ControllableCacheObjectTransformer.transformationShift(shift);

                Object kv2 = kvGen.apply(key); // Brandnew kv to guarantee transtormation.

                assertEquals(kv, kv2);

                assertEquals(Integer.valueOf(-42),
                    cache.invoke(kv2, (e, args) -> {
                        Object val = e.getValue();
                        Object exp = kv2;

                        if (binary)
                            exp = ((BinaryObject)exp).deserialize();

                        if (Objects.equals(exp, val))
                            e.setValue(-42); // Replacing kv via kv2 (same object, but different bytes).

                        return e.getValue();
                    }));

                Object kv3 = kvGen.apply(key);

                cache.invoke(kv, (e, args) -> {
                    e.setValue(kv3); // Replacing -42 with generated value.

                    return null;
                });
            }
        }

        if (mode == CacheAtomicityMode.TRANSACTIONAL)
            transformCnt += NODES * cnt * 2; // [-42, generated], each node.
        else
            transformCnt += cnt * 2; // [-42, generated], primary.

        if (binarizable || binary) { // Binary array is required at backups at put (e.g. to wait for proper Metadata)
            if (mode == CacheAtomicityMode.ATOMIC)
                restoreCnt += (NODES - 1) * cnt; // kv3 will be restored as a part of the update message at backups.
        }

        totalCnt += cnt;

        // Checking.
        for (int shift : shifts) {
            if (shift != 0)
                assertEquals(transformCnt, ControllableCacheObjectTransformer.tCntr.get(shift).get());
            else
                assertNull(ControllableCacheObjectTransformer.tCntr.get(shift));

            if (shift != 0 && restoreCnt > 0)
                assertEquals(restoreCnt, ControllableCacheObjectTransformer.rCntr.get(shift).get());
            else
                assertNull(ControllableCacheObjectTransformer.rCntr.get(shift));
        }

        ControllableCacheObjectTransformer.tCntr.clear();
        ControllableCacheObjectTransformer.rCntr.clear();

        if (binary)
            cache = cache.withKeepBinary();

        // Get (hits/misses).
        while (key > 0) {
            ControllableCacheObjectTransformer.transformationShift(rdm.nextInt()); // Random transformation.

            Object kv = kvGen.apply(key--);
            Object val = cache.get(kv);

            assertEquals(kv, val);

            if (binary) {
                Object dKv = ((BinaryObject)kv).deserialize();
                Object dVal = ((BinaryObject)val).deserialize();

                assertEquals(dKv, dVal);
            }
        }

        // Checking.
        for (int shift : shifts) {
            assertNull(ControllableCacheObjectTransformer.tCntr.get(shift)); // No key transformations at used shifts.
            assertEquals(0, ControllableCacheObjectTransformer.tCntr.size()); // No key transformations at all.

            if (shift != 0)
                assertEquals(totalCnt, ControllableCacheObjectTransformer.rCntr.get(shift).get());
            else
                assertNull(ControllableCacheObjectTransformer.rCntr.get(shift));
        }
    }
}
