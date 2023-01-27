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

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.Test;

/**
 * Checks transformation algorithm change.
 */
public class CacheObjectsTransformationEvolutionTest extends AbstractCacheObjectsTransformationTest {
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


    //TODO TX, Atomic

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
        Ignite ignite = prepareCluster();

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(AbstractCacheObjectsTransformationTest.CACHE_NAME);

        int cnt = 1000;

        int totalCnt = 0;
        int tCnt = 0;
        int rCnt = 0;

        int[] shifts = new int[] {-3, 0/*disabled*/, 7, 42};

        boolean binarizable = false;
        boolean binary = false;

        ThreadLocalRandom rdm = ThreadLocalRandom.current();

        // Regular put, brandnew value.
        for (int i = 0; i < cnt; i++) {
            for (int shift : shifts) {
                ControllableCacheObjectTransformer.transformationShift(shift);

                Object kv = kvGen.apply(++key);

                binarizable = kv instanceof BinarizableData;
                binary = kv instanceof BinaryObject;

                cache.put(kv, kv);
            }
        }

        tCnt += cnt; // Put on primary.

        if (binarizable || binary) // Binary array is required at backups at put (e.g. to wait for proper Metadata)
            rCnt += (NODES - 1) * cnt; // Put on backups.

        totalCnt += cnt;

        // Already used value.
        for (int i = 0; i < cnt; i++) {
            for (int shift : shifts) {
                ControllableCacheObjectTransformer.transformationShift(-shift);

                Object kv = kvGen.apply(++key);

                cache.put(-key, kv); // Initial put causes transformation.

                ControllableCacheObjectTransformer.transformationShift(shift);

                cache.put(kv, kv); // Checking value shift change && key untransforming (othervice will be null on checking get).
            }
        }

        if (!binary) { // Binary value already marshalled using the previous shift.
            tCnt += cnt; // Put on primary.
            totalCnt += cnt;
        }

        if (binarizable) // Will be transformed (and restored!) using the actual shift, while BinaryObject will keep the previous result.
            rCnt += (NODES - 1) * cnt; // Put on backups.

        // Value got from the cache.
        for (int i = 0; i < cnt; i++) {
            for (int shift : shifts) {
                ControllableCacheObjectTransformer.transformationShift(-shift);

                Object kv = kvGen.apply(++key);

                cache.put(-key, kv);

                Object kv0 = cache.withKeepBinary().get(-key);

                ControllableCacheObjectTransformer.transformationShift(shift);

                cache.put(kv0, kv0); // Checking value shift change && key untransforming (othervice will be null on checking get).
            }
        }

        if (!binary && !binarizable) { // Binary value already marshalled using the previous shift.
            tCnt += cnt; // Put on primary.
            totalCnt += cnt;
        }

        for (int shift : shifts) {
            if (shift != 0)
                assertEquals(tCnt, ControllableCacheObjectTransformer.tCntr.get(shift).get());
            else
                assertNull(ControllableCacheObjectTransformer.tCntr.get(shift));

            if (shift != 0 && rCnt > 0)
                assertEquals(rCnt, ControllableCacheObjectTransformer.rCntr.get(shift).get());
            else
                assertNull(ControllableCacheObjectTransformer.rCntr.get(shift));
        }

        ControllableCacheObjectTransformer.tCntr.clear();
        ControllableCacheObjectTransformer.rCntr.clear();

        if (binary)
            cache = cache.withKeepBinary();

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
