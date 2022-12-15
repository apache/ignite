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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import com.google.common.collect.Lists;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.transform.CacheObjectsTransformer;
import org.junit.Test;

/**
 *
 */
public class CacheObjectsTransformationTest extends AbstractCacheObjectsTransformationTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheObjectsTransformSpi(new CacheObjectsTransformSpiAdapter() {
                @Override public CacheObjectsTransformer transformer(CacheConfiguration<?, ?> ccfg) {
                    return new ControllableCacheObjectsTransformer();
                }
            });
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransformable() throws Exception {
        doTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUntransformable() throws Exception {
        try {
            ControllableCacheObjectsTransformer.fail = true;

            doTest();
        }
        finally {
            ControllableCacheObjectsTransformer.fail = false;
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void doTest() throws Exception {
        Ignite ignite = prepareCluster();

        int i = -42; // Avoiding intersection with an incremental key.
        int[] is = new int[] {i, i, i};
        List<Integer> iList = Lists.newArrayList(i, i, i);

        putAndCheck(i, false, false);
        putAndCheck(is, false, false);
        putAndCheck(iList, false, false);

        String str = "test";
        String[] strs = new String[] {str, str, str};
        List<String> strList = Lists.newArrayList(str, str, str);

        putAndCheck(str, false, false);
        putAndCheck(strs, false, false);
        putAndCheck(strList, false, false);

        BinarizableData data = new BinarizableData(str, Lists.newArrayList(i, i, i), i);
        BinarizableData[] datas = new BinarizableData[] {data, data, data};
        List<BinarizableData> dataList = Lists.newArrayList(data, data, data);

        putAndCheck(data, true, false);
        putAndCheck(datas, false, true);
        putAndCheck(dataList, false, true);

        BinarizableData ddata = new BinarizableData(str, Lists.newArrayList(i, i, i), i, data);
        BinarizableData[] ddatas = new BinarizableData[] {ddata, ddata, ddata};
        List<BinarizableData> ddataList = Lists.newArrayList(ddata, ddata, ddata);

        putAndCheck(ddata, true, false);
        putAndCheck(ddatas, false, true);
        putAndCheck(ddataList, false, true);

        BinaryObjectBuilder builder = ignite.binary().builder(BinarizableData.class.getName());

        builder.setField("str", str + "!");
        builder.setField("map", Collections.singletonMap(i, i));
        builder.setField("i", i);

        BinaryObject bo = builder.build();
        BinaryObject[] bos = new BinaryObject[] {bo, bo, bo};
        List<BinaryObject> boList = Lists.newArrayList(bo, bo, bo);

        putAndCheck(bo, true, false);
        putAndCheck(bos, false, true);
        putAndCheck(boList, false, true);

        builder.setField("data", data);

        BinaryObject dbo = builder.build();
        BinaryObject[] dbos = new BinaryObject[] {dbo, dbo, dbo};
        List<BinaryObject> dboList = Lists.newArrayList(dbo, dbo, dbo);

        putAndCheck(dbo, true, false);
        putAndCheck(dbos, false, true);
        putAndCheck(dboList, false, true);
    }

    /**
     * @param obj Object.
     * @param binarizable Binarizable.
     */
    private void putAndCheck(Object obj, boolean binarizable, boolean binarizableCol) {
        for (boolean reversed : new boolean[] {true, false})
            putAndCheck(
                obj,
                binarizable,
                binarizableCol,
                !ControllableCacheObjectsTransformer.fail,
                !ControllableCacheObjectsTransformer.fail,
                reversed);
    }

    /**
     *
     */
    private static final class ControllableCacheObjectsTransformer implements CacheObjectsTransformer {
        /** Fail. */
        private static boolean fail;

        /** {@inheritDoc} */
        @Override public int transform(ByteBuffer original, ByteBuffer transformed, int ignored) throws IgniteCheckedException {
            if (fail)
                throw new IgniteCheckedException("Failed.");

            if (transformed.capacity() < original.remaining()) // At least same capacity is required.
                return original.remaining();

            while (original.hasRemaining())
                transformed.put((byte)(original.get() + 1));

            transformed.flip();

            return 0;
        }

        /** {@inheritDoc} */
        @Override public void restore(ByteBuffer transformed, ByteBuffer restored) {
            while (transformed.hasRemaining())
                restored.put((byte)(transformed.get() - 1));

            restored.flip();
        }
    }
}
