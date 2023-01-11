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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import com.google.common.collect.Lists;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.transform.CacheObjectTransformerSpiAdapter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 */
@RunWith(Parameterized.class)
public class CacheObjectsTransformationTest extends AbstractCacheObjectsTransformationTest {
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
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheObjectTransformSpi(new ControllableCacheObjectTransformerSpi());
    }


    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cfg = super.cacheConfiguration();

        cfg.setAtomicityMode(mode);

        return cfg;
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
            ControllableCacheObjectTransformerSpi.fail = true;

            doTest();
        }
        finally {
            ControllableCacheObjectTransformerSpi.fail = false;
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

        putAndCheck(i);
        putAndCheck(is);
        putAndCheck(iList);

        String str = "test";
        String[] strs = new String[] {str, str, str};
        List<String> strList = Lists.newArrayList(str, str, str);

        putAndCheck(str);
        putAndCheck(strs);
        putAndCheck(strList);

        BinarizableData data = new BinarizableData(str, Lists.newArrayList(i, i, i), i);
        BinarizableData[] datas = new BinarizableData[] {data, data, data};
        List<BinarizableData> dataList = Lists.newArrayList(data, data, data);

        putAndCheck(data);
        putAndCheck(datas);
        putAndCheck(dataList);

        BinarizableData ddata = new BinarizableData(str, Lists.newArrayList(i, i, i), i, data);
        BinarizableData[] ddatas = new BinarizableData[] {ddata, ddata, ddata};
        List<BinarizableData> ddataList = Lists.newArrayList(ddata, ddata, ddata);

        putAndCheck(ddata);
        putAndCheck(ddatas);
        putAndCheck(ddataList);

        BinaryObjectBuilder builder = ignite.binary().builder(BinarizableData.class.getName());

        builder.setField("str", str + "!");
        builder.setField("map", Collections.singletonMap(i, i));
        builder.setField("i", i);

        BinaryObject bo = builder.build();
        BinaryObject[] bos = new BinaryObject[] {bo, bo, bo};
        List<BinaryObject> boList = Lists.newArrayList(bo, bo, bo);

        putAndCheck(bo);
        putAndCheck(bos);
        putAndCheck(boList);

        builder.setField("data", data);

        BinaryObject dbo = builder.build();
        BinaryObject[] dbos = new BinaryObject[] {dbo, dbo, dbo};
        List<BinaryObject> dboList = Lists.newArrayList(dbo, dbo, dbo);

        putAndCheck(dbo);
        putAndCheck(dbos);
        putAndCheck(dboList);
    }

    /**
     * @param obj Object.
     */
    private void putAndCheck(Object obj) {
        for (boolean reversed : new boolean[] {true, false})
            putAndCheck(
                obj,
                !ControllableCacheObjectTransformerSpi.fail,
                !ControllableCacheObjectTransformerSpi.fail,
                reversed);
    }

    /**
     *
     */
    private static final class ControllableCacheObjectTransformerSpi extends CacheObjectTransformerSpiAdapter {
        /** Shift. */
        private static final int SHIFT = 42;

        /** Fail. */
        private static volatile boolean fail;

        /** {@inheritDoc} */
        @Override protected ByteBuffer transform(ByteBuffer original) throws IgniteCheckedException {
            if (fail)
                throw new IgniteCheckedException("Failed.");

            ByteBuffer transformed = byteBuffer(original.remaining()); // Same capacity is required.

            while (original.hasRemaining())
                transformed.put((byte)(original.get() + SHIFT));

            transformed.flip();

            return transformed;
        }

        /** {@inheritDoc} */
        @Override protected ByteBuffer restore(ByteBuffer transformed, int length) {
            ByteBuffer restored = byteBuffer(length);

            while (transformed.hasRemaining())
                restored.put((byte)(transformed.get() - SHIFT));

            restored.flip();

            return restored;
        }
    }
}
