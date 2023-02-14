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
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
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
        return super.getConfiguration(igniteInstanceName).setCacheObjectTransformer(new ControllableCacheObjectTransformer());
    }


    /** {@inheritDoc} */
    @Override protected CacheConfiguration<?,?> cacheConfiguration() {
        CacheConfiguration<?,?> cfg = super.cacheConfiguration();

        cfg.setAtomicityMode(mode);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransformable() throws Exception {
        ControllableCacheObjectTransformer.transformationShift(42);

        assertFalse(ControllableCacheObjectTransformer.failOnTransformation());

        doTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUntransformable() throws Exception {
        ControllableCacheObjectTransformer.transformationShift(0); // Fail on transformation.

        assertTrue(ControllableCacheObjectTransformer.failOnTransformation());

        doTest();
    }

    /**
     * @throws Exception If failed.
     */
    private void doTest() throws Exception {
        Ignite ignite = prepareCluster();

        int i = -42; // Avoiding intersection with an incremental key.
        int[] is = new int[] {i, i, i};
        List<Integer> iList = F.asList(i, i, i);

        putAndCheck(i);
        putAndCheck(is);
        putAndCheck(iList);

        String str = "test";
        String[] strs = new String[] {str, str, str};
        List<String> strList = F.asList(str, str, str);

        putAndCheck(str);
        putAndCheck(strs);
        putAndCheck(strList);

        BinarizableData data = new BinarizableData(str, F.asList(i, i, i), i);
        BinarizableData[] datas = new BinarizableData[] {data, data, data};
        List<BinarizableData> dataList = F.asList(data, data, data);

        putAndCheck(data);
        putAndCheck(datas);
        putAndCheck(dataList);

        BinarizableData ddata = new BinarizableData(str, F.asList(i, i, i), i, data);
        BinarizableData[] ddatas = new BinarizableData[] {ddata, ddata, ddata};
        List<BinarizableData> ddataList = F.asList(ddata, ddata, ddata);

        putAndCheck(ddata);
        putAndCheck(ddatas);
        putAndCheck(ddataList);

        BinaryObjectBuilder builder = ignite.binary().builder(BinarizableData.class.getName());

        builder.setField("str", str + "!");
        builder.setField("list", F.asList(i, i));
        builder.setField("i", i);

        BinaryObject bo = builder.build();
        BinaryObject[] bos = new BinaryObject[] {bo, bo, bo};
        List<BinaryObject> boList = F.asList(bo, bo, bo);

        putAndCheck(bo);
        putAndCheck(bos);
        putAndCheck(boList);

        builder.setField("data", data);

        BinaryObject dbo = builder.build();
        BinaryObject[] dbos = new BinaryObject[] {dbo, dbo, dbo};
        List<BinaryObject> dboList = F.asList(dbo, dbo, dbo);

        putAndCheck(dbo);
        putAndCheck(dbos);
        putAndCheck(dboList);
    }

    /**
     * @param val Value.
     */
    private void putAndCheck(Object val) {
        for (boolean reversed : new boolean[] {true, false})
            putAndCheck(val, !ControllableCacheObjectTransformer.failOnTransformation(), reversed);
    }
}
