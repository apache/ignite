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

package org.apache.ignite.internal.processors.cache.objects;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.cache.objects.AbstractCacheObjectsCompressionTest.CompressionTransformer.CompressionType;

/**
 *
 */
@RunWith(Parameterized.class)
public class CacheObjectsCompressionTest extends AbstractCacheObjectsCompressionTest {
    /** Thin client. */
    @Parameterized.Parameter
    public CompressionType type;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "type={0}")
    public static Collection<?> parameters() {
        List<Object[]> res = new ArrayList<>();

        for (CompressionType type : CompressionType.values())
            res.add(new Object[] {type});

        return res;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCompression() throws Exception {
        try {
            CompressionTransformer.type = type;

            Ignite ignite = prepareCluster();

            int i = 42;
            String str = "Test string";

            putAndCheck(str, false, false, false);

            StringData sd = new StringData("");

            putAndCheck(sd, true, false, false);

            List<Object> sdList = Collections.singletonList(sd);

            putAndCheck(sdList, false, true, false);

            StringBuilder sb = new StringBuilder();

            for (int k = 0; k < 100; k++)
                sb.append("AAAAAAAAAA");

            String str2 = sb.toString();

            putAndCheck(str2, false, false, type != CompressionType.DISABLED);

            List<Object> list = new ArrayList<>();

            list.add(new BinarizableData(str, null, i));
            list.add(new BinarizableData(str, null, i));
            list.add(new BinarizableData(str, null, i));

            putAndCheck(list, false, true, type != CompressionType.DISABLED);

            BinarizableData data = new BinarizableData(str, list, i);

            putAndCheck(data, true, false, type != CompressionType.DISABLED);

            List<Object> list2 = new ArrayList<>();

            list2.add(new BinarizableData(str, null, i + 1));
            list2.add(new BinarizableData(str, null, i + 1));
            list2.add(new BinarizableData(str, null, i + 1));

            putAndCheck(list2, false, true, type != CompressionType.DISABLED);

            BinarizableData data2 = new BinarizableData(str, list2, i + 1);

            putAndCheck(data2, true, false, type != CompressionType.DISABLED);

            BinaryObjectBuilder builder = ignite.binary().builder(BinarizableData.class.getName());

            builder.setField("str", str2);
            builder.setField("list", list);
            builder.setField("i", i);

            putAndCheck(builder.build(), true, false, type != CompressionType.DISABLED);

            builder.setField("str", str);

            putAndCheck(builder.build(), true, false, type != CompressionType.DISABLED);
        }
        finally {
            CompressionTransformer.type = CompressionTransformer.CompressionType.defaultType();  // Restoring default.
        }
    }

    /**
     *
     */
    private void putAndCheck(Object obj, boolean binarizable, boolean binarizableCol, boolean compressible) {
        putAndCheck(obj, binarizable, binarizableCol, false, compressible, false);
    }
}
