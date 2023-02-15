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
import java.util.Collections;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.cache.transform.AbstractCacheObjectsCompressionTest.CompressionTransformer.CompressionType;

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

            putAndGet(i, false); // No chances to compress integer.

            String str = "Test string";

            putAndGet(str, false); // Too short string.

            StringData sd = new StringData(str);

            putAndGet(sd, false); // Too short wrapped string.

            List<Object> sdList = Collections.singletonList(sd);

            putAndGet(sdList, false); // Too short wrapped string.

            StringBuilder sb = new StringBuilder();

            for (int k = 0; k < 100; k++)
                sb.append("AAAAAAAAAA");

            String str2 = sb.toString();

            putAndGet(str2, type != CompressionType.DISABLED);

            List<Object> list = new ArrayList<>();

            list.add(new BinarizableData(str, null, i));

            putAndGet(list, false); // Too short list.

            // Adding more elements.
            list.add(new BinarizableData(str, null, i));
            list.add(new BinarizableData(str, null, i));

            putAndGet(list, type != CompressionType.DISABLED); // Enough to be compressed.

            BinarizableData data = new BinarizableData(str, list, i);

            putAndGet(data, type != CompressionType.DISABLED);

            BinaryObjectBuilder builder = ignite.binary().builder(BinarizableData.class.getName());

            builder.setField("str", str2); // Wrapped string, enough to be compressed.
            builder.setField("list", list); // Wrapped strings, enough to be compressed.
            builder.setField("i", i);

            putAndGet(builder.build(), type != CompressionType.DISABLED); // Enough to be compressed.

            builder.setField("str", str);

            putAndGet(builder.build(), type != CompressionType.DISABLED); // Still enough to be compressed.

            builder.setField("list", null);

            putAndGet(builder.build(), false); // Too short wrapped string.
        }
        finally {
            CompressionTransformer.type = CompressionTransformer.CompressionType.defaultType();  // Restoring default.
        }
    }

    /**
     *
     */
    private void putAndGet(Object val, boolean compressible) {
        putAndGet(val, compressible, false);
    }
}
