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

package org.apache.ignite.internal.metric;

import java.lang.reflect.Field;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.mutabletest.GridBinaryTestClasses.TestObjectAllTypes;
import org.apache.ignite.internal.binary.mutabletest.GridBinaryTestClasses.TestObjectEnum;
import org.apache.ignite.spi.systemview.view.BinaryMetadataView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl.BINARY_METADATA_VIEW;

/** Tests for {@link SystemView} for binary meta. */
public class SystemViewBinaryMetaTest extends SystemViewAbstractTest {
    /** */
    @Test
    public void testBinaryMeta() throws Exception {
        try (IgniteEx g = startGrid(0)) {
            IgniteCache<Integer, TestObjectAllTypes> c1 = g.createCache("test-cache");
            IgniteCache<Integer, TestObjectEnum> c2 = g.createCache("test-enum-cache");

            c1.put(1, new TestObjectAllTypes());
            c2.put(1, TestObjectEnum.A);

            SystemView<BinaryMetadataView> view = g.context().systemView().view(BINARY_METADATA_VIEW);

            assertNotNull(view);
            assertEquals(2, view.size());

            for (BinaryMetadataView meta : view) {
                if (TestObjectEnum.class.getName().contains(meta.typeName())) {
                    assertTrue(meta.isEnum());

                    assertEquals(0, meta.fieldsCount());
                }
                else {
                    assertFalse(meta.isEnum());

                    Field[] fields = TestObjectAllTypes.class.getDeclaredFields();

                    assertEquals(fields.length, meta.fieldsCount());

                    for (Field field : fields)
                        assertTrue(meta.fields().contains(field.getName()));
                }
            }
        }
    }
}
