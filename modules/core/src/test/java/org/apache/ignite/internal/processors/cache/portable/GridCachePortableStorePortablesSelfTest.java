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
package org.apache.ignite.internal.processors.cache.portable;

import org.apache.ignite.portable.*;

import java.util.*;

/**
 * Tests for cache store with portables.
 */
public class GridCachePortableStorePortablesSelfTest extends GridCachePortableStoreAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected boolean keepPortableInStore() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void populateMap(Map<Object, Object> map, int... idxs) {
        assert map != null;
        assert idxs != null;

        for (int idx : idxs)
            map.put(portable(new Key(idx)), portable(new Value(idx)));
    }

    /** {@inheritDoc} */
    @Override protected void checkMap(Map<Object, Object> map, int... idxs) {
        assert map != null;
        assert idxs != null;

        assertEquals(idxs.length, map.size());

        for (int idx : idxs) {
            Object val = map.get(portable(new Key(idx)));

            assertTrue(String.valueOf(val), val instanceof PortableObject);

            PortableObject po = (PortableObject)val;

            assertEquals("Value", po.metaData().typeName());
            assertEquals(new Integer(idx), po.field("idx"));
        }
    }

    /**
     * @param obj Object.
     * @return Portable object.
     */
    private Object portable(Object obj) {
        return grid().portables().toPortable(obj);
    }
}
