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
package org.apache.ignite.internal.processors.cache.binary;

import java.util.Map;
import org.apache.ignite.binary.BinaryNameMapper;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.binary.BinaryContext;

/**
 * Tests for cache store with binary.
 */
public class GridCacheBinaryStoreBinariesDefaultMappersSelfTest extends GridCacheBinaryStoreAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected boolean keepBinaryInStore() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void populateMap(Map<Object, Object> map, int... idxs) {
        assert map != null;
        assert idxs != null;

        for (int idx : idxs)
            map.put(binary(new Key(idx)), binary(new Value(idx)));
    }

    /** {@inheritDoc} */
    @Override protected void checkMap(Map<Object, Object> map, int... idxs) {
        assert map != null;
        assert idxs != null;

        assertEquals(idxs.length, map.size());

        for (int idx : idxs) {
            Object val = map.get(binary(new Key(idx)));

            assertTrue(String.valueOf(val), val instanceof BinaryObject);

            BinaryObject po = (BinaryObject)val;

            assertEquals(expectedTypeName(Value.class.getName()), po.type().typeName());
            assertEquals(new Integer(idx), po.field("idx"));
        }
    }

    /**
     * @param clsName Class name.
     * @return Type name.
     */
    private String expectedTypeName(String clsName) {
        BinaryNameMapper nameMapper = cfg.getBinaryConfiguration().getNameMapper();

        if (nameMapper == null)
            nameMapper = BinaryContext.defaultNameMapper();

        return nameMapper.typeName(clsName);
    }

    /**
     * @param obj Object.
     * @return Binary object.
     */
    private Object binary(Object obj) {
        return grid().binary().toBinary(obj);
    }
}
