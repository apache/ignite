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

package org.apache.ignite.internal.table;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.Test;

/**
 * Basic table operations test.
 *
 * <p>TODO: IGNITE-14487 Add bulk operations tests.
 * TODO: IGNITE-14487 Add async operations tests.
 */
public class KeyValueViewOperationsSimpleSchemaTest {
    /**
     * Creates table view.
     *
     * @return Table KV-view.
     */
    private KeyValueView<Long, Long> kvView() {
        return kvViewForValueType(NativeTypes.INT64, Long.class);
    }

    @Test
    public void put() {
        KeyValueView<Long, Long> tbl = kvView();
        
        assertNull(tbl.get(1L));
        
        // Put KV pair.
        tbl.put(1L, 11L);
        
        assertEquals(11L, tbl.get(1L));
        assertEquals(11L, tbl.get(1L));
        
        // Update KV pair.
        tbl.put(1L, 22L);
        
        assertEquals(22L, tbl.get(1L));
        assertEquals(22L, tbl.get(1L));
        
        // Remove KV pair.
        tbl.put(1L, null);
        
        assertNull(tbl.get(1L));
        
        // Put KV pair.
        tbl.put(1L, 33L);
        assertEquals(33L, tbl.get(1L));
    }

    @Test
    public void putIfAbsent() {
        KeyValueView<Long, Long> tbl = kvView();
        
        assertNull(tbl.get(1L));
        
        // Insert new KV pair.
        assertTrue(tbl.putIfAbsent(1L, 11L));
        
        assertEquals(11L, tbl.get(1L));
        
        // Update KV pair.
        assertFalse(tbl.putIfAbsent(1L, 22L));
        
        assertEquals(11L, tbl.get(1L));
    }

    @Test
    public void getAndPut() {
        KeyValueView<Long, Long> tbl = kvView();
        
        assertNull(tbl.get(1L));
        
        // Insert new tuple.
        assertNull(tbl.getAndPut(1L, 11L));
        
        assertEquals(11L, tbl.get(1L));
        
        assertEquals(11L, tbl.getAndPut(1L, 22L));
        assertEquals(22L, tbl.getAndPut(1L, 33L));
        
        assertEquals(33L, tbl.get(1L));
    }

    @Test
    public void contains() {
        KeyValueView<Long, Long> tbl = kvView();
        
        // Not-existed value.
        assertFalse(tbl.contains(1L));
        
        // Put KV pair.
        tbl.put(1L, 11L);
        assertTrue(tbl.contains(1L));
        
        // Delete key.
        assertTrue(tbl.remove(1L));
        assertFalse(tbl.contains(1L));
        
        // Put KV pair.
        tbl.put(1L, 22L);
        assertTrue(tbl.contains(1L));
        
        // Delete key.
        tbl.remove(2L);
        assertFalse(tbl.contains(2L));
    }

    @Test
    public void remove() {
        KeyValueView<Long, Long> tbl = kvView();
        
        // Put KV pair.
        tbl.put(1L, 11L);
        
        // Delete existed key.
        assertEquals(11L, tbl.get(1L));
        assertTrue(tbl.remove(1L));
        assertNull(tbl.get(1L));
        
        // Delete already deleted key.
        assertFalse(tbl.remove(1L));
        
        // Put KV pair.
        tbl.put(1L, 22L);
        assertEquals(22L, tbl.get(1L));
        
        // Delete existed key.
        assertTrue(tbl.remove(1L));
        assertNull(tbl.get(1L));
        
        // Delete not existed key.
        assertNull(tbl.get(2L));
        assertFalse(tbl.remove(2L));
    }

    @Test
    public void removeExact() {
        KeyValueView<Long, Long> tbl = kvView();
        
        // Put KV pair.
        tbl.put(1L, 11L);
        assertEquals(11L, tbl.get(1L));
        
        // Fails to delete KV pair with unexpected value.
        assertFalse(tbl.remove(1L, 22L));
        assertEquals(11L, tbl.get(1L));
        
        // Delete KV pair with expected value.
        assertTrue(tbl.remove(1L, 11L));
        assertNull(tbl.get(1L));
        
        // Once again.
        assertFalse(tbl.remove(1L, 11L));
        assertNull(tbl.get(1L));
        
        // Try to remove non-existed key.
        assertFalse(tbl.remove(1L, 11L));
        assertNull(tbl.get(1L));
        
        // Put KV pair.
        tbl.put(1L, 22L);
        assertEquals(22L, tbl.get(1L));
        
        // Check null value ignored.
        assertThrows(Throwable.class, () -> tbl.remove(1L, null));
        assertEquals(22L, tbl.get(1L));
        
        // Delete KV pair with expected value.
        assertTrue(tbl.remove(1L, 22L));
        assertNull(tbl.get(1L));
        
        assertFalse(tbl.remove(2L, 22L));
        assertNull(tbl.get(2L));
    }

    @Test
    public void replace() {
        KeyValueView<Long, Long> tbl = kvView();
        
        // Ignore replace operation for non-existed KV pair.
        assertFalse(tbl.replace(1L, 11L));
        assertNull(tbl.get(1L));
        
        tbl.put(1L, 11L);
        
        // Replace existed KV pair.
        assertTrue(tbl.replace(1L, 22L));
        assertEquals(22L, tbl.get(1L));
        
        // Remove existed KV pair.
        assertTrue(tbl.replace(1L, null));
        assertNull(tbl.get(1L));
        
        // Ignore replace operation for non-existed KV pair.
        assertFalse(tbl.replace(1L, 33L));
        assertNull(tbl.get(1L));
        
        tbl.put(1L, 33L);
        assertEquals(33L, tbl.get(1L));
        
        // Remove non-existed KV pair.
        assertFalse(tbl.replace(2L, null));
        assertNull(tbl.get(2L));
    }

    @Test
    public void replaceExact() {
        KeyValueView<Long, Long> tbl = kvView();
        
        // Insert KV pair.
        assertTrue(tbl.replace(1L, null, 11L));
        assertEquals(11L, tbl.get(1L));
        assertNull(tbl.get(2L));
        
        // Ignore replace operation for non-existed KV pair.
        assertFalse(tbl.replace(2L, 11L, 22L));
        assertNull(tbl.get(2L));
        
        // Replace existed KV pair.
        assertTrue(tbl.replace(1L, 11L, 22L));
        assertEquals(22L, tbl.get(1L));
        
        // Remove existed KV pair.
        assertTrue(tbl.replace(1L, 22L, null));
        assertNull(tbl.get(1L));
        
        // Insert KV pair.
        assertTrue(tbl.replace(1L, null, 33L));
        assertEquals(33L, tbl.get(1L));
        
        // Remove non-existed KV pair.
        assertTrue(tbl.replace(2L, null, null));
    }
    
    @Test
    public void putGetAllTypes() {
        Random rnd = new Random();
        Long key = 42L;
        
        List<NativeType> allTypes = List.of(
                NativeTypes.INT8,
                NativeTypes.INT16,
                NativeTypes.INT32,
                NativeTypes.INT64,
                NativeTypes.FLOAT,
                NativeTypes.DOUBLE,
                NativeTypes.DATE,
                NativeTypes.UUID,
                NativeTypes.numberOf(20),
                NativeTypes.decimalOf(25, 5),
                NativeTypes.bitmaskOf(22),
                NativeTypes.time(),
                NativeTypes.datetime(),
                NativeTypes.timestamp(),
                NativeTypes.BYTES,
                NativeTypes.STRING);
        
        // Validate all types are tested.
        assertEquals(Set.of(NativeTypeSpec.values()),
                allTypes.stream().map(NativeType::spec).collect(Collectors.toSet()));
        
        for (NativeType type : allTypes) {
            final Object val = SchemaTestUtils.generateRandomValue(rnd, type);
            
            assertFalse(type.mismatch(NativeTypes.fromObject(val)));
            
            KeyValueViewImpl<Long, Object> kvView = kvViewForValueType(NativeTypes.fromObject(val),
                    (Class<Object>) val.getClass());
            
            kvView.put(key, val);
    
            if (val instanceof byte[]) {
                assertArrayEquals((byte[]) val, (byte[]) kvView.get(key));
            } else {
                assertEquals(val, kvView.get(key));
            }
        }
    }
    
    /**
     * Creates key-value view.
     *
     * @param type   Value column native type.
     * @param valueClass Value class.
     */
    private <T> KeyValueViewImpl<Long, T> kvViewForValueType(NativeType type, Class<T> valueClass) {
        Mapper<Long> keyMapper = Mapper.identity(Long.class);
        Mapper<T> valMapper = Mapper.identity(valueClass);
        
        SchemaDescriptor schema = new SchemaDescriptor(
                1,
                new Column[]{new Column("id", NativeTypes.INT64, false)},
                new Column[]{new Column("val", type, false)}
        );
        
        return new KeyValueViewImpl<>(
                new DummyInternalTableImpl(),
                new DummySchemaManagerImpl(schema),
                keyMapper,
                valMapper,
                null
        );
    }
}
