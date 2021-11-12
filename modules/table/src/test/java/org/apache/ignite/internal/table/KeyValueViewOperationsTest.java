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

import static org.apache.ignite.internal.schema.NativeTypes.BYTES;
import static org.apache.ignite.internal.schema.NativeTypes.DATE;
import static org.apache.ignite.internal.schema.NativeTypes.DOUBLE;
import static org.apache.ignite.internal.schema.NativeTypes.FLOAT;
import static org.apache.ignite.internal.schema.NativeTypes.INT16;
import static org.apache.ignite.internal.schema.NativeTypes.INT32;
import static org.apache.ignite.internal.schema.NativeTypes.INT64;
import static org.apache.ignite.internal.schema.NativeTypes.INT8;
import static org.apache.ignite.internal.schema.NativeTypes.STRING;
import static org.apache.ignite.internal.schema.NativeTypes.datetime;
import static org.apache.ignite.internal.schema.NativeTypes.time;
import static org.apache.ignite.internal.schema.NativeTypes.timestamp;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.testobjects.TestObjectWithAllTypes;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.Test;

/**
 * Basic table operations test.
 */
//TODO: IGNITE-14487 Add bulk operations tests.
//TODO: IGNITE-14487 Add async operations tests.
public class KeyValueViewOperationsTest {
    
    private final Random rnd = new Random();
    
    @Test
    public void put() {
        final TestKeyObject key = TestKeyObject.randomObject(rnd);
        final TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes obj2 = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes obj3 = TestObjectWithAllTypes.randomObject(rnd);
        
        KeyValueView<TestKeyObject, TestObjectWithAllTypes> tbl = kvView();
        
        assertNull(tbl.get(key));
        
        // Put KV pair.
        tbl.put(key, obj);
        
        assertEquals(obj, tbl.get(key));
        assertEquals(obj, tbl.get(key));
        
        // Update KV pair.
        tbl.put(key, obj2);
        
        assertEquals(obj2, tbl.get(key));
        assertEquals(obj2, tbl.get(key));
        
        // Remove KV pair.
        tbl.put(key, null);
        
        assertNull(tbl.get(key));
        
        // Put KV pair.
        tbl.put(key, obj3);
        assertEquals(obj3, tbl.get(key));
    }
    
    @Test
    public void putIfAbsent() {
        final TestKeyObject key = TestKeyObject.randomObject(rnd);
        final TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes obj2 = TestObjectWithAllTypes.randomObject(rnd);
        
        KeyValueView<TestKeyObject, TestObjectWithAllTypes> tbl = kvView();
        
        assertNull(tbl.get(key));
        
        // Insert new KV pair.
        assertTrue(tbl.putIfAbsent(key, obj));
        
        assertEquals(obj, tbl.get(key));
        
        // Update KV pair.
        assertFalse(tbl.putIfAbsent(key, obj2));
        
        assertEquals(obj, tbl.get(key));
    }
    
    @Test
    public void getAndPut() {
        final TestKeyObject key = TestKeyObject.randomObject(rnd);
        final TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes obj2 = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes obj3 = TestObjectWithAllTypes.randomObject(rnd);
        
        KeyValueView<TestKeyObject, TestObjectWithAllTypes> tbl = kvView();
        
        assertNull(tbl.get(key));
        
        // Insert new tuple.
        assertNull(tbl.getAndPut(key, obj));
        
        assertEquals(obj, tbl.get(key));
        
        assertEquals(obj, tbl.getAndPut(key, obj2));
        assertEquals(obj2, tbl.getAndPut(key, obj3));
        
        assertEquals(obj3, tbl.get(key));
    }
    
    @Test
    public void contains() {
        final TestKeyObject key = TestKeyObject.randomObject(rnd);
        final TestKeyObject key2 = TestKeyObject.randomObject(rnd);
        final TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes obj2 = TestObjectWithAllTypes.randomObject(rnd);
    
        KeyValueView<TestKeyObject, TestObjectWithAllTypes> tbl = kvView();
        
        // Not-existed value.
        assertFalse(tbl.contains(key));
        
        // Put KV pair.
        tbl.put(key, obj);
        assertTrue(tbl.contains(key));
        
        // Delete key.
        assertTrue(tbl.remove(key));
        assertFalse(tbl.contains(key));
        
        // Put KV pair.
        tbl.put(key, obj2);
        assertTrue(tbl.contains(key));
        
        // Delete key.
        tbl.remove(key2);
        assertFalse(tbl.contains(key2));
    }
    
    @Test
    public void remove() {
        final TestKeyObject key = TestKeyObject.randomObject(rnd);
        final TestKeyObject key2 = TestKeyObject.randomObject(rnd);
        final TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes obj2 = TestObjectWithAllTypes.randomObject(rnd);
        
        KeyValueView<TestKeyObject, TestObjectWithAllTypes> tbl = kvView();
        
        // Put KV pair.
        tbl.put(key, obj);
        
        // Delete existed key.
        assertEquals(obj, tbl.get(key));
        assertTrue(tbl.remove(key));
        assertNull(tbl.get(key));
        
        // Delete already deleted key.
        assertFalse(tbl.remove(key));
        
        // Put KV pair.
        tbl.put(key, obj2);
        assertEquals(obj2, tbl.get(key));
        
        // Delete existed key.
        assertTrue(tbl.remove(key));
        assertNull(tbl.get(key));
        
        // Delete not existed key.
        assertNull(tbl.get(key2));
        assertFalse(tbl.remove(key2));
    }
    
    @Test
    public void removeExact() {
        final TestKeyObject key = TestKeyObject.randomObject(rnd);
        final TestKeyObject key2 = TestKeyObject.randomObject(rnd);
        final TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes obj2 = TestObjectWithAllTypes.randomObject(rnd);
        
        KeyValueView<TestKeyObject, TestObjectWithAllTypes> tbl = kvView();
        
        // Put KV pair.
        tbl.put(key, obj);
        assertEquals(obj, tbl.get(key));
        
        // Fails to delete KV pair with unexpected value.
        assertFalse(tbl.remove(key, obj2));
        assertEquals(obj, tbl.get(key));
        
        // Delete KV pair with expected value.
        assertTrue(tbl.remove(key, obj));
        assertNull(tbl.get(key));
        
        // Once again.
        assertFalse(tbl.remove(key, obj));
        assertNull(tbl.get(key));
        
        // Try to remove non-existed key.
        assertFalse(tbl.remove(key, obj));
        assertNull(tbl.get(key));
        
        // Put KV pair.
        tbl.put(key, obj2);
        assertEquals(obj2, tbl.get(key));
        
        // Check null value ignored.
        assertThrows(Throwable.class, () -> tbl.remove(key, null));
        assertEquals(obj2, tbl.get(key));
        
        // Delete KV pair with expected value.
        assertTrue(tbl.remove(key, obj2));
        assertNull(tbl.get(key));
        
        assertFalse(tbl.remove(key2, obj2));
        assertNull(tbl.get(key2));
    }
    
    @Test
    public void replace() {
        final TestKeyObject key = TestKeyObject.randomObject(rnd);
        final TestKeyObject key2 = TestKeyObject.randomObject(rnd);
        final TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes obj2 = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes obj3 = TestObjectWithAllTypes.randomObject(rnd);
        
        KeyValueView<TestKeyObject, TestObjectWithAllTypes> tbl = kvView();
        
        // Ignore replace operation for non-existed KV pair.
        assertFalse(tbl.replace(key, obj));
        assertNull(tbl.get(key));
        
        tbl.put(key, obj);
        
        // Replace existed KV pair.
        assertTrue(tbl.replace(key, obj2));
        assertEquals(obj2, tbl.get(key));
        
        // Remove existed KV pair.
        assertTrue(tbl.replace(key, null));
        assertNull(tbl.get(key));
        
        // Ignore replace operation for non-existed KV pair.
        assertFalse(tbl.replace(key, obj3));
        assertNull(tbl.get(key));
        
        tbl.put(key, obj3);
        assertEquals(obj3, tbl.get(key));
        
        // Remove non-existed KV pair.
        assertFalse(tbl.replace(key2, null));
        assertNull(tbl.get(key2));
    }
    
    @Test
    public void replaceExact() {
        final TestKeyObject key = TestKeyObject.randomObject(rnd);
        final TestKeyObject key2 = TestKeyObject.randomObject(rnd);
        final TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes obj2 = TestObjectWithAllTypes.randomObject(rnd);
        final TestObjectWithAllTypes obj3 = TestObjectWithAllTypes.randomObject(rnd);
        
        KeyValueView<TestKeyObject, TestObjectWithAllTypes> tbl = kvView();
        
        // Insert KV pair.
        assertTrue(tbl.replace(key, null, obj));
        assertEquals(obj, tbl.get(key));
        assertNull(tbl.get(key2));
        
        // Ignore replace operation for non-existed KV pair.
        assertFalse(tbl.replace(key2, obj, obj2));
        assertNull(tbl.get(key2));
        
        // Replace existed KV pair.
        assertTrue(tbl.replace(key, obj, obj2));
        assertEquals(obj2, tbl.get(key));
        
        // Remove existed KV pair.
        assertTrue(tbl.replace(key, obj2, null));
        assertNull(tbl.get(key));
        
        // Insert KV pair.
        assertTrue(tbl.replace(key, null, obj3));
        assertEquals(obj3, tbl.get(key));
        
        // Remove non-existed KV pair.
        assertTrue(tbl.replace(key2, null, null));
    }
    
    /**
     * Creates key-value view.
     */
    private KeyValueViewImpl<TestKeyObject, TestObjectWithAllTypes> kvView() {
        Mapper<TestKeyObject> keyMapper = Mapper.identity(TestKeyObject.class);
        Mapper<TestObjectWithAllTypes> valMapper = Mapper.identity(TestObjectWithAllTypes.class);
        
        Column[] valCols = {
                new Column("primitiveByteCol", INT8, false),
                new Column("primitiveShortCol", INT16, false),
                new Column("primitiveIntCol", INT32, false),
                new Column("primitiveLongCol", INT64, false),
                new Column("primitiveFloatCol", FLOAT, false),
                new Column("primitiveDoubleCol", DOUBLE, false),
                
                new Column("byteCol", INT8, true),
                new Column("shortCol", INT16, true),
                new Column("intCol", INT32, true),
                new Column("longCol", INT64, true),
                new Column("nullLongCol", INT64, true),
                new Column("floatCol", FLOAT, true),
                new Column("doubleCol", DOUBLE, true),
                
                new Column("dateCol", DATE, true),
                new Column("timeCol", time(), true),
                new Column("dateTimeCol", datetime(), true),
                new Column("timestampCol", timestamp(), true),
                
                new Column("uuidCol", NativeTypes.UUID, true),
                new Column("bitmaskCol", NativeTypes.bitmaskOf(42), true),
                new Column("stringCol", STRING, true),
                new Column("nullBytesCol", BYTES, true),
                new Column("bytesCol", BYTES, true),
                new Column("numberCol", NativeTypes.numberOf(12), true),
                new Column("decimalCol", NativeTypes.decimalOf(19, 3), true),
        };
        
        SchemaDescriptor schema = new SchemaDescriptor(
                1,
                new Column[]{new Column("id", NativeTypes.INT64, false)},
                valCols
        );
    
        // Validate all types are tested.
        Set<NativeTypeSpec> testedTypes = Arrays.stream(valCols).map(c -> c.type().spec())
                .collect(Collectors.toSet());
        Set<NativeTypeSpec> missedTypes = Arrays.stream(NativeTypeSpec.values())
                .filter(t -> !testedTypes.contains(t)).collect(Collectors.toSet());
    
        assertEquals(Collections.emptySet(), missedTypes);
    
        return new KeyValueViewImpl<>(
                new DummyInternalTableImpl(),
                new DummySchemaManagerImpl(schema),
                keyMapper,
                valMapper,
                null
        );
    }
    
    /**
     * Test object.
     */
    @SuppressWarnings({"InstanceVariableMayNotBeInitialized", "unused"})
    public static class TestKeyObject {
        public static TestKeyObject randomObject(Random rnd) {
            return new TestKeyObject(rnd.nextLong());
        }
        
        private long id;
        
        private TestKeyObject() {
        }
        
        public TestKeyObject(long id) {
            this.id = id;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            
            TestKeyObject that = (TestKeyObject) o;
            
            return id == that.id;
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }
}
