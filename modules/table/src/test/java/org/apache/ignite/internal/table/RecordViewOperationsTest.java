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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
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
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

/**
 * Basic table operations test.
 */
//TODO: IGNITE-14487 Add bulk operations tests.
//TODO: IGNITE-14487 Add async operations tests.
public class RecordViewOperationsTest {
    
    private final Random rnd = new Random();
    
    @Test
    public void upsert() {
        final TestObjectWithAllTypes key = key(rnd);
        
        final TestObjectWithAllTypes obj = randomObject(rnd, key);
        final TestObjectWithAllTypes obj2 = randomObject(rnd, key);
        final TestObjectWithAllTypes obj3 = randomObject(rnd, key);
        
        RecordView<TestObjectWithAllTypes> tbl = recordView();
        
        assertNull(tbl.get(key));
        
        // Insert new row.
        tbl.upsert(obj);
        assertEquals(obj, tbl.get(key));
        
        // Upsert row.
        tbl.upsert(obj2);
        assertEquals(obj2, tbl.get(key));
        
        // Remove row.
        tbl.delete(key);
        assertNull(tbl.get(key));
        
        // Insert new row.
        tbl.upsert(obj3);
        assertEquals(obj3, tbl.get(key));
    }
    
    @Test
    public void insert() {
        final TestObjectWithAllTypes key = key(rnd);
        final TestObjectWithAllTypes obj = randomObject(rnd, key);
        final TestObjectWithAllTypes obj2 = randomObject(rnd, key);
        
        RecordView<TestObjectWithAllTypes> tbl = recordView();
        
        assertNull(tbl.get(key));
        
        // Insert new row.
        assertTrue(tbl.insert(obj));
        assertEquals(obj, tbl.get(key));
        
        // Ignore existed row pair.
        assertFalse(tbl.insert(obj2));
        assertEquals(obj, tbl.get(key));
    }
    
    @Test
    public void getAndUpsert() {
        final TestObjectWithAllTypes key = key(rnd);
        final TestObjectWithAllTypes obj = randomObject(rnd, key);
        final TestObjectWithAllTypes obj2 = randomObject(rnd, key);
        final TestObjectWithAllTypes obj3 = randomObject(rnd, key);
        
        RecordView<TestObjectWithAllTypes> tbl = recordView();
        
        assertNull(tbl.get(key));
        
        // Insert new row.
        assertNull(tbl.getAndUpsert(obj));
        assertEquals(obj, tbl.get(key));
        
        // Update exited row.
        assertEquals(obj, tbl.getAndUpsert(obj2));
        assertEquals(obj2, tbl.getAndUpsert(obj3));
        
        assertEquals(obj3, tbl.get(key));
    }
    
    @Test
    public void remove() {
        final TestObjectWithAllTypes key = key(rnd);
        final TestObjectWithAllTypes obj = randomObject(rnd, key);
        final TestObjectWithAllTypes obj2 = randomObject(rnd, key);
        
        RecordView<TestObjectWithAllTypes> tbl = recordView();
        
        // Delete not existed key.
        assertNull(tbl.get(key));
        assertFalse(tbl.delete(key));
        
        // Insert a new row.
        tbl.upsert(obj);
        
        // Delete existed row.
        assertEquals(obj, tbl.get(key));
        assertTrue(tbl.delete(key));
        assertNull(tbl.get(key));
        
        // Delete already deleted row.
        assertFalse(tbl.delete(key));
        
        // Insert a new row.
        tbl.upsert(obj2);
        assertEquals(obj2, tbl.get(key));
    }
    
    @Test
    public void removeExact() {
        final TestObjectWithAllTypes key = key(rnd);
        final TestObjectWithAllTypes obj = randomObject(rnd, key);
        final TestObjectWithAllTypes obj2 = randomObject(rnd, key);
        
        RecordView<TestObjectWithAllTypes> tbl = recordView();
        
        // Insert a new row.
        tbl.upsert(obj);
        assertEquals(obj, tbl.get(key));
        
        // Fails to delete row with unexpected value.
        assertFalse(tbl.deleteExact(obj2));
        assertEquals(obj, tbl.get(key));
        
        // Delete row with expected value.
        assertTrue(tbl.deleteExact(obj));
        assertNull(tbl.get(key));
        
        // Try to remove non-existed key.
        assertFalse(tbl.deleteExact(obj));
        assertNull(tbl.get(key));
        
        // Insert a new row.
        tbl.upsert(obj2);
        assertEquals(obj2, tbl.get(key));
        
        // Delete row with expected value.
        assertTrue(tbl.delete(obj2));
        assertNull(tbl.get(key));
        
        assertFalse(tbl.delete(obj2));
        assertNull(tbl.get(obj2));
    }
    
    @Test
    public void replace() {
        final TestObjectWithAllTypes key = key(rnd);
        final TestObjectWithAllTypes obj = randomObject(rnd, key);
        final TestObjectWithAllTypes obj2 = randomObject(rnd, key);
        final TestObjectWithAllTypes obj3 = randomObject(rnd, key);
        
        RecordView<TestObjectWithAllTypes> tbl = recordView();
        
        // Ignore replace operation for non-existed row.
        assertFalse(tbl.replace(obj));
        assertNull(tbl.get(key));
        
        // Insert new row.
        tbl.upsert(obj);
        
        // Replace existed row.
        assertTrue(tbl.replace(obj2));
        assertEquals(obj2, tbl.get(key));
        
        // Replace existed row.
        assertTrue(tbl.replace(obj3));
        assertEquals(obj3, tbl.get(key));
        
        // Remove existed row.
        assertTrue(tbl.delete(key));
        assertNull(tbl.get(key));
        
        tbl.upsert(obj);
        assertEquals(obj, tbl.get(key));
    }
    
    @Test
    public void replaceExact() {
        final TestObjectWithAllTypes key = key(rnd);
        final TestObjectWithAllTypes obj = randomObject(rnd, key);
        final TestObjectWithAllTypes obj2 = randomObject(rnd, key);
        final TestObjectWithAllTypes obj3 = randomObject(rnd, key);
        final TestObjectWithAllTypes obj4 = randomObject(rnd, key);
        
        RecordView<TestObjectWithAllTypes> tbl = recordView();
        
        // Ignore replace operation for non-existed row.
        assertFalse(tbl.replace(obj, obj2));
        assertNull(tbl.get(key));
        
        // Insert new row.
        tbl.upsert(obj);
        
        // Ignore un-exepected row replacement.
        assertFalse(tbl.replace(obj2, obj3));
        assertEquals(obj, tbl.get(key));
        
        // Replace existed row.
        assertTrue(tbl.replace(obj, obj2));
        assertEquals(obj2, tbl.get(key));
        
        // Replace existed KV pair.
        assertTrue(tbl.replace(obj2, obj3));
        assertEquals(obj3, tbl.get(key));
        
        // Remove existed row.
        assertTrue(tbl.delete(key));
        assertNull(tbl.get(key));
        
        assertFalse(tbl.replace(key, obj4));
        assertNull(tbl.get(key));
    }
    
    /**
     * Creates RecordView.
     */
    private RecordViewImpl<TestObjectWithAllTypes> recordView() {
        Mapper<TestObjectWithAllTypes> recMapper = Mapper.identity(TestObjectWithAllTypes.class);
        
        Column[] valCols = {
                new Column("primitiveByteCol", INT8, false),
                new Column("primitiveShortCol", INT16, false),
                new Column("primitiveIntCol", INT32, false),
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
                new Column[]{new Column("primitiveLongCol", NativeTypes.INT64, false)},
                valCols
        );
        
        // Validate all types are tested.
        Set<NativeTypeSpec> testedTypes = Arrays.stream(valCols).map(c -> c.type().spec())
                .collect(Collectors.toSet());
        Set<NativeTypeSpec> missedTypes = Arrays.stream(NativeTypeSpec.values())
                .filter(t -> !testedTypes.contains(t)).collect(Collectors.toSet());
        
        assertEquals(Collections.emptySet(), missedTypes);
        
        return new RecordViewImpl<>(
                new DummyInternalTableImpl(),
                new DummySchemaManagerImpl(schema),
                recMapper,
                null
        );
    }
    
    @NotNull
    private TestObjectWithAllTypes randomObject(Random rnd, TestObjectWithAllTypes key) {
        TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);
        
        obj.setPrimitiveLongCol(key.getPrimitiveLongCol());
        
        return obj;
    }
    
    @NotNull
    private static TestObjectWithAllTypes key(Random rnd) {
        TestObjectWithAllTypes key = new TestObjectWithAllTypes();
        
        key.setPrimitiveLongCol(rnd.nextLong());
        
        return key;
    }
}
