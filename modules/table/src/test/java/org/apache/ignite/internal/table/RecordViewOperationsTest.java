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
import static org.mockito.Answers.RETURNS_DEEP_STUBS;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.testobjects.TestObjectWithAllTypes;
import org.apache.ignite.internal.storage.basic.ConcurrentHashMapPartitionStorage;
import org.apache.ignite.internal.table.distributed.storage.VersionedRowStore;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

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

        assertNull(tbl.get(null, key));

        // Insert new row.
        tbl.upsert(null, obj);
        assertEquals(obj, tbl.get(null, key));

        // Upsert row.
        tbl.upsert(null, obj2);
        assertEquals(obj2, tbl.get(null, key));

        // Remove row.
        tbl.delete(null, key);
        assertNull(tbl.get(null, key));

        // Insert new row.
        tbl.upsert(null, obj3);
        assertEquals(obj3, tbl.get(null, key));
    }

    @Test
    public void insert() {
        final TestObjectWithAllTypes key = key(rnd);
        final TestObjectWithAllTypes obj = randomObject(rnd, key);
        final TestObjectWithAllTypes obj2 = randomObject(rnd, key);

        RecordView<TestObjectWithAllTypes> tbl = recordView();

        assertNull(tbl.get(null, key));

        // Insert new row.
        assertTrue(tbl.insert(null, obj));
        assertEquals(obj, tbl.get(null, key));

        // Ignore existed row pair.
        assertFalse(tbl.insert(null, obj2));
        assertEquals(obj, tbl.get(null, key));
    }

    @Test
    public void getAndUpsert() {
        final TestObjectWithAllTypes key = key(rnd);
        final TestObjectWithAllTypes obj = randomObject(rnd, key);
        final TestObjectWithAllTypes obj2 = randomObject(rnd, key);
        final TestObjectWithAllTypes obj3 = randomObject(rnd, key);

        RecordView<TestObjectWithAllTypes> tbl = recordView();

        assertNull(tbl.get(null, key));

        // Insert new row.
        assertNull(tbl.getAndUpsert(null, obj));
        assertEquals(obj, tbl.get(null, key));

        // Update exited row.
        assertEquals(obj, tbl.getAndUpsert(null, obj2));
        assertEquals(obj2, tbl.getAndUpsert(null, obj3));

        assertEquals(obj3, tbl.get(null, key));
    }

    @Test
    public void remove() {
        final TestObjectWithAllTypes key = key(rnd);
        final TestObjectWithAllTypes obj = randomObject(rnd, key);
        final TestObjectWithAllTypes obj2 = randomObject(rnd, key);

        RecordView<TestObjectWithAllTypes> tbl = recordView();

        // Delete not existed key.
        assertNull(tbl.get(null, key));
        assertFalse(tbl.delete(null, key));

        // Insert a new row.
        tbl.upsert(null, obj);

        // Delete existed row.
        assertEquals(obj, tbl.get(null, key));
        assertTrue(tbl.delete(null, key));
        assertNull(tbl.get(null, key));

        // Delete already deleted row.
        assertFalse(tbl.delete(null, key));

        // Insert a new row.
        tbl.upsert(null, obj2);
        assertEquals(obj2, tbl.get(null, key));
    }

    @Test
    public void removeExact() {
        final TestObjectWithAllTypes key = key(rnd);
        final TestObjectWithAllTypes obj = randomObject(rnd, key);
        final TestObjectWithAllTypes obj2 = randomObject(rnd, key);

        RecordView<TestObjectWithAllTypes> tbl = recordView();

        // Insert a new row.
        tbl.upsert(null, obj);
        assertEquals(obj, tbl.get(null, key));

        // Fails to delete row with unexpected value.
        assertFalse(tbl.deleteExact(null, obj2));
        assertEquals(obj, tbl.get(null, key));

        // Delete row with expected value.
        assertTrue(tbl.deleteExact(null, obj));
        assertNull(tbl.get(null, key));

        // Try to remove non-existed key.
        assertFalse(tbl.deleteExact(null, obj));
        assertNull(tbl.get(null, key));

        // Insert a new row.
        tbl.upsert(null, obj2);
        assertEquals(obj2, tbl.get(null, key));

        // Delete row with expected value.
        assertTrue(tbl.delete(null, obj2));
        assertNull(tbl.get(null, key));

        assertFalse(tbl.delete(null, obj2));
        assertNull(tbl.get(null, obj2));
    }

    @Test
    public void replace() {
        final TestObjectWithAllTypes key = key(rnd);
        final TestObjectWithAllTypes obj = randomObject(rnd, key);
        final TestObjectWithAllTypes obj2 = randomObject(rnd, key);
        final TestObjectWithAllTypes obj3 = randomObject(rnd, key);

        RecordView<TestObjectWithAllTypes> tbl = recordView();

        // Ignore replace operation for non-existed row.
        assertFalse(tbl.replace(null, obj));
        assertNull(tbl.get(null, key));

        // Insert new row.
        tbl.upsert(null, obj);

        // Replace existed row.
        assertTrue(tbl.replace(null, obj2));
        assertEquals(obj2, tbl.get(null, key));

        // Replace existed row.
        assertTrue(tbl.replace(null, obj3));
        assertEquals(obj3, tbl.get(null, key));

        // Remove existed row.
        assertTrue(tbl.delete(null, key));
        assertNull(tbl.get(null, key));

        tbl.upsert(null, obj);
        assertEquals(obj, tbl.get(null, key));
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
        assertFalse(tbl.replace(null, obj, obj2));
        assertNull(tbl.get(null, key));

        // Insert new row.
        tbl.upsert(null, obj);

        // Ignore un-exepected row replacement.
        assertFalse(tbl.replace(null, obj2, obj3));
        assertEquals(obj, tbl.get(null, key));

        // Replace existed row.
        assertTrue(tbl.replace(null, obj, obj2));
        assertEquals(obj2, tbl.get(null, key));

        // Replace existed KV pair.
        assertTrue(tbl.replace(null, obj2, obj3));
        assertEquals(obj3, tbl.get(null, key));

        // Remove existed row.
        assertTrue(tbl.delete(null, key));
        assertNull(tbl.get(null, key));

        assertFalse(tbl.replace(null, key, obj4));
        assertNull(tbl.get(null, key));
    }

    @Test
    public void getAll() {
        final TestObjectWithAllTypes key1 = key(rnd);
        final TestObjectWithAllTypes key2 = key(rnd);
        final TestObjectWithAllTypes key3 = key(rnd);
        final TestObjectWithAllTypes val1 = randomObject(rnd, key1);
        final TestObjectWithAllTypes val3 = randomObject(rnd, key3);

        RecordView<TestObjectWithAllTypes> tbl = recordView();

        tbl.upsertAll(null, List.of(val1, val3));

        Collection<TestObjectWithAllTypes> res = tbl.getAll(null, List.of(key1, key2, key3));

        assertEquals(2, res.size());
        assertTrue(res.contains(val1));
        assertTrue(res.contains(val3));
    }

    /**
     * Creates RecordView.
     */
    private RecordViewImpl<TestObjectWithAllTypes> recordView() {
        ClusterService clusterService = Mockito.mock(ClusterService.class, RETURNS_DEEP_STUBS);
        Mockito.when(clusterService.topologyService().localMember().address())
                .thenReturn(DummyInternalTableImpl.ADDR);

        TxManager txManager = new TxManagerImpl(clusterService, new HeapLockManager());

        DummyInternalTableImpl table = new DummyInternalTableImpl(
                new VersionedRowStore(new ConcurrentHashMapPartitionStorage(), txManager), txManager);

        Mapper<TestObjectWithAllTypes> recMapper = Mapper.of(TestObjectWithAllTypes.class);

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
                table,
                new DummySchemaManagerImpl(schema),
                recMapper
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
