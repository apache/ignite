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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaAware;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.TupleMarshaller;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.schema.SchemaMode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.TupleImpl;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

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
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests server tuple builder implementation.
 * <p>
 * Should be in sync with org.apache.ignite.client.ClientTupleBuilderTest.
 */
public class MutableRowTupleAdapterTest {
    /** Mocked table. */
    private InternalTable tbl = Mockito.when(Mockito.mock(InternalTable.class).schemaMode()).thenReturn(SchemaMode.STRICT_SCHEMA).getMock();

    /** Schema descriptor. */
    private SchemaDescriptor schema = new SchemaDescriptor(
        UUID.randomUUID(),
        42,
        new Column[]{new Column("id", NativeTypes.INT64, false)},
        new Column[]{new Column("name", NativeTypes.STRING, true)}
    );

    @Test
    public void testValueReturnsValueByName() {
        assertEquals(3L, (Long)getTuple().value("id"));
        assertEquals("Shirt", getTuple().value("name"));
    }

    @Test
    public void testValueThrowsOnInvalidColumnName() {
        var ex = assertThrows(IllegalArgumentException.class, () -> getTuple().value("x"));
        assertEquals("Invalid column name: columnName=x", ex.getMessage());
    }

    @Test
    public void testValueReturnsValueByIndex() {
        assertEquals(3L, (Long)getTuple().value(0));
        assertEquals("Shirt", getTuple().value(1));
    }

    @Test
    public void testValueThrowsOnInvalidIndex() {
        var ex = assertThrows(IndexOutOfBoundsException.class, () -> getTuple().value(-1));
        assertEquals("Index -1 out of bounds for length 2", ex.getMessage());

        ex = assertThrows(IndexOutOfBoundsException.class, () -> getTuple().value(3));
        assertEquals("Index 3 out of bounds for length 2", ex.getMessage());
    }

    @Test
    public void testValueOrDefaultReturnsValueByName() {
        assertEquals(3L, getTuple().valueOrDefault("id", -1L));
        assertEquals("Shirt", getTuple().valueOrDefault("name", "y"));
    }

    @Test
    public void testValueOrDefaultReturnsDefaultWhenColumnIsNotSet() {
        assertEquals("foo", getTuple().valueOrDefault("x", "foo"));
    }

    @Test
    public void testValueReturnsOverwrittenValue() {
        assertEquals("foo", getTuple().set("name", "foo").value("name"));
        assertEquals("foo", getTuple().set("name", "foo").value(1));

        assertEquals("foo", getTuple().set("name", "foo").valueOrDefault("name", "bar"));
    }

    @Test
    public void testValueOrDefaultReturnsNullWhenColumnIsSetToNull() {
        assertNull(getTuple().set("name", null).valueOrDefault("name", "foo"));
    }

    @Test
    public void testColumnCountReturnsSchemaSize() {
        assertEquals(2, getTuple().columnCount());

        Tuple tuple = getTuple();

        assertEquals(2, tuple.columnCount());
        assertEquals(2, tuple.set("id", -1).columnCount());

        tuple.valueOrDefault("name", "foo");
        assertEquals(2, tuple.columnCount());

        tuple.valueOrDefault("foo", "bar");
        assertEquals(2, tuple.columnCount());

        tuple.set("foo", "bar");
        assertEquals(3, tuple.columnCount());
    }

    @Test
    public void testColumnNameReturnsNameByIndex() {
        assertEquals("id", getTuple().columnName(0));
        assertEquals("name", getTuple().columnName(1));
    }

    @Test
    public void testColumnNameThrowsOnInvalidIndex() {
        var ex = assertThrows(IndexOutOfBoundsException.class, () -> getTuple().columnName(-1));
        assertEquals("Index -1 out of bounds for length 2", ex.getMessage());

        ex = assertThrows(IndexOutOfBoundsException.class, () -> getTuple().columnName(3));
        assertEquals("Index 3 out of bounds for length 2", ex.getMessage());
    }

    @Test
    public void testColumnIndexReturnsIndexByName() {
        assertEquals(0, getTuple().columnIndex("id"));
        assertEquals(1, getTuple().columnIndex("name"));
    }

    @Test
    public void testColumnIndexForMissingColumns() {
        assertEquals(-1, getTuple().columnIndex("foo"));
    }

    @Test
    public void testKeyValueChunks() {
        SchemaDescriptor schema = new SchemaDescriptor(
            UUID.randomUUID(),
            42,
            new Column[]{new Column("id", NativeTypes.INT64, false)},
            new Column[]{
                new Column("name", NativeTypes.STRING, true),
                new Column("price", NativeTypes.DOUBLE, true)
            }
        );

        Tuple original = new TupleImpl()
                             .set("id", 3L)
                             .set("name", "Shirt")
                             .set("price", 5.99d);

        TupleMarshaller marshaller = new TupleMarshallerImpl(null, tbl, new DummySchemaManagerImpl(schema));

        Row row = new Row(schema, new ByteBufferRow(marshaller.marshal(original).bytes()));

        Tuple key = TableRow.keyTuple(row);
        Tuple val = TableRow.valueTuple(row);

        assertEquals(3L, (Long)key.value("id"));
        assertEquals(3L, (Long)key.value(0));

        assertEquals("Shirt", val.value("name"));
        assertEquals("Shirt", val.value(1));

        assertEquals(5.99d, val.value("price"));
        assertEquals(5.99d, val.value(0));

        // Wrong columns.
        assertThrows(IndexOutOfBoundsException.class, () -> key.value(1));
        assertThrows(IllegalArgumentException.class, () -> key.value("price"));

        assertThrows(IndexOutOfBoundsException.class, () -> val.value(2));
        assertThrows(IllegalArgumentException.class, () -> val.value("id"));
    }

    @Test
    public void testRowTupleMutability() {
        TupleMarshaller marshaller = new TupleMarshallerImpl(null, tbl, new DummySchemaManagerImpl(schema));

        Row row = new Row(schema, new ByteBufferRow(marshaller.marshal(new TupleImpl().set("id", 1L).set("name", "Shirt")).bytes()));

        Tuple tuple = TableRow.tuple(row);
        Tuple key = TableRow.keyTuple(row);
        Tuple val = TableRow.valueTuple(row);

        tuple.set("id", 2L);

        assertEquals(2L, (Long)tuple.value("id"));
        assertEquals(1L, (Long)key.value("id"));

        tuple.set("name", "noname");

        assertEquals("noname", tuple.value("name"));
        assertEquals("Shirt", val.value("name"));

        tuple.set("foo", "bar");

        assertEquals("bar", tuple.value("foo"));
        assertThrows(IllegalArgumentException.class, () -> key.value("foo"));
        assertThrows(IllegalArgumentException.class, () -> val.value("foo"));
    }

    @Test
    public void testKeyValueTupleMutability() {
        TupleMarshaller marshaller = new TupleMarshallerImpl(null, tbl, new DummySchemaManagerImpl(schema));

        Row row = new Row(schema, new ByteBufferRow(marshaller.marshal(new TupleImpl().set("id", 1L).set("name", "Shirt")).bytes()));

        Tuple tuple = TableRow.tuple(row);
        Tuple key = TableRow.keyTuple(row);
        Tuple val = TableRow.valueTuple(row);

        assertTrue(tuple instanceof SchemaAware);

        key.set("id", 3L);

        assertEquals(3L, (Long)key.value("id"));
        assertEquals(1L, (Long)tuple.value("id"));

        val.set("name", "noname");

        assertEquals("noname", val.value("name"));
        assertEquals("Shirt", tuple.value("name"));

        val.set("foo", "bar");

        assertEquals("bar", val.value("foo"));
        assertThrows(IllegalArgumentException.class, () -> key.value("foo"));
        assertThrows(IllegalArgumentException.class, () -> tuple.value("foo"));
    }

    @Test
    public void testRowTupleSchemaAwareness() {
        TupleMarshaller marshaller = new TupleMarshallerImpl(null, tbl, new DummySchemaManagerImpl(schema));

        Row row = new Row(schema, new ByteBufferRow(marshaller.marshal(new TupleImpl().set("id", 1L).set("name", "Shirt")).bytes()));

        Tuple tuple = TableRow.tuple(row);
        Tuple key = TableRow.keyTuple(row);
        Tuple val = TableRow.valueTuple(row);

        assertTrue(tuple instanceof SchemaAware);

        assertNotNull(((SchemaAware)tuple).schema());
        assertNotNull(((SchemaAware)key).schema());
        assertNotNull(((SchemaAware)val).schema());

        tuple.set("name", "noname");

        assertNull(((SchemaAware)tuple).schema());
        assertNotNull(((SchemaAware)key).schema());
        assertNotNull(((SchemaAware)val).schema());
    }

    @Test
    public void testKeyValueTupleSchemaAwareness() {
        TupleMarshaller marshaller = new TupleMarshallerImpl(null, tbl, new DummySchemaManagerImpl(schema));

        Row row = new Row(schema, new ByteBufferRow(marshaller.marshal(new TupleImpl().set("id", 1L).set("name", "Shirt")).bytes()));

        Tuple tuple = TableRow.tuple(row);
        Tuple key = TableRow.keyTuple(row);
        Tuple val = TableRow.valueTuple(row);

        assertTrue(tuple instanceof SchemaAware);

        key.set("foo", "bar");

        assertNotNull(((SchemaAware)tuple).schema());
        assertNull(((SchemaAware)key).schema());
        assertNotNull(((SchemaAware)val).schema());

        val.set("id", 1L);

        assertNotNull(((SchemaAware)tuple).schema());
        assertNull(((SchemaAware)key).schema());
        assertNull(((SchemaAware)val).schema());
    }

    @Test
    public void testVariousColumnTypes() {
        Random rnd = new Random();

        SchemaDescriptor schema = new SchemaDescriptor(tbl.tableId(), 42,
            new Column[]{new Column("keyUuidCol", NativeTypes.UUID, true)},
            new Column[]{
                new Column("valByteCol", INT8, true),
                new Column("valShortCol", INT16, true),
                new Column("valIntCol", INT32, true),
                new Column("valLongCol", INT64, true),
                new Column("valFloatCol", FLOAT, true),
                new Column("valDoubleCol", DOUBLE, true),
                new Column("valDateCol", DATE, true),
                new Column("valTimeCol", time(), true),
                new Column("valDateTimeCol", datetime(), true),
                new Column("valTimeStampCol", timestamp(), true),
                new Column("valBitmask1Col", NativeTypes.bitmaskOf(22), true),
                new Column("valBytesCol", BYTES, false),
                new Column("valStringCol", STRING, false),
                new Column("valNumberCol", NativeTypes.numberOf(20), false),
                new Column("valDecimalCol", NativeTypes.decimalOf(25, 5), false),
            }
        );

        TupleMarshaller marshaller = new TupleMarshallerImpl(null, tbl, new DummySchemaManagerImpl(schema));

        Tuple tuple = new TupleImpl()
                          .set("valByteCol", (byte)1)
                          .set("valShortCol", (short)2)
                          .set("valIntCol", 3)
                          .set("valLongCol", 4L)
                          .set("valFloatCol", 0.055f)
                          .set("valDoubleCol", 0.066d)
                          .set("keyUuidCol", UUID.randomUUID())
                          .set("valDateCol", LocalDate.now())
                          .set("valDateTimeCol", LocalDateTime.now())
                          .set("valTimeCol", LocalTime.now())
                          .set("valTimeStampCol", Instant.now())
                          .set("valBitmask1Col", randomBitSet(rnd, 12))
                          .set("valBytesCol", IgniteTestUtils.randomBytes(rnd, 13))
                          .set("valStringCol", IgniteTestUtils.randomString(rnd, 14))
                          .set("valNumberCol", BigInteger.valueOf(rnd.nextLong()))
                          .set("valDecimalCol", BigDecimal.valueOf(rnd.nextLong(), 5));

        Tuple rowTuple = TableRow.tuple(new Row(schema, new ByteBufferRow(marshaller.marshal(tuple).bytes())));

        checkTuples(schema, tuple, rowTuple);

        rowTuple.set("foo", "bar"); // Force row to tuple conversion.

        checkTuples(schema, tuple, rowTuple);
    }

    private void checkTuples(SchemaDescriptor schema, Tuple tuple, Tuple rowTuple) {
        for (int i = 0; i < schema.length(); i++) {
            Column col = schema.column(i);
            String name = col.name();

            if (col.type().spec() == NativeTypeSpec.BYTES) {
                assertArrayEquals((byte[])tuple.value(tuple.columnIndex(name)), rowTuple.value(rowTuple.columnIndex(name)), "columnIdx=" + i);
                assertArrayEquals((byte[])tuple.value(name), rowTuple.value(name), "columnName=" + name);
            } else {
                assertEquals((Object)tuple.value(tuple.columnIndex(name)), rowTuple.value(rowTuple.columnIndex(name)), "columnIdx=" + i);
                assertEquals((Object)tuple.value(name), rowTuple.value(name), "columnName=" + name);
            }
        }
    }

    private Tuple getTuple() {
        Tuple original = new TupleImpl()
                             .set("id", 3L)
                             .set("name", "Shirt");

        TupleMarshaller marshaller = new TupleMarshallerImpl(null, tbl, new DummySchemaManagerImpl(schema));

        return TableRow.tuple(new Row(schema, new ByteBufferRow(marshaller.marshal(original).bytes())));
    }

    /**
     * @param rnd Random generator.
     * @param bits Amount of bits in bitset.
     * @return Random BitSet.
     */
    private static BitSet randomBitSet(Random rnd, int bits) {
        BitSet set = new BitSet();

        for (int i = 0; i < bits; i++) {
            if (rnd.nextBoolean())
                set.set(i);
        }

        return set;
    }
}
