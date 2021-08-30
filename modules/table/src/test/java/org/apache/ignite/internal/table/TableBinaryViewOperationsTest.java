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

import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.table.impl.TestTupleBuilder;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Basic table operations test.
 * <p>
 * TODO: IGNITE-14486 Add tests for invoke operations.
 * TODO: IGNITE-14486 Add tests for bulk operations.
 * TODO: IGNITE-14486 Add tests for async operations.
 */
public class TableBinaryViewOperationsTest {
    /** Table ID test value. */
    public final java.util.UUID tableId = java.util.UUID.randomUUID();

    /**
     *
     */
    @Test
    public void insert() {
        SchemaDescriptor schema = new SchemaDescriptor(
            tableId,
            1,
            new Column[] {new Column("id", NativeTypes.INT64, false)},
            new Column[] {new Column("val", NativeTypes.INT64, false)}
        );

        Table tbl = new TableImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema), null, null);

        final Tuple tuple = Tuple.create().set("id", 1L).set("val", 11L);
        final Tuple newTuple = Tuple.create().set("id", 1L).set("val", 22L);
        final Tuple nonExistedTuple = Tuple.create().set("id", 2L);

        assertNull(tbl.get(Tuple.create().set("id", 1L)));

        // Insert new tuple.
        assertTrue(tbl.insert(tuple));

        assertEqualsRows(schema, tuple, tbl.get(Tuple.create().set("id", 1L)));

        // Ignore insert operation for exited row.
        assertFalse(tbl.insert(newTuple));

        assertEqualsRows(schema, tuple, tbl.get(Tuple.create().set("id", 1L)));

        assertNull(tbl.get(nonExistedTuple));
    }

    /**
     *
     */
    @Test
    public void upsert() {
        SchemaDescriptor schema = new SchemaDescriptor(
            tableId,
            1,
            new Column[] {new Column("id", NativeTypes.INT64, false)},
            new Column[] {new Column("val", NativeTypes.INT64, false)}
        );

        Table tbl = new TableImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema), null, null);

        final Tuple tuple = Tuple.create().set("id", 1L).set("val", 11L);
        final Tuple newTuple = Tuple.create().set("id", 1L).set("val", 22L);
        final Tuple nonExistedTuple = Tuple.create().set("id", 2L);

        assertNull(tbl.get(Tuple.create().set("id", 1L)));

        // Insert new tuple.
        tbl.upsert(tuple);

        assertEqualsRows(schema, tuple, tbl.get(Tuple.create().set("id", 1L)));

        // Update exited row.
        tbl.upsert(newTuple);

        assertEqualsRows(schema, newTuple, tbl.get(Tuple.create().set("id", 1L)));

        assertNull(tbl.get(nonExistedTuple));
    }

    /**
     *
     */
    @Test
    public void getAndUpsert() {
        SchemaDescriptor schema = new SchemaDescriptor(
            tableId,
            1,
            new Column[] {new Column("id", NativeTypes.INT64, false)},
            new Column[] {new Column("val", NativeTypes.INT64, false)}
        );

        Table tbl = new TableImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema), null, null);

        final Tuple tuple = Tuple.create().set("id", 1L).set("val", 11L);
        final Tuple newTuple = Tuple.create().set("id", 1L).set("val", 22L);

        assertNull(tbl.get(Tuple.create().set("id", 1L)));

        // Insert new tuple.
        assertNull(tbl.getAndUpsert(tuple));

        assertEqualsRows(schema, tuple, tbl.get(Tuple.create().set("id", 1L)));

        // Update exited row.
        assertEqualsRows(schema, tuple, tbl.getAndUpsert(newTuple));

        assertEqualsRows(schema, newTuple, tbl.get(Tuple.create().set("id", 1L)));
    }

    /**
     *
     */
    @Test
    public void remove() {
        SchemaDescriptor schema = new SchemaDescriptor(
            tableId,
            1,
            new Column[] {new Column("id", NativeTypes.INT64, false)},
            new Column[] {new Column("val", NativeTypes.INT64, false)}
        );

        Table tbl = new TableImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema), null, null);

        tbl.upsert(Tuple.create().set("id", 1L).set("val", 11L));

        final Tuple keyTuple = Tuple.create().set("id", 1L);

        // Delete not existed keyTuple.
        assertFalse(tbl.delete(Tuple.create().set("id", 2L)));

        // Delete existed keyTuple.
        assertTrue(tbl.delete(keyTuple));
        assertNull(tbl.get(keyTuple));

        // Delete already deleted keyTuple.
        assertFalse(tbl.delete(keyTuple));
    }

    /**
     *
     */
    @Test
    public void removeExact() {
        SchemaDescriptor schema = new SchemaDescriptor(
            tableId,
            1,
            new Column[] {new Column("id", NativeTypes.INT64, false)},
            new Column[] {new Column("val", NativeTypes.INT64, false)}
        );

        Table tbl = new TableImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema), null, null);

        final Tuple keyTuple = Tuple.create().set("id", 1L);
        final Tuple tuple = Tuple.create().set("id", 1L).set("val", 11L);
        final Tuple tuple2 = Tuple.create().set("id", 1L).set("val", 22L);
        final Tuple nonExistedTuple = Tuple.create().set("id", 2L).set("val", 22L);

        tbl.insert(tuple);

        assertEqualsRows(schema, tuple, tbl.get(keyTuple));

        // Fails to delete not existed tuple.
        assertFalse(tbl.deleteExact(nonExistedTuple));
        assertEqualsRows(schema, tuple, tbl.get(keyTuple));

        // Fails to delete tuple with unexpected value.
        assertFalse(tbl.deleteExact(tuple2));
        assertEqualsRows(schema, tuple, tbl.get(keyTuple));

        // TODO: IGNITE-14479: Fix default value usage.
//        assertFalse(tbl.deleteExact(keyTuple));
//        assertEqualsRows(schema, tuple, tbl.get(keyTuple));

        // Delete tuple with expected value.
        assertTrue(tbl.deleteExact(tuple));
        assertNull(tbl.get(keyTuple));

        // Once again.
        assertFalse(tbl.deleteExact(tuple));
        assertNull(tbl.get(keyTuple));

        // Insert new.
        tbl.insert(tuple2);
        assertEqualsRows(schema, tuple2, tbl.get(keyTuple));

        // Delete tuple with expected value.
        assertTrue(tbl.deleteExact(tuple2));
        assertNull(tbl.get(keyTuple));
    }

    /**
     *
     */
    @Test
    public void replace() {
        SchemaDescriptor schema = new SchemaDescriptor(
            tableId,
            1,
            new Column[] {new Column("id", NativeTypes.INT64, false)},
            new Column[] {new Column("val", NativeTypes.INT64, false)}
        );

        Table tbl = new TableImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema), null, null);

        final Tuple keyTuple = Tuple.create().set("id", 1L);
        final Tuple tuple = Tuple.create().set("id", 1L).set("val", 11L);
        final Tuple tuple2 = Tuple.create().set("id", 1L).set("val", 22L);

        assertNull(tbl.get(keyTuple));

        // Ignore replace operation for non-existed row.
        assertFalse(tbl.replace(tuple));

        assertNull(tbl.get(keyTuple));

        // Insert row.
        tbl.insert(tuple);

        // Replace existed row.
        assertTrue(tbl.replace(tuple2));

        assertEqualsRows(schema, tuple2, tbl.get(keyTuple));
    }

    /**
     *
     */
    @Test
    public void replaceExact() {
        SchemaDescriptor schema = new SchemaDescriptor(
            tableId,
            1,
            new Column[] {new Column("id", NativeTypes.INT64, false)},
            new Column[] {new Column("val", NativeTypes.INT64, false)}
        );

        Table tbl = new TableImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema), null, null);

        final Tuple tuple = Tuple.create().set("id", 1L).set("val", 11L);
        final Tuple tuple2 = Tuple.create().set("id", 1L).set("val", 22L);

        assertNull(tbl.get(Tuple.create().set("id", 1L)));

        // Ignore replace operation for non-existed row.
        // TODO: IGNITE-14479: Fix default value usage.
//        assertTrue(tbl.replace(keyTuple, tuple));

//        assertNull(tbl.get(keyTuple));
//        assertNull(tbl.get(tbl.tupleBuilder().set("id", 1L).set("val", -1)));

        // Insert row.
        tbl.insert(tuple);

        // Replace existed row.
        assertTrue(tbl.replace(tuple, tuple2));

        assertEqualsRows(schema, tuple2, tbl.get(Tuple.create().set("id", 1L)));
    }

    /**
     *
     */
    @Test
    public void validateSchema() {
        SchemaDescriptor schema = new SchemaDescriptor(
            tableId,
            1,
            new Column[] {new Column("id", NativeTypes.INT64, false)},
            new Column[] {
                new Column("val", NativeTypes.INT64, true),
                new Column("str", NativeTypes.stringOf(3), true),
                new Column("blob", NativeTypes.blobOf(3), true)
            }
        );

        Table tbl = new TableImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema), null, null);

        final Tuple keyTuple0 = new TestTupleBuilder().set("id", 0).set("id1", 0);
        final Tuple keyTuple1 = new TestTupleBuilder().set("id1", 0);
        final Tuple tuple0 = new TestTupleBuilder().set("id", 1L).set("str", "qweqweqwe").set("val", 11L);
        final Tuple tuple1 = new TestTupleBuilder().set("id", 1L).set("blob", new byte[] {0, 1, 2, 3}).set("val", 22L);

        assertThrows(InvalidTypeException.class, () -> tbl.get(keyTuple0));
        assertThrows(IllegalArgumentException.class, () -> tbl.get(keyTuple1));

        assertThrows(InvalidTypeException.class, () -> tbl.replace(tuple0));
        assertThrows(InvalidTypeException.class, () -> tbl.replace(tuple1));

        assertThrows(InvalidTypeException.class, () -> tbl.insert(tuple0));
        assertThrows(InvalidTypeException.class, () -> tbl.insert(tuple1));

        assertThrows(InvalidTypeException.class, () -> tbl.replace(tuple0));
        assertThrows(InvalidTypeException.class, () -> tbl.replace(tuple1));
    }

    /**
     *
     */
    @Test
    public void defaultValues() {
        SchemaDescriptor schema = new SchemaDescriptor(
            tableId,
            1,
            new Column[] {new Column("id", NativeTypes.INT64, false)},
            new Column[] {
                new Column("val", NativeTypes.INT64, true, () -> 28L),
                new Column("str", NativeTypes.stringOf(3), true, () -> "ABC"),
                new Column("blob", NativeTypes.blobOf(3), true, () -> new byte[] {0, 1, 2})
            }
        );

        Table tbl = new TableImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema), null, null);

        final Tuple keyTuple0 = Tuple.create().set("id", 0L);
        final Tuple keyTuple1 = Tuple.create().set("id", 1L);

        final Tuple tuple0 = Tuple.create().set("id", 0L);
        final Tuple tupleExpected0 = Tuple.create().set("id", 0L).set("val", 28L).set("str", "ABC").set("blob", new byte[] {0, 1, 2});
        final Tuple tuple1 = Tuple.create().set("id", 1L).set("val", null).set("str", null).set("blob", null);

        tbl.insert(tuple0);
        tbl.insert(tuple1);

        assertEqualsRows(schema, tupleExpected0, tbl.get(keyTuple0));
        assertEqualsRows(schema, tuple1, tbl.get(keyTuple1));
    }

    /**
     * Check tuples equality.
     *
     * @param schema Schema.
     * @param expected Expected tuple.
     * @param actual Actual tuple.
     */
    void assertEqualsRows(SchemaDescriptor schema, Tuple expected, Tuple actual) {
        assertEqualsKeys(schema, expected, actual);
        assertEqualsValues(schema, expected, actual);
    }

    /**
     * Check key columns equality.
     *
     * @param schema Schema.
     * @param expected Expected tuple.
     * @param actual Actual tuple.
     */
    void assertEqualsKeys(SchemaDescriptor schema, Tuple expected, Tuple actual) {
        int nonNullKey = 0;

        for (int i = 0; i < schema.keyColumns().length(); i++) {
            final Column col = schema.keyColumns().column(i);

            final Object val1 = expected.value(col.name());
            final Object val2 = actual.value(col.name());

            Assertions.assertEquals(val1, val2, "Value columns equality check failed: colIdx=" + col.schemaIndex());

            if (schema.isKeyColumn(i) && val1 != null)
                nonNullKey++;
        }

        assertTrue(nonNullKey > 0, "At least one non-null key column must exist.");
    }

    /**
     * Check value columns equality.
     *
     * @param schema Schema.
     * @param expected Expected tuple.
     * @param actual Actual tuple.
     */
    void assertEqualsValues(SchemaDescriptor schema, Tuple expected, Tuple actual) {
        for (int i = 0; i < schema.valueColumns().length(); i++) {
            final Column col = schema.valueColumns().column(i);

            final Object val1 = expected.value(col.name());
            final Object val2 = actual.value(col.name());

            if (val1 instanceof byte[] && val2 instanceof byte[])
                Assertions.assertArrayEquals((byte[])val1, (byte[])val2, "Equality check failed: colIdx=" + col.schemaIndex());
            else
                Assertions.assertEquals(val1, val2, "Equality check failed: colIdx=" + col.schemaIndex());
        }
    }
}
