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
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.table.KeyValueBinaryView;
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
 * TODO: IGNITE-14487 Add bulk operations tests.
 * TODO: IGNITE-14487 Add async operations tests.
 * TODO: IGNITE-14487 Check non-key fields in Tuple is ignored for keys.
 * TODO: IGNITE-14487 Check key fields in Tuple is ignored for value or exception is thrown?
 */
public class KVBinaryViewOperationsTest {
    /** Table ID test value. */
    public final java.util.UUID tableId = java.util.UUID.randomUUID();

    /**
     *
     */
    @Test
    public void put() {
        SchemaDescriptor schema = new SchemaDescriptor(
            tableId,
            1,
            new Column[] {new Column("id", NativeTypes.INT64, false)},
            new Column[] {new Column("val", NativeTypes.INT64, false)}
        );

        KeyValueBinaryView tbl =
            new KVBinaryViewImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema), null, null);

        final Tuple key = Tuple.create().set("id", 1L);
        final Tuple val = Tuple.create().set("val", 11L);
        final Tuple val2 = Tuple.create().set("val", 22L);
        final Tuple val3 = Tuple.create().set("val", 33L);

        assertNull(tbl.get(key));

        // Put KV pair.
        tbl.put(key, val);

        assertEqualsValues(schema, val, tbl.get(key));
        assertEqualsValues(schema, val, tbl.get(Tuple.create().set("id", 1L)));

        // Update KV pair.
        tbl.put(key, val2);

        assertEqualsValues(schema, val2, tbl.get(key));
        assertEqualsValues(schema, val2, tbl.get(Tuple.create().set("id", 1L)));

        // Remove KV pair.
        tbl.put(key, null);

        assertNull(tbl.get(key));

        // Put KV pair.
        tbl.put(key, val3);
        assertEqualsValues(schema, val3, tbl.get(key));
    }

    /**
     *
     */
    @Test
    public void putIfAbsent() {
        SchemaDescriptor schema = new SchemaDescriptor(
            tableId,
            1,
            new Column[] {new Column("id", NativeTypes.INT64, false)},
            new Column[] {new Column("val", NativeTypes.INT64, false)}
        );

        KeyValueBinaryView tbl =
            new KVBinaryViewImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema), null, null);

        final Tuple key = Tuple.create().set("id", 1L);
        final Tuple val = Tuple.create().set("val", 11L);
        final Tuple val2 = Tuple.create().set("val", 22L);

        assertNull(tbl.get(key));

        // Insert new KV pair.
        assertTrue(tbl.putIfAbsent(key, val));

        assertEqualsValues(schema, val, tbl.get(key));
        assertEqualsValues(schema, val, tbl.get(Tuple.create().set("id", 1L)));

        // Update KV pair.
        assertFalse(tbl.putIfAbsent(key, val2));

        assertEqualsValues(schema, val, tbl.get(key));
        assertEqualsValues(schema, val, tbl.get(Tuple.create().set("id", 1L)));
    }

    /**
     *
     */
    @Test
    public void getAndPut() {
        SchemaDescriptor schema = new SchemaDescriptor(
            tableId,
            1,
            new Column[] {new Column("id", NativeTypes.INT64, false)},
            new Column[] {new Column("val", NativeTypes.INT64, false)}
        );

        KeyValueBinaryView tbl =
            new KVBinaryViewImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema), null, null);

        final Tuple key = Tuple.create().set("id", 1L);
        final Tuple val = Tuple.create().set("val", 11L);
        final Tuple val2 = Tuple.create().set("val", 22L);
        final Tuple val3 = Tuple.create().set("val", 33L);

        assertNull(tbl.get(key));

        // Insert new tuple.
        assertNull(tbl.getAndPut(key, val));

        assertEqualsValues(schema, val, tbl.get(key));
        assertEqualsValues(schema, val, tbl.get(Tuple.create().set("id", 1L)));

        assertEqualsValues(schema, val, tbl.getAndPut(key, val2));
        assertEqualsValues(schema, val2, tbl.getAndPut(key, Tuple.create().set("val", 33L)));

        assertEqualsValues(schema, val3, tbl.get(key));
        assertNull(tbl.get(Tuple.create().set("id", 2L)));
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

        KeyValueBinaryView tbl =
            new KVBinaryViewImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema), null, null);

        final Tuple key = Tuple.create().set("id", 1L);
        final Tuple key2 = Tuple.create().set("id", 2L);
        final Tuple val = Tuple.create().set("val", 11L);
        final Tuple val2 = Tuple.create().set("val", 22L);

        // Put KV pair.
        tbl.put(key, val);

        // Delete existed key.
        assertEqualsValues(schema, val, tbl.get(key));
        assertTrue(tbl.remove(key));
        assertNull(tbl.get(key));

        // Delete already deleted key.
        assertFalse(tbl.remove(key));

        // Put KV pair.
        tbl.put(key, val2);
        assertEqualsValues(schema, val2, tbl.get(key));

        // Delete existed key.
        assertTrue(tbl.remove(Tuple.create().set("id", 1L)));
        assertNull(tbl.get(key));

        // Delete not existed key.
        assertNull(tbl.get(key2));
        assertFalse(tbl.remove(key2));
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

        final KeyValueBinaryView tbl =
            new KVBinaryViewImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema), null, null);

        final Tuple key = Tuple.create().set("id", 1L);
        final Tuple key2 = Tuple.create().set("id", 2L);
        final Tuple val = Tuple.create().set("val", 11L);
        final Tuple val2 = Tuple.create().set("val", 22L);

        // Put KV pair.
        tbl.put(key, val);
        assertEqualsValues(schema, val, tbl.get(key));

        // Fails to delete KV pair with unexpected value.
        assertFalse(tbl.remove(key, val2));
        assertEqualsValues(schema, val, tbl.get(key));

        // Delete KV pair with expected value.
        assertTrue(tbl.remove(key, val));
        assertNull(tbl.get(key));

        // Once again.
        assertFalse(tbl.remove(key, val));
        assertNull(tbl.get(key));

        // Try to remove non-existed key.
        assertThrows(Exception.class, () -> tbl.remove(key, null));
        assertNull(tbl.get(key));

        // Put KV pair.
        tbl.put(key, val2);
        assertEqualsValues(schema, val2, tbl.get(key));

        // Check null value ignored.
        assertThrows(Exception.class, () -> tbl.remove(key, null));
        assertEqualsValues(schema, val2, tbl.get(key));

        // Delete KV pair with expected value.
        assertTrue(tbl.remove(key, val2));
        assertNull(tbl.get(key));

        assertFalse(tbl.remove(key2, val2));
        assertNull(tbl.get(key2));
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

        KeyValueBinaryView tbl =
            new KVBinaryViewImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema), null, null);

        final Tuple key = Tuple.create().set("id", 1L);
        final Tuple key2 = Tuple.create().set("id", 2L);
        final Tuple val = Tuple.create().set("val", 11L);
        final Tuple val2 = Tuple.create().set("val", 22L);
        final Tuple val3 = Tuple.create().set("val", 33L);

        // Ignore replace operation for non-existed KV pair.
        assertFalse(tbl.replace(key, val));
        assertNull(tbl.get(key));

        tbl.put(key, val);

        // Replace existed KV pair.
        assertTrue(tbl.replace(key, val2));
        assertEqualsValues(schema, val2, tbl.get(key));

        // Remove existed KV pair.
        assertTrue(tbl.replace(key, null));
        assertNull(tbl.get(key));

        // Ignore replace operation for non-existed KV pair.
        assertFalse(tbl.replace(key, val3));
        assertNull(tbl.get(key));

        tbl.put(key, val3);
        assertEqualsValues(schema, val3, tbl.get(key));

        // Remove non-existed KV pair.
        assertFalse(tbl.replace(key2, null));
        assertNull(tbl.get(key2));
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

        KeyValueBinaryView tbl =
            new KVBinaryViewImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema), null, null);

        final Tuple key = Tuple.create().set("id", 1L);
        final Tuple key2 = Tuple.create().set("id", 2L);
        final Tuple val = Tuple.create().set("val", 11L);
        final Tuple val2 = Tuple.create().set("val", 22L);
        final Tuple val3 = Tuple.create().set("val", 33L);

        // Insert KV pair.
        assertTrue(tbl.replace(key, null, val));
        assertEqualsValues(schema, val, tbl.get(key));
        assertNull(tbl.get(key2));

        // Ignore replace operation for non-existed KV pair.
        assertFalse(tbl.replace(key2, val, val2));
        assertNull(tbl.get(key2));

        // Replace existed KV pair.
        assertTrue(tbl.replace(key, val, val2));
        assertEqualsValues(schema, val2, tbl.get(key));

        // Remove existed KV pair.
        assertTrue(tbl.replace(key, val2, null));
        assertNull(tbl.get(key));

        // Insert KV pair.
        assertTrue(tbl.replace(key, null, val3));
        assertEqualsValues(schema, val3, tbl.get(key));

        // Remove non-existed KV pair.
        assertTrue(tbl.replace(key2, null, null));
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

            Assertions.assertEquals(val1, val2, "Key columns equality check failed: colIdx=" + col.schemaIndex());
        }
    }
}
