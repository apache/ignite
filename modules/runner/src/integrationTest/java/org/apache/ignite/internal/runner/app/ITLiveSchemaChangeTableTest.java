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

package org.apache.ignite.internal.runner.app;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.app.Ignite;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaMismatchException;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.schema.SchemaMode;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Live schema tests.
 */
class ITLiveSchemaChangeTableTest extends AbstractSchemaChangeTest {
    /**
     * Check exception for unknown column when STRICT_SCHEMA is enabled.
     */
    @Test
    public void testStrictSchemaInsertRowOfNewSchema() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        Table tbl = grid.get(0).tables().table(TABLE);

        assertThrows(SchemaMismatchException.class, () -> tbl.insert(Tuple.create().set("key", 1L).set("unknownColumn", 10)));
    }

    /**
     * Check insert row of new schema.
     */
    @Test
    public void testLiveSchemaInsertRowOfNewSchema() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        Table tbl = grid.get(0).tables().table(TABLE);

        ((TableImpl)tbl).schemaType(SchemaMode.LIVE_SCHEMA);

        Tuple row = Tuple.create().set("key", 1L).set("valStrNew", "111").set("valIntNew", 333);

        tbl.insert(row);

        Tuple res = tbl.get(Tuple.create().set("key", 1L));

        assertEquals("111", res.value("valStrNew"));
        assertEquals(Integer.valueOf(333), res.value("valIntNew"));
    }

    /**
     * Check upsert row of old schema with row of new schema.
     */
    @Test
    public void testLiveSchemaUpsertOldSchemaRow() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        Table tbl = grid.get(0).tables().table(TABLE);

        Tuple oldSchemaTuple = Tuple.create().set("key", 32L).set("valInt", 111).set("valStr", "str");

        tbl.insert(oldSchemaTuple);

        ((TableImpl)tbl).schemaType(SchemaMode.LIVE_SCHEMA);

        Tuple upsertOldSchemaTuple = Tuple.create().set("key", 32L).set("valStrNew", "111").set("valIntNew", 333);

        tbl.upsert(upsertOldSchemaTuple);

        Tuple oldSchemaRes = tbl.get(Tuple.create().set("key", 32L));

        assertEquals("111", oldSchemaRes.value("valStrNew"));
        assertEquals(Integer.valueOf(333), oldSchemaRes.value("valIntNew"));
    }

    /**
     * Check inserting row of old schema will not lead to column removal.
     */
    @Test
    public void testLiveSchemaInsertOldSchemaRow() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        Table tbl = grid.get(0).tables().table(TABLE);

        Tuple oldSchemaTuple = Tuple.create().set("key", 32L).set("valInt", 111).set("valStr", "str");

        ((TableImpl)tbl).schemaType(SchemaMode.LIVE_SCHEMA);

        Tuple row = Tuple.create().set("key", 1L).set("valStrNew", "111").set("valIntNew", 333);

        tbl.insert(row);
        tbl.insert(oldSchemaTuple);

        Tuple res = tbl.get(Tuple.create().set("key", 1L));

        assertEquals("111", res.value("valStrNew"));
        assertEquals(Integer.valueOf(333), res.value("valIntNew"));

        SchemaDescriptor schema = ((TableImpl)tbl).schemaView().schema();

        assertTrue(schema.columnNames().contains("valStrNew"));
        assertTrue(schema.columnNames().contains("valIntNew"));
    }

    /**
     * Check strict schema works correctly after live schema
     */
    @Test
    public void testLiveSchemaAddColumnsSwitchToStrict() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        Table tbl = grid.get(0).tables().table(TABLE);

        ((TableImpl)tbl).schemaType(SchemaMode.LIVE_SCHEMA);

        Tuple val = Tuple.create().set("key", 1L).set("valStrNew", "111").set("valIntNew", 333);

        tbl.insert(val);

        Tuple res = tbl.get(Tuple.create().set("key", 1L));
        assertEquals("111", res.value("valStrNew"));
        assertEquals(Integer.valueOf(333), res.value("valIntNew"));

        ((TableImpl)tbl).schemaType(SchemaMode.STRICT_SCHEMA);

        Tuple anotherKey = Tuple.create().set("key", 2L).set("valStrNew", "111").set("valIntNew", 333);

        tbl.insert(anotherKey);

        Tuple newRes = tbl.get(Tuple.create().set("key", 2L));

        assertEquals("111", newRes.value("valStrNew"));
        assertEquals(Integer.valueOf(333), newRes.value("valIntNew"));

        assertThrows(SchemaMismatchException.class, () -> tbl.insert(Tuple.create().set("key", 1L).set("unknownColumn", 10)));
    }

    /**
     * Check upsert row of old schema with row of new schema.
     */
    @Test
    public void testLiveSchemaUpsertSchemaTwice() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        Table tbl = grid.get(0).tables().table(TABLE);

        ((TableImpl)tbl).schemaType(SchemaMode.LIVE_SCHEMA);

        Tuple oldSchemaVal = Tuple.create().set("key", 32L).set("valInt", 111).set("valStr", "str");
        Tuple upsertOldSchemaVal = Tuple.create().set("key", 32L).set("valStrNew", "111").set("valIntNew", 333);
        Tuple secondUpsertOldSchemaVal = Tuple.create().set("key", 32L).set("valStrNew", "111").set("valIntNew", 333).set("anotherNewVal", 48L);

        tbl.insert(oldSchemaVal);
        tbl.upsert(upsertOldSchemaVal);
        tbl.upsert(secondUpsertOldSchemaVal);

        Tuple oldSchemaRes = tbl.get(Tuple.create().set("key", 32L));

        assertEquals("111", oldSchemaRes.value("valStrNew"));
        assertEquals(Integer.valueOf(333), oldSchemaRes.value("valIntNew"));
        assertEquals(Long.valueOf(48L), oldSchemaRes.value("anotherNewVal"));
    }

    /**
     * Check live schema tuple can handle different value types.
     */
    @Test
    public void testLiveSchemaDifferentColumnTypes() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        Table tbl = grid.get(0).tables().table(TABLE);

        ((TableImpl)tbl).schemaType(SchemaMode.LIVE_SCHEMA);

        UUID uuid = UUID.randomUUID();

        Tuple row = Tuple.create()
            .set("key", 1L)
            .set("valByteNew", (byte)10)
            .set("valShortNew", (short)48)
            .set("valIntNew", 333)
            .set("valLongNew", 55L)
            .set("valFloatNew", 32.23f)
            .set("valDoubleNew", 100.101d)
            .set("valStrNew", "111")
            .set("valUUIDNew", uuid);

        tbl.insert(row);

        Tuple res = tbl.get(Tuple.create().set("key", 1L));

        assertEquals(Byte.valueOf((byte)10), res.value("valByteNew"));
        assertEquals(Short.valueOf((short)48), res.value("valShortNew"));
        assertEquals(Integer.valueOf(333), res.value("valIntNew"));
        assertEquals(Long.valueOf(55L), res.value("valLongNew"));
        assertEquals(32.23f, res.value("valFloatNew"), 0.001f);
        assertEquals(100.101d, res.value("valDoubleNew"), 0.001f);

        assertEquals("111", res.value("valStrNew"));
        assertEquals(uuid, res.value("valUUIDNew"));

        Tuple secondRow = Tuple.create().set("key", 2L);

        tbl.insert(secondRow);

        Tuple nullRes = tbl.get(secondRow);

        assertNull(nullRes.value("valByteNew"));
        assertNull(nullRes.value("valShortNew"));
        assertNull(nullRes.value("valIntNew"));
        assertNull(nullRes.value("valLongNew"));
        assertNull(nullRes.value("valFloatNew"));
        assertNull(nullRes.value("valDoubleNew"));
        assertNull(nullRes.value("valUUIDNew"));
        assertNull(nullRes.value("valStrNew"));
    }

    /**
     * Check live schema tuple update schema only once.
     */
    @Test
    public void testLiveSchemaBuilderUpdateSchemaOnlyOnce() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        Table tbl = grid.get(0).tables().table(TABLE);

        ((TableImpl)tbl).schemaType(SchemaMode.LIVE_SCHEMA);

        UUID uuid = UUID.randomUUID();

        Tuple row = Tuple.create()
            .set("key", 1L)
            .set("valByteNew", (byte)10)
            .set("valShortNew", (short)48)
            .set("valIntNew", 333)
            .set("valLongNew", 55L)
            .set("valFloatNew", 32.23f)
            .set("valDoubleNew", 100.101d)
            .set("valStrNew", "111")
            .set("valUUIDNew", uuid);

        tbl.insert(row);

        SchemaDescriptor schema = ((TableImpl)tbl).schemaView().schema();

        assertEquals(2, schema.version());
    }

    /**
     * Check live schema tuple can handle unsupported values and null`s correctly.
     */
    @Test
    public void testLiveSchemaNullAndUnsupportedTypes() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        Table tbl = grid.get(0).tables().table(TABLE);

        ((TableImpl)tbl).schemaType(SchemaMode.LIVE_SCHEMA);

        Tuple rowWithObject = Tuple.create().set("key", 1L).set("newBrokenColumn", new Object());

        assertThrows(InvalidTypeException.class, () -> tbl.insert(rowWithObject));

        Tuple rowWithNull = Tuple.create().set("key", 1L).set("valStrNew", null).set("valIntNew", 333);

        assertThrows(InvalidTypeException.class, () -> tbl.insert(rowWithNull));
    }
}
