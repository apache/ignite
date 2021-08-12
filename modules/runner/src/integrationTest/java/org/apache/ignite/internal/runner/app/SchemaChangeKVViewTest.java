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

import java.io.Serializable;
import java.util.List;
import java.util.function.Supplier;
import org.apache.ignite.app.Ignite;
import org.apache.ignite.internal.table.ColumnNotFoundException;
import org.apache.ignite.schema.Column;
import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.table.KeyValueBinaryView;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Ignition interface tests.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-14581")
class SchemaChangeKVViewTest extends AbstractSchemaChangeTest {
    /**
     * Check add a new column to table schema.
     */
    @Test
    public void testDropColumn() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        KeyValueBinaryView kvView = grid.get(1).tables().table(TABLE).kvView();

        {
            kvView.put(kvView.tupleBuilder().set("key", 1L).build(),
                kvView.tupleBuilder().set("valInt", 111).set("valStr", "str").build());
        }

        dropColumn(grid, "valStr");

        {
            // Check old row conversion.
            final Tuple keyTuple = kvView.tupleBuilder().set("key", 1L).build();

            assertEquals(111, (Integer)kvView.get(keyTuple).value("valInt"));
            assertThrows(ColumnNotFoundException.class, () -> kvView.get(keyTuple).value("valStr"));

            // Check tuple of outdated schema.
            assertThrows(ColumnNotFoundException.class, () -> kvView.put(
                kvView.tupleBuilder().set("key", 2L).build(),
                kvView.tupleBuilder().set("valInt", -222).set("valStr", "str").build())
            );

            // Check tuple of correct schema.
            kvView.put(kvView.tupleBuilder().set("key", 2L).build(), kvView.tupleBuilder().set("valInt", 222).build());

            final Tuple keyTuple2 = kvView.tupleBuilder().set("key", 2L).build();

            assertEquals(222, (Integer)kvView.get(keyTuple2).value("valInt"));
            assertThrows(ColumnNotFoundException.class, () -> kvView.get(keyTuple2).value("valStr"));
        }
    }


    /**
     * Check drop column from table schema.
     */
    @Test
    public void testAddNewColumn() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        KeyValueBinaryView kvView = grid.get(1).tables().table(TABLE).kvView();

        {
            kvView.put(kvView.tupleBuilder().set("key", 1L).build(), kvView.tupleBuilder().set("valInt", 111).build());

            assertThrows(ColumnNotFoundException.class, () -> kvView.put(
                kvView.tupleBuilder().set("key", 1L).build(),
                kvView.tupleBuilder().set("valInt", -111).set("valStrNew", "str").build())
            );
        }

        addColumn(grid, SchemaBuilders.column("valStrNew", ColumnType.string()).asNullable().withDefaultValue("default").build());

        {
            // Check old row conversion.
            Tuple keyTuple = kvView.tupleBuilder().set("key", 1L).build();

            assertEquals(111, (Integer)kvView.get(keyTuple).value("valInt"));
            assertEquals("default", kvView.get(keyTuple).value("valStrNew"));

            // Check tuple of new schema.
            kvView.put(kvView.tupleBuilder().set("key", 2L).build(),
                kvView.tupleBuilder().set("valInt", 222).set("valStrNew", "str").build());

            Tuple keyTuple2 = kvView.tupleBuilder().set("key", 2L).build();

            assertEquals(222, (Integer)kvView.get(keyTuple2).value("valInt"));
            assertEquals("str", kvView.get(keyTuple2).value("valStrNew"));
        }
    }

    /**
     * Check rename column from table schema.
     */
    @Test
    public void testRenameColumn() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        KeyValueBinaryView kvView = grid.get(1).tables().table(TABLE).kvView();

        {
            kvView.put(kvView.tupleBuilder().set("key", 1L).build(), kvView.tupleBuilder().set("valInt", 111).build());

            assertThrows(ColumnNotFoundException.class, () -> kvView.put(
                kvView.tupleBuilder().set("key", 2L).build(),
                kvView.tupleBuilder().set("valRenamed", 222).build())
            );
        }

        renameColumn(grid, "valInt", "valRenamed");

        {
            assertNull(kvView.get(kvView.tupleBuilder().set("key", 2L).build()));

            // Check old row conversion.
            Tuple keyTuple1 = kvView.tupleBuilder().set("key", 1L).build();

            assertEquals(111, (Integer)kvView.get(keyTuple1).value("valRenamed"));
            assertThrows(ColumnNotFoundException.class, () -> kvView.get(keyTuple1).value("valInt"));

            // Check tuple of correct schema.
            assertThrows(ColumnNotFoundException.class, () -> kvView.put(
                kvView.tupleBuilder().set("key", 2L).build(),
                kvView.tupleBuilder().set("valInt", -222).build())
            );

            assertNull(kvView.get(kvView.tupleBuilder().set("key", 2L).build()));

            // Check tuple of new schema.
            kvView.put(kvView.tupleBuilder().set("key", 2L).build(), kvView.tupleBuilder().set("valRenamed", 222).build());

            Tuple keyTuple2 = kvView.tupleBuilder().set("key", 2L).build();

            assertEquals(222, (Integer)kvView.get(keyTuple2).value("valRenamed"));
            assertThrows(ColumnNotFoundException.class, () -> kvView.get(keyTuple2).value("valInt"));
        }
    }

    /**
     * Check merge table schema changes.
     */
    @Test
    public void testMergeChangesAddDropAdd() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        final Column column = SchemaBuilders.column("val", ColumnType.string()).asNullable().withDefaultValue("default").build();

        KeyValueBinaryView kvView = grid.get(1).tables().table(TABLE).kvView();

        {
            kvView.put(kvView.tupleBuilder().set("key", 1L).build(),
                kvView.tupleBuilder().set("valInt", 111).build());

            assertThrows(ColumnNotFoundException.class, () -> kvView.put(
                kvView.tupleBuilder().set("key", 2L).build(),
                kvView.tupleBuilder().set("val", "I'not exists").build())
            );
        }

        addColumn(grid, column);

        {
            assertNull(kvView.get(kvView.tupleBuilder().set("key", 2L).build()));

            kvView.put(kvView.tupleBuilder().set("key", 2L).build(),
                kvView.tupleBuilder().set("valInt", 222).set("val", "string").build());

            kvView.put(kvView.tupleBuilder().set("key", 3L).build(),
                kvView.tupleBuilder().set("valInt", 333).build());
        }

        dropColumn(grid, column.name());

        {
            kvView.put(kvView.tupleBuilder().set("key", 4L).build(),
                kvView.tupleBuilder().set("valInt", 444).build());

            assertThrows(ColumnNotFoundException.class, () -> kvView.put(
                kvView.tupleBuilder().set("key", 4L).build(),
                kvView.tupleBuilder().set("val", "I'm not exist").build())
            );
        }

        addColumn(grid, SchemaBuilders.column("val", ColumnType.string()).asNullable().withDefaultValue("default").build());

        {
            kvView.put(kvView.tupleBuilder().set("key", 5L).build(),
                kvView.tupleBuilder().set("valInt", 555).build());

            // Check old row conversion.
            Tuple keyTuple1 = kvView.tupleBuilder().set("key", 1L).build();

            assertEquals(111, (Integer)kvView.get(keyTuple1).value("valInt"));
            assertEquals("default", kvView.get(keyTuple1).value("val"));

            Tuple keyTuple2 = kvView.tupleBuilder().set("key", 2L).build();

            assertEquals(222, (Integer)kvView.get(keyTuple2).value("valInt"));
            assertEquals("default", kvView.get(keyTuple2).value("val"));

            Tuple keyTuple3 = kvView.tupleBuilder().set("key", 3L).build();

            assertEquals(333, (Integer)kvView.get(keyTuple3).value("valInt"));
            assertEquals("default", kvView.get(keyTuple3).value("val"));

            Tuple keyTuple4 = kvView.tupleBuilder().set("key", 4L).build();

            assertEquals(444, (Integer)kvView.get(keyTuple4).value("valInt"));
            assertEquals("default", kvView.get(keyTuple4).value("val"));

            Tuple keyTuple5 = kvView.tupleBuilder().set("key", 5L).build();

            assertEquals(555, (Integer)kvView.get(keyTuple5).value("valInt"));
            assertEquals("default", kvView.get(keyTuple5).value("val"));
        }
    }


    /**
     * Check merge table schema changes.
     */
    @Test
    public void testMergeChangesColumnDefault() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        KeyValueBinaryView kvView = grid.get(1).tables().table(TABLE).kvView();

        final String colName = "valStr";

        {
            kvView.put(kvView.tupleBuilder().set("key", 1L).build(), kvView.tupleBuilder().set("valInt", 111).build());
        }

        changeDefault(grid, colName, (Supplier<Object> & Serializable)() -> "newDefault");
        addColumn(grid, SchemaBuilders.column("val", ColumnType.string()).withDefaultValue("newDefault").build());

        {
            kvView.put(kvView.tupleBuilder().set("key", 2L).build(), kvView.tupleBuilder().set("valInt", 222).build());
        }

        changeDefault(grid, colName, (Supplier<Object> & Serializable)() -> "brandNewDefault");
        changeDefault(grid, "val", (Supplier<Object> & Serializable)() -> "brandNewDefault");

        {
            kvView.put(kvView.tupleBuilder().set("key", 3L).build(), kvView.tupleBuilder().set("valInt", 333).build());

            // Check old row conversion.
            Tuple keyTuple1 = kvView.tupleBuilder().set("key", 1L).build();

            assertEquals(111, (Integer)kvView.get(keyTuple1).value("valInt"));
            assertEquals("default", kvView.get(keyTuple1).value("valStr"));
            assertEquals("newDefault", kvView.get(keyTuple1).value("val"));

            Tuple keyTuple2 = kvView.tupleBuilder().set("key", 2L).build();

            assertEquals(222, (Integer)kvView.get(keyTuple2).value("valInt"));
            assertEquals("newDefault", kvView.get(keyTuple2).value("valStr"));
            assertEquals("newDefault", kvView.get(keyTuple2).value("val"));

            Tuple keyTuple3 = kvView.tupleBuilder().set("key", 3L).build();

            assertEquals(333, (Integer)kvView.get(keyTuple3).value("valInt"));
            assertEquals("brandNewDefault", kvView.get(keyTuple3).value("valStr"));
            assertEquals("brandNewDefault", kvView.get(keyTuple3).value("val"));
        }
    }
}
