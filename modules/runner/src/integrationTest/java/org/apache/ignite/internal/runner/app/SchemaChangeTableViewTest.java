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
import org.apache.ignite.schema.Column;
import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.table.Table;
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
class SchemaChangeTableViewTest extends AbstractSchemaChangeTest {
    /**
     * Check add a new column to table schema.
     */
    @Test
    public void testDropColumn() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        final Table tbl = grid.get(0).tables().table(TABLE);

        {
            tbl.insert(Tuple.create().set("key", 1L).set("valInt", 111).set("valStr", "str"));
        }

        dropColumn(grid, "valStr");

        {
            // Check old row conversion.
            final Tuple keyTuple = Tuple.create().set("key", 1L);

            assertEquals(1, (Long)tbl.get(keyTuple).value("key"));
            assertEquals(111, (Integer)tbl.get(keyTuple).value("valInt"));
            assertThrows(IllegalArgumentException.class, () -> tbl.get(keyTuple).value("valStr"));

            // Check tuple of outdated schema.
            assertThrows(IllegalArgumentException.class,
                () -> tbl.insert(Tuple.create().set("key", 2L).set("valInt", -222).set("valStr", "str"))
            );

            // Check tuple of correct schema.
            tbl.insert(Tuple.create().set("key", 2L).set("valInt", 222));

            final Tuple keyTuple2 = Tuple.create().set("key", 2L);

            assertEquals(2, (Long)tbl.get(keyTuple2).value("key"));
            assertEquals(222, (Integer)tbl.get(keyTuple2).value("valInt"));
            assertThrows(IllegalArgumentException.class, () -> tbl.get(keyTuple2).value("valStr"));
        }
    }

    /**
     * Check drop column from table schema.
     */
    @Test
    public void testAddNewColumn() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        Table tbl = grid.get(0).tables().table(TABLE);

        {
            tbl.insert(Tuple.create().set("key", 1L).set("valInt", 111));

            assertThrows(IllegalArgumentException.class,
                () -> tbl.insert(Tuple.create().set("key", 1L).set("valInt", -111).set("valStrNew", "str"))
            );
        }

        addColumn(grid, SchemaBuilders.column("valStrNew", ColumnType.string()).asNullable().withDefaultValue("default").build());

        // Check old row conversion.
        Tuple keyTuple1 = Tuple.create().set("key", 1L);

        assertEquals(1, (Long)tbl.get(keyTuple1).value("key"));
        assertEquals(111, (Integer)tbl.get(keyTuple1).value("valInt"));
        assertEquals("default", tbl.get(keyTuple1).value("valStrNew"));

        // Check tuple of new schema.
        tbl.insert(Tuple.create().set("key", 2L).set("valInt", 222).set("valStrNew", "str"));

        Tuple keyTuple2 = Tuple.create().set("key", 2L);

        assertEquals(2, (Long)tbl.get(keyTuple2).value("key"));
        assertEquals(222, (Integer)tbl.get(keyTuple2).value("valInt"));
        assertEquals("str", tbl.get(keyTuple2).value("valStrNew"));
    }

    /**
     * Check column renaming.
     */
    @Test
    void testRenameColumn() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        Table tbl = grid.get(0).tables().table(TABLE);

        {
            tbl.insert(Tuple.create().set("key", 1L).set("valInt", 111));

            assertThrows(IllegalArgumentException.class,
                    () -> tbl.insert(Tuple.create().set("key", 2L).set("valRenamed", -222))
            );
        }

        renameColumn(grid, "valInt", "valRenamed");

        {
            // Check old row conversion.
            Tuple keyTuple1 = Tuple.create().set("key", 1L);

            assertEquals(1, (Long) tbl.get(keyTuple1).value("key"));
            assertEquals(111, (Integer) tbl.get(keyTuple1).value("valRenamed"));
            assertThrows(IllegalArgumentException.class, () -> tbl.get(keyTuple1).value("valInt"));

            // Check tuple of outdated schema.
            assertThrows(IllegalArgumentException.class,
                    () -> tbl.insert(Tuple.create().set("key", 2L).set("valInt", -222))
            );

            // Check tuple of correct schema.
            tbl.insert(Tuple.create().set("key", 2L).set("valRenamed", 222));

            Tuple keyTuple2 = Tuple.create().set("key", 2L);

            assertEquals(2, (Long) tbl.get(keyTuple2).value("key"));
            assertEquals(222, (Integer) tbl.get(keyTuple2).value("valRenamed"));
            assertThrows(IllegalArgumentException.class, () -> tbl.get(keyTuple2).value("valInt"));
        }
    }

    /**
     * Rename column then add a new column with same name.
     */
    @Test
    void testRenameThenAddColumnWithSameName() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        Table tbl = grid.get(0).tables().table(TABLE);

        {
            tbl.insert(Tuple.create().set("key", 1L).set("valInt", 111));

            assertThrows(IllegalArgumentException.class,
                () -> tbl.insert(Tuple.create().set("key", 2L).set("val2", -222))
            );
        }

        renameColumn(grid, "valInt", "val2");
        addColumn(grid, SchemaBuilders.column("valInt", ColumnType.INT32).asNullable().withDefaultValue(-1).build());

        {
            // Check old row conversion.
            Tuple keyTuple1 = Tuple.create().set("key", 1L);

            assertEquals(1, (Long)tbl.get(keyTuple1).value("key"));
            assertEquals(111, (Integer)tbl.get(keyTuple1).value("val2"));
            assertEquals(-1, (Integer)tbl.get(keyTuple1).value("valInt"));

            // Check tuple of outdated schema.
            assertNull(tbl.get(Tuple.create().set("key", 2L)));

            // Check tuple of correct schema.
            tbl.insert(Tuple.create().set("key", 2L).set("val2", 222));

            Tuple keyTuple2 = Tuple.create().set("key", 2L);

            assertEquals(2, (Long)tbl.get(keyTuple2).value("key"));
            assertEquals(222, (Integer)tbl.get(keyTuple2).value("val2"));
            assertEquals(-1, (Integer)tbl.get(keyTuple2).value("valInt"));
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

        Table tbl = grid.get(0).tables().table(TABLE);

        {
            tbl.insert(Tuple.create().set("key", 1L).set("valInt", 111));

            assertThrows(IllegalArgumentException.class, () -> tbl.insert(
                Tuple.create().set("key", 2L).set("val", "I'not exists"))
            );
        }

        addColumn(grid, column);

        {
            assertNull(tbl.get(Tuple.create().set("key", 2L)));

            tbl.insert(Tuple.create().set("key", 2L).set("valInt", 222).set("val", "string"));

            tbl.insert(Tuple.create().set("key", 3L).set("valInt", 333));
        }

        dropColumn(grid, column.name());

        {
            tbl.insert(Tuple.create().set("key", 4L).set("valInt", 444));

            assertThrows(IllegalArgumentException.class, () -> tbl.insert(
                Tuple.create().set("key", 4L).set("val", "I'm not exist"))
            );
        }

        addColumn(grid, SchemaBuilders.column("val", ColumnType.string()).withDefaultValue("default").build());

        {
            tbl.insert(Tuple.create().set("key", 5L).set("valInt", 555));

            // Check old row conversion.
            Tuple keyTuple1 = Tuple.create().set("key", 1L);

            assertEquals(111, (Integer)tbl.get(keyTuple1).value("valInt"));
            assertEquals("default", tbl.get(keyTuple1).value("val"));

            Tuple keyTuple2 = Tuple.create().set("key", 2L);

            assertEquals(222, (Integer)tbl.get(keyTuple2).value("valInt"));
            assertEquals("default", tbl.get(keyTuple2).value("val"));

            Tuple keyTuple3 = Tuple.create().set("key", 3L);

            assertEquals(333, (Integer)tbl.get(keyTuple3).value("valInt"));
            assertEquals("default", tbl.get(keyTuple3).value("val"));

            Tuple keyTuple4 = Tuple.create().set("key", 4L);

            assertEquals(444, (Integer)tbl.get(keyTuple4).value("valInt"));
            assertEquals("default", tbl.get(keyTuple4).value("val"));

            Tuple keyTuple5 = Tuple.create().set("key", 5L);

            assertEquals(555, (Integer)tbl.get(keyTuple5).value("valInt"));
            assertEquals("default", tbl.get(keyTuple5).value("val"));
        }
    }

    /**
     * Check merge column default value changes.
     */
    @Test
    public void testMergeChangesColumnDefault() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        Table tbl = grid.get(0).tables().table(TABLE);

        final String colName = "valStr";

        {
            tbl.insert(Tuple.create().set("key", 1L).set("valInt", 111));
        }

        changeDefault(grid, colName, (Supplier<Object> & Serializable)() -> "newDefault");
        addColumn(grid, SchemaBuilders.column("val", ColumnType.string()).withDefaultValue("newDefault").build());

        {
            tbl.insert(Tuple.create().set("key", 2L).set("valInt", 222));
        }

        changeDefault(grid, colName, (Supplier<Object> & Serializable)() -> "brandNewDefault");
        changeDefault(grid, "val", (Supplier<Object> & Serializable)() -> "brandNewDefault");

        {
            tbl.insert(Tuple.create().set("key", 3L).set("valInt", 333));

            // Check old row conversion.
            Tuple keyTuple1 = Tuple.create().set("key", 1L);

            assertEquals(111, (Integer)tbl.get(keyTuple1).value("valInt"));
            assertEquals("default", tbl.get(keyTuple1).value("valStr"));
            assertEquals("newDefault", tbl.get(keyTuple1).value("val"));

            Tuple keyTuple2 = Tuple.create().set("key", 2L);

            assertEquals(222, (Integer)tbl.get(keyTuple2).value("valInt"));
            assertEquals("newDefault", tbl.get(keyTuple2).value("valStr"));
            assertEquals("newDefault", tbl.get(keyTuple2).value("val"));

            Tuple keyTuple3 = Tuple.create().set("key", 3L);

            assertEquals(333, (Integer)tbl.get(keyTuple3).value("valInt"));
            assertEquals("brandNewDefault", tbl.get(keyTuple3).value("valStr"));
            assertEquals("brandNewDefault", tbl.get(keyTuple3).value("val"));
        }
    }
}
